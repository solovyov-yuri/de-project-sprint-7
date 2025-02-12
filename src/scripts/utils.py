from logging import Logger

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType
from timezonefinder import TimezoneFinder
from pyspark.sql.window import Window
import numpy as np


# @pandas_udf(StringType())
# def get_timezone(lat: pd.Series, lng: pd.Series) -> pd.Series:
#     tf = TimezoneFinder()
#     return lat.combine(
#         lng,
#         lambda lat, lng: tf.timezone_at(lat=lat, lng=lng) if pd.notnull(lat) and pd.notnull(lng) else None
#     )

def get_timezone(lat, lng):
    if lat is None or lng is None:
        print("lat or lng is not defined")
        return None
    tf = TimezoneFinder()
    return tf.timezone_at(lat=lat, lng=lng)

get_timezone_udf = F.udf(get_timezone, StringType())

def read_events(event_type: str, events_dir: str, spark: SparkSession, logger: Logger) -> DataFrame:
    """
    Read events from parquet file and filter by event_type
    """
    try:
        df = spark.read.parquet(events_dir) \
            .where(F.col("event_type") == event_type) \
            .select(
                F.col("event.message_id"),
                F.coalesce(
                    F.col("event.message_from"),
                    F.col("event.reaction_from"),
                    F.col("event.user")
                ).alias("user_id"),
                F.coalesce(F.col("event.message_ts"), F.col("event.datetime")).alias("datetime"),
                "lat",
                "lon"
            )
        logger.info(f"Events {event_type} are read from {events_dir}.")
        return df

    except Exception as e:
        logger.error(f"Error while reading events: {e}")
        raise


def read_geo(geo_dir: str, spark: SparkSession, logger: Logger) -> DataFrame:
    """
    Read geo data from csv file. Add timezone column.
    """
    try:
        df = spark.read.options(delimiter=";", header=True).csv(geo_dir) \
            .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(DoubleType())) \
            .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(DoubleType())) \
            .withColumn("timezone", get_timezone_udf(F.col("lat"), F.col("lng"))) \
            .withColumnRenamed("lat", "geo_lat") \
            .withColumnRenamed("lng", "geo_lon")

        logger.info(f"Geo data is read from {geo_dir}.")
        return df

    except Exception as e:
        logger.error(f"Error while reading geo data: {e}")
        raise


def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Радиус Земли в километрах
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return R * c

@pandas_udf(DoubleType())
def haversine_udf(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    return haversine(lat1, lon1, lat2, lon2)


def add_distance(events_df: DataFrame, geo_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add distants to each city from geo_df.
    """
    geo_df = F.broadcast(geo_df)

    result_df = events_df.join(geo_df, how="cross") \
        .withColumn(
            "distance",
            haversine_udf(
                F.col("lat"), F.col("lon"), F.col("geo_lat"), F.col("geo_lon")
            )
        )

    return result_df


def add_closest_city(df: DataFrame, geo_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add city that close to event.
    """
    df_with_distance = add_distance(df, geo_df, logger)
    
    window = Window.partitionBy("message_id", "user_id").orderBy("distance")

    result_df = df_with_distance \
        .withColumn("rank", F.row_number().over(window))\
        .filter(F.col("rank") == 1) \
        .select(
            "message_id",
            "user_id",
            "datetime",
            "city",
            "timezone"
        )

    return result_df