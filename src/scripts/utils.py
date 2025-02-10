from logging import Logger

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType
from timezonefinder import TimezoneFinder


@pandas_udf(StringType())
def get_timezone(lat: pd.Series, lng: pd.Series) -> pd.Series:
    tf = TimezoneFinder()
    return lat.combine(
        lng,
        lambda lat, lng: tf.timezone_at(lat=lat, lng=lng) if pd.notnull(lat) and pd.notnull(lng) else None
    )


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
        logger.info(f"Events {event_type} are read from {events_dir}. Number of rows: {df.count()}")
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
            .withColumn("timezone", get_timezone(F.col("lat"), F.col("lng"))) \
            .withColumnRenamed("lat", "geo_lat") \
            .withColumnRenamed("lng", "geo_lon")

        logger.info(f"Geo data is read from {geo_dir}. Number of rows: {df.count()}")
        return df

    except Exception as e:
        logger.error(f"Error while reading geo data: {e}")
        raise


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points
    """
    R = 6371.0

    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)

    a = F.sin(d_lat / 2) * F.sin(d_lat / 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) * F.sin(d_lon / 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c


def add_closest_city(events_df: DataFrame, geo_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add closest city to DataFrame
    """
    geo_df = F.broadcast(geo_df)

    udf_calculate_the_distance = F.udf(calculate_distance, DoubleType())

    result_df = events_df.join(geo_df, how="cross") \
        .withColumn(
            "distance",
            udf_calculate_the_distance(
                F.col("lat"), F.col("lon"), F.col("geo_lat"), F.col("geo_lon")
            )
        ) \
        .select(
            "user_id",
            "message_id",
            "datetime",
            "lat",
            "lon",
            "geo_lat",
            "geo_lon",
            "distance"
        )

    return result_df
