import subprocess
from calendar import monthrange
from datetime import datetime, timedelta
from logging import Logger

import numpy as np
import pandas as pd
from logger import LoggerConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
from timezonefinder import TimezoneFinder

logger = LoggerConfig.get_logger("utils")


def input_paths(
    base_path: str,
    logger: Logger,
    date: str = "",
    depth: int = 1,
    event_type: str = None,
) -> list:
    """
    Create list of directories to read data from.
    """
    pattern = "%Y-%m-%d"
    try:
        dt = datetime.strptime(date, pattern)
        date_partition = [f"date={(dt - timedelta(days=x)).strftime(pattern)}" for x in range(depth)] if date else ["*"]
        logger.info(f"Date is defined. Using {date_partition} dates.")
    except ValueError:
        date_partition = "*"
        logger.error("Date is not defined. Using all dates.")

    event_type_partition = f"event_type={event_type}" if event_type else "*"
    logger.info(f"event_type_partition: {event_type_partition}")

    result = []

    for date in date_partition:
        result.append(f"{base_path}/{date}/{event_type_partition}")

    logger.info(f"Input paths are created: {result}")

    return result


def path_exists(path: str) -> bool:
    """
    Check if path exists in HDFS.
    """
    try:
        result = subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True)
        logger.info(f"Path {path} exists: {result.returncode == 0}")
        return result.returncode == 0
    except Exception:
        logger.error(f"Error while checking path {path}, result.returncode == 0")
        return False


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


def read_events(event_paths: list, spark: SparkSession, logger: Logger) -> DataFrame:
    """
    Read events from parquet files.
    """

    existing_paths = [path for path in event_paths if path_exists(path)]
    logger.info(existing_paths)

    if not existing_paths:
        logger.warning("No valid event paths found. Returning empty DataFrame.")

        # Явно задаем схему для пустого DataFrame
        schema = StructType(
            [
                StructField("message_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("datetime", TimestampType(), True),
                StructField("lat", DoubleType(), True),
                StructField("lon", DoubleType(), True),
                StructField("subscription_channel", StringType(), True),
            ]
        )

        return spark.createDataFrame([], schema=schema)

    try:
        df = (
            spark.read.parquet(*existing_paths)
            .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .select(
                F.col("event.message_id"),
                F.coalesce(
                    F.col("event.message_from"),
                    F.col("event.reaction_from"),
                    F.col("event.user"),
                ).alias("user_id"),
                F.coalesce(F.col("event.message_ts"), F.col("event.datetime")).alias("datetime"),
                "lat",
                "lon",
                "event.subscription_channel",
                F.col("event.message_to").alias("message_to"),
                F.col("event.message_ts").alias("message_ts"),
            )
        )
        logger.info(f"Events are read from {existing_paths}.")
        return df

    except Exception as e:
        logger.error(f"Error while reading events: {e}")
        raise e


def read_geo(geo_dir: str, spark: SparkSession, logger: Logger) -> DataFrame:
    """
    Read geo data from csv file. Add timezone column.
    """
    try:
        df = (
            spark.read.options(delimiter=";", header=True)
            .csv(geo_dir)
            .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(DoubleType()))
            .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(DoubleType()))
            .withColumn("timezone", get_timezone_udf(F.col("lat"), F.col("lng")))
            .withColumnRenamed("lat", "geo_lat")
            .withColumnRenamed("lng", "geo_lon")
        )

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


def days_in_month(execution_date: str) -> int:
    """
    Returns the number of days in a month.
    """
    year, month, day = execution_date.split("-")
    return monthrange(int(year), int(month))[1]


@pandas_udf(DoubleType())
def haversine_udf(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    return haversine(lat1, lon1, lat2, lon2)


def add_distance(events_df: DataFrame, geo_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add distance to each city from geo_df.
    """
    geo_df = F.broadcast(geo_df)
    try:
        result_df = events_df.crossJoin(geo_df).withColumn(
            "distance",
            haversine_udf(
                F.col("lat"),
                F.col("lon"),
                F.col("geo_lat"),
                F.col("geo_lon"),
            ),
        )

        logger.info("Distance data added to dataframe.")
        return result_df

    except Exception as e:
        logger.error(f"Error while adding distance data: {e}")
        raise e


def get_registrations(messages_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Get first event for each user to get registration date.
    """

    window = Window.partitionBy("user_id").orderBy("datetime")

    user_df = messages_df.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1)

    return user_df


def add_closest_city(df: DataFrame, geo_df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add city that close to event.
    """
    df_with_distance = add_distance(df, geo_df, logger)

    try:
        window = Window.partitionBy("message_id", "user_id").orderBy("distance")

        result_df = (
            df_with_distance.withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .select("message_id", "user_id", "datetime", "city", "timezone")
        )
        logger.info("City closest to event added to dataframe.")

        return result_df
    except Exception as e:
        logger.error(f"Error while adding closest city: {e}")
        raise e


def agg_events_by_geo_n_period(df: DataFrame, atr_prefix: str, logger: Logger) -> DataFrame:
    """
    Creates mart with events sum by geo and week/month
    """

    df_with_weeks = (
        df.withColumn("event_date", F.to_date(F.col("datetime")))
        .withColumn("year", F.year("event_date"))
        .withColumn("month", F.month("event_date"))
        .withColumn("week", F.weekofyear("event_date"))
    )

    week_agg_df = df_with_weeks.groupBy("year", "month", "week", "city").agg(F.count("*").alias(f"week_{atr_prefix}"))
    month_agg_df = df_with_weeks.groupBy("year", "month", "city").agg(F.count("*").alias(f"month_{atr_prefix}"))

    result_df = week_agg_df.join(month_agg_df, on=["year", "month", "city"], how="left").select(
        "year", "month", "week", F.col("city").alias("zone_id"), f"week_{atr_prefix}", f"month_{atr_prefix}"
    )

    return result_df
