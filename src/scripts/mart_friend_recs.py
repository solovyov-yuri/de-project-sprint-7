import sys

from logger import LoggerConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import add_closest_city, add_local_time, haversine_udf, input_paths, read_events, read_geo

logger = LoggerConfig.get_logger("Mart Friend Recommendations")


def main():
    """
    Main pipeline logic.
    """
    # Check viriables
    if len(sys.argv) < 4:
        logger.error("Not enough arguments! Usage: mart_zones.py <input_path> <geo_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    try:
        date = sys.argv[4]
    except IndexError:
        date = ""
        logger.warning("Varible 'date' set as None.")

    try:
        depth = int(sys.argv[5])
    except IndexError:
        depth = 1
        logger.warning("Varible 'depth' set as 1.")

    logger.info(
        f"Received parameters: input_path={input_path}, output_path={output_path}, \
            geo_path={geo_path}, date={date}, depth={depth}"
    )

    # Create Spark session
    try:
        spark = SparkSession.builder.appName("Mart Friend Recommendations").getOrCreate()
        logger.info("SparkSession successfully created.")
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}", exc_info=True)
        sys.exit(1)

    # Create input paths
    messages_path = input_paths(input_path, logger, date, depth, "message")
    subscriptions_path = input_paths(input_path, logger, date, depth, "subscription")

    # Read data
    geo_df = read_geo(geo_path, spark, logger)
    messages_df = read_events(messages_path, spark, logger)
    subscriptions_df = read_events(subscriptions_path, spark, logger)

    # Get subscriptions pairs
    user_subs_pairs = (
        subscriptions_df.alias("a")
        .join(subscriptions_df.alias("b"), on="subscription_channel")
        .filter(F.col("a.user_id") < F.col("b.user_id"))
        .select(
            F.col("a.user_id").alias("user_left"),
            F.col("b.user_id").alias("user_right"),
            F.col("a.subscription_channel").alias("subscription_channel"),
        )
    )

    # Add closest city and local time
    messages_with_closest_city = add_closest_city(messages_df, geo_df, logger)
    messages_with_local_time = add_local_time(messages_with_closest_city, logger)

    # Get user chats
    user_chats = messages_df.union(messages_df).distinct()

    # Get subscriptions pairs without chats
    user_pairs = user_subs_pairs.join(
        user_chats,
        on=(user_subs_pairs.user_left == user_chats.user_id) & (user_subs_pairs.user_right == user_chats.message_to),
        how="left_anti",
    ).select("user_left", "user_right")

    # Add coordinates
    cols = [
        "user_left",
        "user_right",
        "lat_left",
        "lon_left",
        "lat_right",
        "lon_right",
        "a.datetime",
        "a.city",
        "a.local_time",
    ]

    user_pairs_with_coords = (
        user_pairs.join(messages_with_local_time.withColumnRenamed("user_id", "user_left").alias("a"), on="user_left")
        .withColumnRenamed("lat", "lat_left")
        .withColumnRenamed("lon", "lon_left")
        .join(messages_with_local_time.withColumnRenamed("user_id", "user_right"), on="user_right")
        .withColumnRenamed("lat", "lat_right")
        .withColumnRenamed("lon", "lon_right")
        .select(cols)
    )

    # Get friend recommendations
    friend_recommendations = (
        user_pairs_with_coords.withColumn(
            "distance",
            haversine_udf("lat_left", "lon_left", "lat_right", "lon_right"),
        )
        .filter(F.col("distance") <= 10)
        .withColumn("processed_dttm", F.lit(date))
        .select(
            "user_left",
            "user_right",
            "processed_dttm",
            F.col("city").alias("zone_id"),
            "local_time",
        )
    )

    # Save friend recommendations
    friend_recommendations.write.mode("overwrite").partitionBy("processed_dttm").parquet(
        f"{output_path}/mart_friend_recs"
    )
    logger.info(f"Friend recommendations are saved to {output_path}")

    spark.stop()
    logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
