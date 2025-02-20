import sys

from logger import LoggerConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import add_closest_city, haversine_udf, input_paths, read_events, read_geo

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
        f"Received parameters: input_path={input_path}, geo_path={geo_path}, output_path={output_path}, date={date}"
    )

    # Create Spark session
    try:
        spark = SparkSession.builder.appName("Mart Zones").getOrCreate()
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
            F.col("a.subscription_channel"),
        )
    )

    # Get user chats
    user_chats = (
        messages_df.select(
            F.col("user_id").alias("user_a"),
            F.col("message_to").alias("user_b"),
        )
        .union(
            messages_df.select(
                F.col("message_to").alias("user_a"),
                F.col("user_id").alias("user_a"),
            )
        )
        .distinct()
    )

    # Get subscriptions pairs without chats
    friend_recommendations = user_subs_pairs.join(
        user_chats,
        (user_subs_pairs.user_left == user_chats.user_a) & (user_subs_pairs.user_right == user_chats.user_b),
        "left_anti",
    )

    # Get user location
    window = Window.partitionBy("user_id").orderBy(F.col("message_ts").desc())

    user_location = (
        messages_df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select(
            F.col("user_id"),
            F.col("lat"),
            F.col("lon"),
        )
    )

    user_location_with_city = add_closest_city(user_location, geo_df, logger)

    friend_recommendations = (
        friend_recommendations.join(user_location, (friend_recommendations.user_left == user_location.user_id))
        .select("user_left", F.col("lat").alias("left_lat"), F.col("lon").alias("left_lon"), "user_right")
        .join(user_location, (friend_recommendations.user_right == user_location.user_id))
        .select(
            "user_left",
            F.col("left_lat"),
            F.col("left_lon"),
            "user_right",
            F.col("lat").alias("right_lat"),
            F.col("lon").alias("right_lon"),
        )
        .withColumn(
            "distance",
            haversine_udf(
                F.col("left_lat"),
                F.col("left_lon"),
                F.col("right_lat"),
                F.col("right_lon"),
            ),
        )
        .filter(F.col("distance") <= 1)
        .withColumn("processed_dttm", F.lit(date))
        .select("user_left", "user_right", "zone_id", "local_time")
    )
