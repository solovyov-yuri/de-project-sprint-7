import sys

from logger import LoggerConfig
from pyspark.sql import SparkSession
from utils import add_closest_city, agg_events_by_geo_n_period, input_paths, read_events, read_geo, get_registrations

logger = LoggerConfig.get_logger("Mart Geo")


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
        spark = SparkSession.builder.appName("Mart Geo").getOrCreate()
        logger.info("SparkSession successfully created.")
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}", exc_info=True)
        sys.exit(1)

    # Create input paths
    messages_path = input_paths(input_path, logger, date, depth, "message")
    reactions_path = input_paths(input_path, logger, date, depth, "reaction")
    subscription_path = input_paths(input_path, logger, date, depth, "subscription")

    # Read data
    geo_df = read_geo(geo_path, spark, logger)

    messages_df = read_events(messages_path, spark, logger)
    reactions_df = read_events(reactions_path, spark, logger)
    subscription_df = read_events(subscription_path, spark, logger)

    # Get registratrions
    users_df = get_registrations(messages_df, logger)

    # Add closest city
    messages_with_closest_city = add_closest_city(messages_df, geo_df, logger)
    reactions_with_closest_city = add_closest_city(reactions_df, geo_df, logger)
    subscription_with_closest_city = add_closest_city(subscription_df, geo_df, logger)
    users_with_closest_city = add_closest_city(users_df, geo_df, logger)

    # Aggregate events
    messages_agg = agg_events_by_geo_n_period(messages_with_closest_city, "message", logger)
    reactions_agg = agg_events_by_geo_n_period(reactions_with_closest_city, "reaction", logger)
    subscription_agg = agg_events_by_geo_n_period(subscription_with_closest_city, "subscription", logger)
    user_agg = agg_events_by_geo_n_period(users_with_closest_city, "user", logger)

    # Join dataframes to get final mart
    cols = [
        "month",
        "week",
        "zone_id",
        "week_message",
        "week_reaction",
        "week_subscription",
        "week_user",
        "month_message",
        "month_reaction",
        "month_subscription",
        "month_user",
    ]
    join_on = ["year", "month", "week", "zone_id"]

    mart_geo = (
        user_agg.join(messages_agg, on=join_on, how="full")
        .join(reactions_agg, on=join_on, how="full")
        .join(subscription_agg, on=join_on, how="full")
        .select(cols)
    )

    mart_geo.write.mode("overwrite").parquet(f"{output_path}/mart_geo")
    logger.info(f"Mart Geo saved in {output_path}")


if __name__ == "__main__":
    main()
