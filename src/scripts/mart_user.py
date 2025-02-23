import os
import sys

from logger import LoggerConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import add_closest_city, input_paths, read_events, read_geo

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"

logger = LoggerConfig.get_logger("Mart Zones")


def main():
    cmd = "pip install timezonefinder"

    os.system(cmd)

    # Check viriables
    if len(sys.argv) < 4:
        logger.error("Not enough arguments! Usage: mart_user.py <input_path> <geo_path> <output_path>")
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
        spark = SparkSession.builder.appName("Mart User").getOrCreate()
        logger.info("SparkSession successfully created.")
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}", exc_info=True)
        sys.exit(1)

    # Create input paths
    messages_path = input_paths(input_path, logger, date, depth, "message")

    # Read data
    geo_df = read_geo(geo_path, spark, logger)

    messages_df = read_events(messages_path, spark, logger)

    # Add closest city
    messages_with_closest_city = add_closest_city(messages_df, geo_df, logger)

    # Add home city
    window = Window.partitionBy("user_id", "city").orderBy("datetime")

    home_city_df = (
        messages_with_closest_city.withColumn("prev_city", F.lag("city").over(window))
        .withColumn("is_new_city", (F.col("city") != F.col("prev_city")).cast("int"))
        .withColumn("group_id", F.sum("is_new_city").over(window))
        .withColumn("stay_duration", F.datediff(F.lead("datetime").over(window), F.col("datetime")))
        .filter(F.col("stay_duration") >= 27)
        .groupBy("user_id", "city")
        .agg(F.max("stay_duration").alias("total_stay"))
        .orderBy("total_stay", ascending=False)
        .groupBy("user_id")
        .agg(F.first("city").alias("home_city"))
    )

    mart_user = (
        messages_with_closest_city.groupBy("user_id")
        .agg(
            F.last("city").alias("act_city"),
        )
        .join(home_city_df, on="user_id", how="left")
    )

    # Add travel features
    travel_features = messages_with_closest_city.groupBy("user_id").agg(
        F.collect_list("city").alias("travel_array"),
        F.count("city").alias("travel_count"),
    )
    mart_user = mart_user.join(travel_features, on="user_id", how="left")

    # Add local time
    window = Window.partitionBy("user_id").orderBy("datetime")

    user_last_message_local_time = (
        messages_with_closest_city.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") == 1)
        .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.col("timezone")))
        .select("user_id", "local_time")
    )

    mart_user = mart_user.join(user_last_message_local_time, on="user_id", how="left")

    # Save data
    mart_user.write.mode("overwrite").parquet(f"{output_path}/mart_user")
    logger.info(f"Mart User saved in {output_path}")

    spark.stop()
    logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
