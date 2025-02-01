import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"


def main():
    GEO_DIR = sys.argv[1]
    EVENTS_DIR = sys.argv[2]
    OUT_PATH = sys.argv[3]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("Mart User") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "2") \
        .getOrCreate() \

    geo_df = spark.read.options(delimiter=";", header=True).csv(GEO_DIR) \
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(DoubleType())) \
        .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(DoubleType())) \
        .withColumnRenamed("lat", "geo_lat") \
        .withColumnRenamed("lng", "geo_lon")

    events_df = spark.read.parquet(EVENTS_DIR)

    def get_distance(lat1, lon1, lat2, lon2):
        R = 6371
        d_lat = F.radians(lat2 - lat1)
        d_lon = F.radians(lon2 - lon1)

        a = F.sin(d_lat / 2) * F.sin(d_lat / 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) * F.sin(d_lon / 2)
        c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
        return R * c

    get_distance_udf = F.udf(get_distance, DoubleType())

    message_df = events_df \
        .where("event_type == 'message'")\
        .select(
            "event.message_id",
            F.col("event.message_from").alias("user_id"),
            "event.message_ts",
            "lat",
            "lon"
        )

    message_with_distance = message_df.join(geo_df, how="cross") \
        .withColumn("distance", get_distance_udf(F.col("lat"), F.col("lon"), F.col("geo_lat"), F.col("geo_lon"))) \
        .select(
            "user_id",
            'message_id',
            'distance',
            "message_ts",
            'city'
        )

    window = Window.partitionBy("message_id", "user_id").orderBy("distance")

    message_with_city = message_with_distance \
        .withColumn("rank", F.row_number().over(window))\
        .filter(F.col("rank") == 1) \
        .select(
            "message_id",
            "user_id",
            "message_ts",
            "city"
        )

    window_spec = Window.partitionBy("user_id", "city").orderBy("message_ts")

    home_city_df = message_with_city \
        .withColumn("prev_city", F.lag("city").over(window_spec)) \
        .withColumn("is_new_city", (F.col("city") != F.col("prev_city")).cast("int")) \
        .withColumn("group_id", F.sum("is_new_city").over(window_spec)) \
        .withColumn("stay_duration", F.datediff(F.lead("message_ts").over(window_spec), F.col("message_ts"))) \
        .filter(F.col("stay_duration") >= 27) \
        .groupBy("user_id", "city").agg(
            F.max("stay_duration").alias("total_stay")
        ) \
        .orderBy("total_stay", ascending=False) \
        .groupBy("user_id").agg(
            F.first("city").alias("home_city")
        )

    mart_user = message_with_city \
        .groupBy("user_id").agg(
            F.last("city").alias("act_city")
        ).join(home_city_df, on="user_id", how="left")

    mart_user.write.mode("overwrite").format("parquet").save(f"{OUT_PATH}/mart_users")


if __name__ == "__main__":
    main()
