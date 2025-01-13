import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"


def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    spark = SparkSession.builder.appName("Partition Overwrite").getOrCreate()

    pattern = "%Y-%m-%d"
    dt = datetime.strptime(date, pattern)

    dt -= timedelta(1)
    date = datetime.strftime(dt, pattern)

    events = spark.read.parquet(f"{base_input_path}/date={date}")

    events.write.mode("overwrite").partitionBy("event_type").format("parquet").save(f"{base_output_path}/date={date}")


if __name__ == "__main__":
    main()
