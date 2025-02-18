import os
import sys
from datetime import datetime, timedelta
from logger import LoggerConfig

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"


logger = LoggerConfig.get_logger("Partitioning")


def main():
    try:
        if len(sys.argv) < 4:
            logger.error("Not enough arguments! Usage: script.py <date> <input_path> <output_path>")
            sys.exit(1)

        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        logger.info(f"Received parameters: date={date}, input_path={base_input_path}, output_path={base_output_path}")

        try:
            spark = SparkSession.builder.appName("Partition Overwrite").getOrCreate()
            logger.info("SparkSession successfully created.")
        except Exception as e:
            logger.error(f"Error creating SparkSession: {e}", exc_info=True)
            sys.exit(1)

        # Date processing
        try:
            dt = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
            date = dt.strftime("%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Error parsing date {date}: {e}", exc_info=True)
            sys.exit(1)

        input_path = f"{base_input_path}/date={date}"
        output_path = f"{base_output_path}/date={date}"

        logger.info(f"Reading data from {input_path}")
        events = spark.read.parquet(input_path)

        logger.info(f"Saving data to {output_path}")
        events.write.mode("overwrite").partitionBy("event_type").format("parquet").save(output_path)

        logger.info("Processing completed successfully!")

    except Exception as e:
        logger.error(f"Unhandled error: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("SparkSession closed.")


if __name__ == "__main__":
    main()
