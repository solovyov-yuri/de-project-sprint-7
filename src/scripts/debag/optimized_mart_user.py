import logging
import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from timezonefinder import TimezoneFinder

# Устанавливаем переменные окружения для PySpark
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"

# --- 1. Настройка логирования ---
def setup_logging():
    """Конфигурируем логирование"""
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logger


logger = setup_logging()

# Пути к данным (можно вынести в конфиг)
GEO_DIR = "/user/solovyovyu/geo.csv"
EVENTS_DIR = "/user/solovyovyu/data/geo/events"
OUT_PATH = "/user/solovyovyu/analytics"

# --- 2. Инициализация Spark ---
try:
    spark = SparkSession.builder \
        .master("local") \
        .appName("Mart User") \
        .getOrCreate()
    logger.info("SparkSession создан успешно.")
except Exception as e:
    logger.error(f"Ошибка при создании SparkSession: {e}")
    raise


# --- 3. Функция для вычисления расстояния ---
def get_distance(lat1, lon1, lat2, lon2):
    """Вычисление расстояния между двумя точками (Haversine formula)"""
    R = 6371
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)

    a = F.sin(d_lat / 2) * F.sin(d_lat / 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) * F.sin(d_lon / 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c

get_distance_udf = F.udf(get_distance, DoubleType())

# --- 4. Оптимизированная функция для таймзоны ---
@pandas_udf(StringType())
def get_timezone(lat: pd.Series, lng: pd.Series) -> pd.Series:
    tf = TimezoneFinder()
    return lat.combine(lng, lambda lat, lng: tf.timezone_at(lat=lat, lng=lng) if pd.notnull(lat) and pd.notnull(lng) else None)

# --- 5. Загрузка geo.csv ---
try:
    geo_df = spark.read.options(delimiter=";", header=True).csv(GEO_DIR) \
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(DoubleType())) \
        .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(DoubleType())) \
        .withColumn("timezone", get_timezone(F.col("lat"), F.col("lng"))) \
        .withColumnRenamed("lat", "geo_lat") \
        .withColumnRenamed("lng", "geo_lon")

    logger.info(f"Данные загружены из {GEO_DIR}. Количество строк: {geo_df.count()}")

except Exception as e:
    logger.error(f"Ошибка при загрузке geo.csv: {e}")
    raise

# --- 6. Загрузка events ---
try:
    events_df = spark.read.parquet(EVENTS_DIR)
    logger.info(f"Данные загружены из {EVENTS_DIR}. Количество строк: {events_df.count()}")
except Exception as e:
    logger.error(f"Ошибка при загрузке events: {e}")
    raise

# --- 7. Фильтрация событий message ---
message_df = events_df \
   .where("event_type == 'message'")\
   .select(
       "event.message_id",
       F.col("event.message_from").alias("user_id"),
       F.coalesce(F.col("event.message_ts"), F.col("event.datetime")).alias("datetime"),
       "lat",
       "lon"
   )

# --- 8. Оптимизация JOIN ---
from pyspark.sql.functions import broadcast

geo_df = broadcast(geo_df)  # Оптимизация, если geo_df маленький

message_with_distance = message_df.join(geo_df, how="cross") \
    .withColumn("distance", get_distance(F.col("lat"), F.col("lon"), F.col("geo_lat"), F.col("geo_lon"))) \
    .select(
        "user_id",
        'message_id',
        'distance',
        "datetime",
        'city',
        "timezone"
    )

# --- 9. Определение home_city ---
window_spec = Window.partitionBy("user_id", "city").orderBy("datetime")

home_city_df = message_with_distance \
    .withColumn("prev_city", F.lag("city").over(window_spec)) \
    .withColumn("is_new_city", (F.col("city") != F.col("prev_city")).cast("int")) \
    .withColumn("group_id", F.sum("is_new_city").over(window_spec)) \
    .withColumn("stay_duration", F.datediff(F.lead("datetime").over(window_spec), F.col("datetime"))) \
    .filter(F.col("stay_duration") >= 27) \
    .groupBy("user_id", "city").agg(
        F.max("stay_duration").alias("total_stay")
    ) \
    .orderBy("total_stay", ascending=False) \
    .groupBy("user_id").agg(
        F.first("city").alias("home_city")  # Можно заменить на mode(), если доступно
    )

# --- 10. Добавление travel_features ---
travel_features = message_with_distance.groupBy("user_id").agg(
    F.collect_list("city").alias("travel_array"),
    F.countDistinct("city").alias("travel_count")
)

mart_user = home_city_df.join(travel_features, on="user_id", how="left")

# --- 11. Вычисление local_time ---
window = Window.partitionBy("user_id").orderBy("datetime")

user_last_message_local_time = message_with_distance \
    .withColumn("rank", F.rank().over(window)) \
    .filter(F.col("rank") == 1) \
    .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.col("timezone"))) \
    .select("user_id", "local_time")

mart_user = mart_user.join(user_last_message_local_time, on="user_id", how="left")

logger.info("Формирование mart_user завершено.")

# --- 12. Сохранение данных ---
mart_user.write.mode("overwrite").parquet(OUT_PATH)
logger.info(f"Файл сохранен в {OUT_PATH}")
spark.stop()
