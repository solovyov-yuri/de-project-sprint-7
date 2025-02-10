import logging
import os
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Настроим логирование в файл (на воркерах)
log_file = "/tmp/spark_udf.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def setup_worker_logger():
    """Настраивает логирование на воркерах Spark (должно выполняться внутри UDF)."""
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

# --- UDF с логированием ---
@pandas_udf(StringType())
def get_timezone(lat: pd.Series, lng: pd.Series) -> pd.Series:
    setup_worker_logger()  # Инициализируем логгер на воркерах
    from timezonefinder import TimezoneFinder

    tf = TimezoneFinder()
    results = []

    for la, lo in zip(lat, lng):
        try:
            if pd.isnull(la) or pd.isnull(lo):
                raise ValueError("Latitude or Longitude is missing")
            tz = tf.timezone_at(lat=la, lng=lo)
            results.append(tz if tz else "UNKNOWN")
        except Exception as e:
            error_msg = f"Ошибка в get_timezone({la}, {lo}): {e}"
            logger.error(error_msg)  # Логируем в файл на воркерах
            results.append(None)  # Возвращаем None вместо ошибки
    
    return pd.Series(results)

# --- Использование UDF ---
geo_df = geo_df.withColumn("timezone", get_timezone(F.col("geo_lat"), F.col("geo_lon")))

# --- Альтернативный способ: добавление колонки с ошибками ---
geo_df = geo_df.withColumn("error_message", F.when(F.col("timezone").isNull(), "Timezone not found"))
