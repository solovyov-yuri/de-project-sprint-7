from calendar import monthrange
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"


default_args = {
    "owner": "Solovyov",
    "depends_on_past": False,
}
dag = DAG(
    dag_id="build_marts_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 2),
    end_date=datetime(2022, 6, 10),
    catchup=False,
)

execution_date = "{{ ds }}"

# Task 1
t1 = SparkSubmitOperator(
    task_id="partition_data",
    dag=dag,
    application="/lessons/partition_overwrite.py",
    conn_id="spark_yarn",
    application_args=[
        execution_date,
        "/user/master/data/geo/events",
        "/user/solovyovyu/data/geo/events",
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=2,
    executor_memory="2g",
)

t2 = SparkSubmitOperator(
    task_id="build_mart_users",
    dag=dag,
    application="/lessons/mart_users.py",
    conn_id="spark_yarn",
    application_args=[
        "/user/solovyovyu/data/geo/events",
        "/user/solovyovyu/geo.csv",
        "/user/solovyovyu/analytics",
        execution_date,
        30,
    ],
)

# Task 2
t3 = SparkSubmitOperator(
    task_id="build_mart_geo",
    dag=dag,
    application="/lessons/mart_geo.py",
    conn_id="spark_yarn",
    application_args=[
        "/user/solovyovyu/data/geo/events",
        "/user/solovyovyu/geo.csv",
        "/user/solovyovyu/analytics",
        execution_date,
        30,
    ],
)

t4 = SparkSubmitOperator(
    task_id="build_mart_friends_recommendations",
    dag=dag,
    application="/lessons/mart_friends_recommendations.py",
    conn_id="spark_yarn",
    application_args=[
        "/user/solovyovyu/data/geo/events",
        "/user/solovyovyu/geo.csv",
        "/user/solovyovyu/analytics",
        execution_date,
        30,
    ],
)

t1 >> [t2, t3, t4]
