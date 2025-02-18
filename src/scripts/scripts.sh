# export environment variables
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Overwrite partitions
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster partition_overwrite.py 2022-05-04 /user/master/data/geo/events /user/solovyovyu/data/geo/events
/usr/lib/spark/bin/spark-submit --master local partition_overwrite.py 2022-05-04 /user/master/data/geo/events /user/solovyovyu/data/geo/events

# Mart User
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster mart_user.py /user/solovyovyu/geo.csv /user/master/data/geo/events /user/solovyovyu/analytics
/usr/lib/spark/bin/spark-submit --master local mart_user.py /user/solovyovyu/geo.csv /user/master/data/geo/events /user/solovyovyu/analytics


# Mart Geo
/usr/lib/spark/bin/spark-submit --master local mart_geo.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv  /user/solovyovyu/analytics 2022-05-04 30