# 
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Overwrite partitions
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster partition_overwrite.py 2022-05-04 /user/master/data/geo/events /user/solovyovyu/data/geo/events
/usr/lib/spark/bin/spark-submit --master local partition_overwrite.py 2022-05-04 /user/master/data/geo/events /user/solovyovyu/data/geo/events

