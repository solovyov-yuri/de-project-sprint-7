# Commands to test Spark scripts via spark-submit

# export environment variables
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Overwrite partitions
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster partition_overwrite.py /user/master/data/geo/events /user/solovyovyu/data/geo/events 2022-02-01
/usr/lib/spark/bin/spark-submit --master local partition_overwrite.py /user/master/data/geo/events /user/solovyovyu/data/geo/events 2022-02-01

# Mart User
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster mart_user.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30
/usr/lib/spark/bin/spark-submit --master local mart_user.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30

# Mart Geo
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster mart_geo.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30
/usr/lib/spark/bin/spark-submit --master local mart_geo.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30

# Mart Friend Recommendation
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster mart_friend_recs.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30
/usr/lib/spark/bin/spark-submit --master local mart_friend_recs.py /user/solovyovyu/data/geo/events /user/solovyovyu/geo.csv /user/solovyovyu/analytics 2022-02-01 30