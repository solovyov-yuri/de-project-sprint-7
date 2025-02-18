import subprocess
from datetime import datetime, timedelta  # noqa

date = "2022-01-02"
row_data_path = "/user/master/data/geo/events"
partitioned_events_path = "/user/solovyovyu/data/geo/events"
geo_path = "/user/solovyovyu/geo.csv"
mart_path = "/user/solovyovyu/analytics"
last_date = "2022-06-09"

pattern = "%Y-%m-%d"
dt = datetime.strptime(date, pattern)

# # Run partition_overwrite.py
# while dt <= datetime.strptime(last_date, pattern):
#     subprocess.run(
#         [
#             "python",
#             "partition_overwrite.py",
#             dt.strftime(pattern),
#             row_data_path,
#             partitioned_events_path,
#         ]
#     )
#     dt += timedelta(days=1)

# Run mart_zones.py
subprocess.run(
    [
        "python",
        "mart_geo.py",
        partitioned_events_path,
        geo_path,
        mart_path,
    ]
)
