import sys
import datetime
from influxdb import InfluxDBClient

def write_to_influxdb(data):
    client = InfluxDBClient(host="localhost", port=8086)
    client.create_database("niova-monitoring")
    client.switch_database("niova-monitoring")
    client.write_points(data)

path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()

line_arr = lines[0].split()
total_percent = float(line_arr[2].strip("%"))

code_cov_body = []
code_cov_body.append(
    {
        "measurement" : "total_code_cov",
        "tags" : {
            "cover_percent" : total_percent,
        },
        "time": datetime.datetime.utcnow().isoformat() + "Z",
        "fields": {
            "cover_percent" : total_percent,
        }
    }
)

write_to_influxdb(code_cov_body)

