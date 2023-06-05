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

# Process total code cover output
line_arr = lines[0].split()
total_percent = float(line_arr[2].strip("%"))

# Body to store all cover data
code_cov_body = []
# Append total cover percentage
code_cov_body.append(
    {
        "measurement" : "total_code_cov",
        "tags" : {
            "name" : "total_code_cov",
        },
        "time": datetime.datetime.utcnow().isoformat() + "Z",
        "fields": {
            "cover_percent" : total_percent,
        }
    }
)

# Process code coverage for individual
path1 = sys.argv[2]
with open(path1) as f:
    data = f.readlines()

temp = {}
for ele in data:
    try:
        val = float(ele.split()[2].strip('%'))
    except:
        val = 0.0
    temp[ele.split()[0]] = val

# Appending individual cover data to measurements
for k, v in temp.items():
    print(k)
    print(v)
    code_cov_body.append(
        {
            "measurement" : "total_code_cov",
            "tags" : {
                "name" : k,
            },
            "time": datetime.datetime.utcnow().isoformat() + "Z",
            "fields": {
                "cover_percent" : v,
            }
        }
        )

# Write data to influxDB
write_to_influxdb(code_cov_body)
