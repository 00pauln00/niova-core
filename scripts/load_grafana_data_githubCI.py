import json, base64, requests, sys, os, time
from pprint import pprint
from datetime import datetime
from requests.structures import CaseInsensitiveDict
from prometheus_client import CollectorRegistry, push_to_gateway
from prometheus_client import Histogram
from influxdb import InfluxDBClient
from slugify import slugify
from ansible_parser import parse_ansible_logs


def conv_time(time):
    t = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S%z")

    return t


def get_time_delta_seconds(t1, t2):
    td = t1.__sub__(t2)
    return td.seconds


def write_worflow_timings_influxdb(data, workflow_name):

    workflow_data = {}
    job_ids = []
    branch_names = []

    for workflow_run in data["workflow_runs"]:
        run_date = datetime.strptime(
            workflow_run["run_started_at"], "%Y-%m-%dT%H:%M:%S%z"
        )
        # print(run_date, datetime.today().date())
        if run_date.date() >= datetime(2022, 8, 10).date():
            if workflow_run["name"] == workflow_name:
                actions_url = "https://api.github.com/repos/00pauln00/niova-core/actions/runs/{}/jobs".format(
                    str(workflow_run["id"])
                )
                resp1 = requests.get(actions_url, headers=headers)
                resp1 = requests.get(actions_url, headers=headers)
                work = json.loads(resp1.text)
                workflow_data.update({str(workflow_run["id"]): work})
                job_ids.append(workflow_run["id"])
                branch_names.append(workflow_run["head_branch"])

    #pprint(len(job_ids))
    #pprint(len(branch_names))

    temp_dict = {}
    temp_dict.update({workflow_name: workflow_data})

    for job_id, branch_name in zip(job_ids, branch_names):
        influx_data = temp_dict[workflow_name][str(job_id)]

        del influx_data["jobs"][0]["check_run_url"]
        # del influx_data['jobs'][0]['head_sha']
        del influx_data["jobs"][0]["labels"]
        del influx_data["jobs"][0]["node_id"]
        del influx_data["jobs"][0]["run_attempt"]
        del influx_data["jobs"][0]["run_id"]
        del influx_data["jobs"][0]["run_url"]
        del influx_data["jobs"][0]["runner_group_id"]
        del influx_data["jobs"][0]["runner_group_name"]
        del influx_data["jobs"][0]["runner_id"]
        del influx_data["jobs"][0]["runner_name"]

        t1 = conv_time(influx_data["jobs"][0]["completed_at"])
        t2 = conv_time(influx_data["jobs"][0]["started_at"])
        td = get_time_delta_seconds(t1, t2)
        influx_data["jobs"][0]["total_time"] = td

        del influx_data["jobs"][0]["completed_at"]
        # del influx_data['jobs'][0]['id']
        del influx_data["jobs"][0]["url"]
        # pprint(influx_data)

        influx_data = influx_data["jobs"][0]
        # pprint(influx_data)
        # 7540338907
        failed_at = ""
        if influx_data["conclusion"] == "failure":
            for step in influx_data["steps"]:
                if step["conclusion"] == "failure":
                    failed_at = step["name"]

        ansible_op = parse_ansible_logs(influx_data["id"])
        data = ansible_op[len(ansible_op) - 1]
        if isinstance(data, dict):
            code_coverage = data.get('code_coverage', {})
        else:
            code_coverage = {}
        
        if workflow_name == "holon_golang_apps_recipes_workflow" or workflow_name == "holon_controlplane_app_recipes_workflow" or workflow_name == "holon_lease_recipes_workflow" or workflow_name == "holon_lease_stale_recipes_workflow":
            for key, value in code_coverage.items():
                coverage.append(
                                {
                                "measurement": "code_coverage",
                                "tags": {
                                        "workflow_name": workflow_name,
                                        "job_id": str(influx_data["id"]),
                                        "run_id": str(job_id),
                                        "commit_id": str(influx_data["head_sha"]),
                                        "branch_name": branch_name,
                                        "library_name": key
                                    },
                                "fields": { "job_url": influx_data["html_url"],
                                            "precentage": value
                                    }
                                }
                )
        recipe_list = []
        for recipe in ansible_op[:-1]:
            tasks = recipe["tasks"]
            summary = recipe["summary"]
            total_recipe_time = sum(tasks.values())
            if summary["failed"] != 0:
                recipe_body.append(
                        {
                            "measurement": "recipe_time",
                            "tags": {
                                "workflow_name": workflow_name,
                                "job_id": str(influx_data["id"]),
                                "run_id": str(job_id),
                                "recipe_conclusion": "failure",
                                "commit_id": str(influx_data["head_sha"]),
                                "recipe_name": recipe["recipe_name"],
                                "branch_name": branch_name
                            },
                            "time": influx_data["started_at"],
                            "fields": {"job_url": influx_data["html_url"]},
                        }
                    )
            else:
                top3_tasks = sorted(tasks, key=tasks.get, reverse=True)[:3]
                recipe_list.append(recipe["recipe_name"])
                recipe_body.append(
                    {
                        "measurement": "recipe_time",
                        "tags": {
                            "workflow_name": workflow_name,
                            "job_id": (influx_data["id"]),
                            "run_id": (job_id),
                            "recipe_conclusion": "success",
                            "commit_id": str(influx_data["head_sha"]),
                            "recipe_name": recipe["recipe_name"],
                            "branch_name": branch_name,
                        },
                        "time": influx_data["started_at"],
                        "fields": {
                            "recipe_name": recipe["recipe_name"],
                            "recipe_time": total_recipe_time,
                            "job_id": str(influx_data["id"]),
                            "Task_1": top3_tasks[0]
                            + " - "
                            + str(tasks.get(top3_tasks[0])),
                            "Task_2": top3_tasks[1]
                            + " - "
                            + str(tasks.get(top3_tasks[1])),
                            "Task_3": top3_tasks[2]
                            + " - "
                            + str(tasks.get(top3_tasks[2])),
                        },
                    }

                )

        json_body.append(
            {
                "measurement": "gitActions",
                "tags": {
                    "conclusion": influx_data["conclusion"],
                    "tag_url": influx_data["html_url"],
                    "commit_id": str(influx_data["head_sha"]),
                    "workflow_name": workflow_name,
                    "branch_name": branch_name,
                },
                "time": influx_data["started_at"],
                "fields": {
                    "workflow_name": workflow_name,
                    "branch_name": branch_name,
                    "run_id": str(job_id),
                    "commit_id": str(influx_data["head_sha"]),
                    "job_id": str(influx_data["id"]),
                    "duration": influx_data["total_time"],
                    "conclusion": influx_data["conclusion"],
                    "html_url": influx_data["html_url"],
                    "failed_at": failed_at,
                    "recipes": json.dumps(recipe_list),
                },
            }
        )

def write_workflow_timings_local(data):
    dict_values = {}
    for i in data["workflow_runs"]:
        # current dateTime
        now = datetime.now()
        # convert to string
        date_time_str = now.strftime("%Y-%m-%d")

        extracted_date = i["updated_at"]

        if extracted_date[0:10] == date_time_str:
            # path till where we need to make date dir
            date_path = os.path.join(path, date_time_str)
            curr_time = time.strftime("%H:%M:%S", time.localtime())
            date_time_path = date_path + "_" + curr_time

            if not (os.path.exists(date_time_path)):
                os.mkdir(date_time_path)

            dict_values.update({i["id"]: [i["name"], i["head_branch"]]})
        else:
            return

    # for every key in the dictionary it will give workflow job
    for key in dict_values:
        workflow_name = slugify(dict_values[key][0])
        url = "https://api.github.com/repos/00pauln00/niova-core/actions/runs/{}/jobs".format(
            key
        )

        resp = requests.get(url, headers=headers)

        branchname_path = date_time_path + "/" + dict_values[key][1]

        # creating branch folder
        if not (os.path.exists(branchname_path)):
            os.mkdir(branchname_path)

        fname = branchname_path + "/" + workflow_name + "_" + str(key) + ".json"
        with open(fname, "w") as outfile:
            outfile.write(str(resp.text))

        f = open(fname)
        work = json.load(f)
        # print("work: ", work)
        f.close()

        myfile_path = (
            date_time_path
            + "/"
            + dict_values[key][1]
            + "/"
            + dict_values[key][1]
            + "parsed.json"
        )

        with open(myfile_path, "a") as file:
            for job in work["jobs"]:
                WorkFlowName = workflow_name
                WorkFlowStartTime = job["started_at"]
                WorkFlowCompletedTime = job["completed_at"]
                WorkFlowConclusionTime = job["conclusion"]
                time1 = datetime.strptime(WorkFlowStartTime, "%Y-%m-%dT%H:%M:%SZ")
                time2 = datetime.strptime(WorkFlowCompletedTime, "%Y-%m-%dT%H:%M:%SZ")
                WorkFlowTime = (time2.__sub__(time1)).seconds
                workflow_entry = [
                    WorkFlowStartTime,
                    WorkFlowCompletedTime,
                    WorkFlowConclusionTime,
                ]
                # pprint.pprint(workflow_entry)

                step_res = {}
                for step in job["steps"]:
                    name = step["name"]
                    StartTime = step["started_at"]
                    EndTime = step["completed_at"]
                    Conclusion = step["conclusion"]
                    t1 = datetime.strptime(StartTime, "%Y-%m-%dT%H:%M:%S.%f%z")
                    t2 = datetime.strptime(EndTime, "%Y-%m-%dT%H:%M:%S.%f%z")
                    time_delta = t2.__sub__(t1)
                    step_res[name] = time_delta.seconds

                res = {
                    WorkFlowName: {
                        WorkFlowStartTime: {
                            "time_taken": WorkFlowTime,
                            "task_timings": step_res,
                        }
                    }
                }

                json.dump(workflow_entry, file)
                json.dump(res, file)


def write_to_influxdb(data):
    client = InfluxDBClient(host="localhost", port=8086)
    client.create_database("niova-monitoring")
    client.switch_database("niova-monitoring")
    client.write_points(data)


pages = 20
PageNo = 1
path = "/home/nikhil/Desktop/work/paroscale/githubJsonOutput/27jul"

# workflow_name = sys.argv[2]


workflow_names = [
    "holon_golang_apps_recipes_workflow",
    "holon_basic_recipes_workflow",
    "holon_coalesced_wr_recipes_workflow",
    "holon_rebuild_recipes_workflow",
    "holon_controlplane_app_recipes_workflow",
    "C/C++ CI",
    "holon_bulk_recovery_recipes_workflow",
    "holon_failover_workflow",
    "holon_prevote_recipes_workflow",
    "holon_coalesced_wr_recipes_workflow",
    "holon_lease_recipes_workflow",
    "holon_lease_stale_recipes_workflow",
]

json_body = []
recipe_body = []
coverage = []
while PageNo <= 5:
    url = "https://api.github.com/repos/00pauln00/niova-core/actions/runs?page={}&per_page=100".format(
        PageNo
    )
    token = sys.argv[1]

    headers = CaseInsensitiveDict()
    token_bytes = token.encode("ascii")
    base64_token_bytes = base64.b64encode(token_bytes)
    headers["Authorization"] = "Basic " + str(base64_token_bytes[1:])
    headers["Accept"] = "application/vnd.github.v3+json"
    resp = requests.get(url, headers=headers)

    data = json.loads(resp.text)
    # write_workflow_timings_local(data)
    for name in workflow_names:
        write_worflow_timings_influxdb(data, name)

    PageNo = PageNo + 1


write_to_influxdb(json_body)
write_to_influxdb(recipe_body)
write_to_influxdb(coverage)
