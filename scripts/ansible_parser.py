from curses.ascii import GS
import json, sys, base64, requests
from textwrap import indent
from requests.structures import CaseInsensitiveDict
import re, operator, ast
from pprint import pprint

from sqlalchemy import true


def prepare_result_json(recap_text):
    # "###*\w*:\W*\w*\W*\w*"
    # pprint(recap_text)
    recipe_name_obj = re.findall(r"###*\w*:\W*\w*\W*\w*", recap_text, flags=re.DOTALL)
    recipe_name_txt = recipe_name_obj[0]

    recipe_name_txt = recipe_name_txt.strip().replace("#", "")
    recipe_name_txt = recipe_name_txt.strip().replace("'", "")
    recipe_name_txt = recipe_name_txt.strip().replace("\\n", "")
    recipe_name_txt = recipe_name_txt.strip().replace('"', "")
    recipe_name_txt = recipe_name_txt.strip().replace(",", "")
    recipe_name_txt = recipe_name_txt.strip().replace("  ", "")
    recipe_name_txt = recipe_name_txt.strip().replace("recipe_name: ", "")

    # print(recipe_name_txt)

    return recipe_name_txt


def prepare_summary(recap_text):
    order = ["ok", "changed", "unreachable", "failed", "skipped", "rescued", "ignored"]
    # regex to get the strings for the summary -
    # starting from ok=%d, till ignored=%d%d%d
    summary_obj = re.findall(r"ok=.*ignored=...", recap_text, flags=re.DOTALL)
    summary_txt = summary_obj[0]

    # Clean the string
    summary_txt = summary_txt.strip().replace(",", "")
    summary_txt = summary_txt.strip().replace('"', "")
    summary_txt = summary_txt.strip().replace("'", "")
    summary_txt = summary_txt.strip().replace("\\n", "")

    # split the string to get key and values
    summary_res = summary_txt.split()
    summary_res = [i.split("=")[-1] for i in summary_res]
    summary_res = [x.strip() for x in summary_res]

    # Prepare result in json format
    json_res = {key: int(value) for key, value in zip(order, summary_res)}

    return json_res

def code_coverage(coverage):
    text = ' '.join(coverage)
    pattern = r'\*+".*'
    result = re.sub(pattern, '', text)
    new = re.findall(r'Z\s+(.*?)%', str(result))
    res = str(new)
    coverage = res.strip().replace("\\t", "")
    my_list = ast.literal_eval(coverage)
    result = [re.sub(r'coverage:\s*', '', item) for item in my_list]
    my_dict = {}
    for item in my_list:
        key_value = item.split("coverage: ")
        key = key_value[0]
        value = float(key_value[1])
        my_dict[key] = value

    return my_dict

def prepare_tasks(recap_text):
    # regex to get the strings from after === till the last
    # timing in the format of 00.00s
    task_obj = re.findall(r"===*.*\ds", recap_text, flags=re.DOTALL)
    task_txt = task_obj[0]

    task_txt = task_txt.strip().replace(",", "")
    task_txt = task_txt.strip().replace("=", "")
    task_txt = task_txt.strip().replace("-", "")
    task_txt = task_txt.strip().replace("'", "")
    task_txt = task_txt.strip().replace('"', "")
    task_txt = task_txt.strip().replace("   ", "")
    task_txt = task_txt.strip().replace("\\n", "")
    task_txt = task_txt.strip().replace("\\r", "")
    task_txt = task_txt.strip().replace("\\", "")
    task_txt = task_txt.strip().replace(":", "")

    # regex to get all the words between -
    # timestamp ending with Z and timing ending with s
    # in the format of 2021-05-21 11:11Z .... 00.00s
    task_txt = re.findall("\dZ.*?\ds", task_txt, flags=re.DOTALL)
    task_res = {}
    task_count = 0
    for task in task_txt:
        task_count = task_count + 1
        task = task.split()

        # deleting the timestamp
        del task[0]
        timing = task[len(task) - 1]
        if task[0] == "basic_recipe_for_covid_goapp" or task[0] == "basic_recipe_for_foodpalace_goapp":
            timing = timing[-5:] 
            timing = float(timing[: len(timing) - 1])
        else:
            timing = float(timing[: len(timing) - 1])
        # deleting the time taken
        del task[len(task) - 1]

        task_name = "task_" + str(task_count) + "_" + "_".join(task)

        task_res.update({task_name: float(timing)})

    return task_res


def get_job_logs(job_id):
    url = (
        "https://api.github.com/repos/00pauln00/niova-core/actions/jobs/{}/logs".format(
            job_id
        )
    )
    headers = CaseInsensitiveDict()
    token_bytes = token.encode("ascii")
    base64_token_bytes = base64.b64encode(token_bytes)

    headers["Authorization"] = "Basic " + str(base64_token_bytes[1:])
    headers["Accept"] = "application/vnd.github+json"

    resp = requests.get(url, headers=headers)

    return resp.text


token = sys.argv[1]


def runner(play_recap_text, code_cov):
    result_dict = {}
    result_json = []
    for recap_text in play_recap_text:
        result_dict.update({"recipe_name": prepare_result_json(recap_text)})
        result_dict.update({"tasks": prepare_tasks(recap_text)})
        result_dict.update({"summary": prepare_summary(recap_text)})
        result_json.append(result_dict)
        result_dict = {}
    result_json.append({"code_coverage": code_coverage(code_cov)})
    return result_json


def parse_ansible_logs(job_id):
    logs = get_job_logs(job_id)
    # "PLAY RECAP.*?recipe_name: *\W*\w*.yml"gs
    # "PLAY RECAP.*?recipe_name: *\W*\w*.yml#*"gs
    play_recap_text = re.findall(
        r"PLAY RECAP.*?recipe_name: *\W*\w*.*?\w.yml", str(logs), flags=re.DOTALL
    )
    
    code_cov = re.findall(
            r"(?s)Report(.*?)\*\*\*\* End \*\*\*\*", str(logs), flags=re.DOTALL
            )
    parsed_op = runner(play_recap_text, code_cov)

    # for recipe in parsed_op:
    #     summary = recipe['summary']
    #     if summary['failed'] == 0:
    #         print("Working!")

    # pprint(parsed_op)
    return parsed_op


parse_ansible_logs(7540338907)
