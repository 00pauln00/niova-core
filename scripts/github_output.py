import os, json, csv, base64, requests, sys
from datetime import datetime
import subprocess
import slugify
from requests.structures import CaseInsensitiveDict

# path were we will store the output 
path = "/home/apatil/outputgithub"

if not(os.path.exists(path)):
    os.mkdir(path)

# current dateTime
now = datetime.now()

# convert to string
date_time_str = now.strftime("%Y-%m-%d")

# path till where we need to make date dir
date_path = os.path.join(path, date_time_str) 

if not(os.path.exists(date_path)):
    os.mkdir(date_path)

flag = False
PageNo = 1

while(not flag):    
    # it will curl the runs form the github api and store that output
    url = "https://api.github.com/repos/00pauln00/niova-core/actions/runs?page={}&per_page=100".format(PageNo)
    login_info = sys.argv[1]

    headers = CaseInsensitiveDict()
    sample_string_bytes = login_info.encode("ascii")
    base64_bytes = base64.b64encode(sample_string_bytes)

    headers["Authorization"] = "Basic " + str(base64_bytes[1:])
    headers["Accept"] = "application/vnd.github.v3+json"

    resp = requests.get(url, headers=headers)

    data = json.loads(resp.text)

    # appending key as run_id and value as workflow_name and branch_name in dictionary
    dict_values = {}
    for i in data['workflow_runs']:
        extracted_date =  i['updated_at']
        if extracted_date[0:10] == date_time_str :
            dict_values.update({i["id"]: [i["name"], i["head_branch"]]})
        else: 
            flag = True
            break
        PageNo += 1

# for every key in the dictionary it will give workflow job 
for key in dict_values:
    workflow_name = slugify(dict_values[key][0])
    url = "https://api.github.com/repos/00pauln00/niova-core/actions/runs/{}/jobs".format(key)

    headers = CaseInsensitiveDict()
    sample_string_bytes = login_info.encode("ascii")
    base64_bytes = base64.b64encode(sample_string_bytes)

    headers["Authorization"] = "Basic " + str(base64_bytes[1:])
    headers["Accept"] = "application/vnd.github.v3+json"

    resp = requests.get(url, headers=headers)
    
    branchname_path = date_path + "/" + dict_values[key][1]
    
    # creating branch folder 
    if not(os.path.exists(branchname_path)):
        os.mkdir(branchname_path)

    fname = branchname_path + "/" + workflow_name + "_" + str(key) + ".json"
    with open( fname, 'w') as outfile:
        outfile.write(str(resp.text))

    f = open(fname)
    work = json.load(f)
    f.close()
    
    # creating the file in desired branch 
    myfile = open(date_path+ "/" + dict_values[key][1] +"/" + dict_values[key][1] +"-parsed.csv", 'a', newline='')
    
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    headerList = ['WorkflowName', 'WorkFlowStartTime', 'WorkFlowCompletedTime', 
                  'WorkFlowConclusionTime', 'JobName', 'StartTime', 'EndTime', 'Conclusion']
    wr.writerow(headerList)

    for job in work["jobs"]:
        WorkFlowName = workflow_name
        WorkFlowStartTime = job["started_at"]
        WorkFlowCompletedTime = job["completed_at"]
        WorkFlowConclusionTime = job["conclusion"]
        workflow_entry = [WorkFlowName, WorkFlowStartTime,
                           WorkFlowCompletedTime, WorkFlowConclusionTime]
        wr.writerow(workflow_entry)
        
        for step in job["steps"]:
            name = step["name"]
            StartTime = step["started_at"]
            EndTime = step["completed_at"]
            Conclusion = step["conclusion"]
            entry = ["", "", "", "", name, StartTime, EndTime, Conclusion]
            wr.writerow(entry)
    
    wr.writerow('')
    myfile.flush()
    myfile.close()
 
