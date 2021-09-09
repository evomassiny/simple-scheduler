import os.path
from pprint import pprint
from requests import Session
from time import sleep

repo_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# dummy
cred_path = os.path.join(repo_directory, "utils", os.path.basename(__file__))

print("mockup login")
session = Session()
with open(cred_path, "rb") as cred_fd:
    session.post(
        "http://127.0.0.1:8000/rest/scheduler/login",
        files={"credential": ("creds.enc", cred_fd, "text/plain")},
    )

# submit job
workflow_path = os.path.join(repo_directory, "utils", "echo-workflow.xml")
print("submit", workflow_path)
with open(workflow_path, "rb") as workflow_fd:
    response = session.post(
        "http://127.0.0.1:8000/rest/scheduler/submit/",
        files={"file": ("workflow.xml", workflow_fd, "application/xml")},
    )
    pprint(response.text)
    data = response.json()
    pprint(data)
    print()


job_id = data['id']
print("get status")
for _ in range(4):
    response = session.get(
        "http://127.0.0.1:8000/rest/scheduler/jobs/{job_id:d}".format(job_id=job_id),
    )
    data = response.json()
    pprint(data)
    sleep(0.5)
    print()



print("Killing job")
response = session.put(
    "http://127.0.0.1:8000/rest/scheduler/jobs/{job_id:d}/kill".format(job_id=job_id),
)
if response.status_code == 200:
    data = response.json()
    pprint(data)
else:
    print("Failed:")
    print(response.text)
print()

print("get status")
response = session.get(
    "http://127.0.0.1:8000/rest/scheduler/jobs/{job_id:d}".format(job_id=job_id),
)
data = response.json()
pprint(data)
