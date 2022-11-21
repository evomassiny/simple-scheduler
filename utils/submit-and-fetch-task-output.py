import os.path
from pprint import pprint
from requests import Session
from time import sleep
from zipfile import ZIP_DEFLATED, ZipFile
from typing import List, Optional

repo_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

cred_path = os.path.join(
    repo_directory,
    "backend",
    "test-data",
    "authentification",
    "credential-java-debug.enc",
)

print("login with debug credential")
session = Session()
with open(cred_path, "rb") as cred_fd:
    r = session.post(
        "http://127.0.0.1:8000/rest/scheduler/login",
        files={"credential": ("creds.enc", cred_fd, "text/plain")},
    )
    print(r.content)

# submit job
workflow_path = os.path.join(repo_directory, "utils", "echo-workflow.xml")
zip_file = workflow_path.replace(".xml", ".zip")

print("submit job", workflow_path)
with open(workflow_path, "rb") as workflow_fd:

    with ZipFile(zip_file, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("job.xml", workflow_fd.read())

job: Optional[int] = None
with open(zip_file, "rb") as zip_fd:
    response = session.post(
        "http://127.0.0.1:8000/rest/scheduler/submit/",
        files={"file": (os.path.basename(zip_file), zip_fd, "application/zip")},
    )
    job = response.json()["id"]

response = session.get(
    f"http://127.0.0.1:8000/rest/scheduler/jobs/{job:d}/tasks",
)
data = response.json()

tasks: List[int] = []
for task_id in data["tasks"]:
    tasks.append(int(task_id))
print(tasks)

while True:
    try:
        for task in tasks:
            response = session.get(
                f"http://127.0.0.1:8000/rest/scheduler/tasks/{task:d}",
            )
            data = response.json()
            pprint(data)
            print()
        sleep(1)
        print()
    except:
        print("login with debug credential")
        session = Session()
        with open(cred_path, "rb") as cred_fd:
            r = session.post(
                "http://127.0.0.1:8000/rest/scheduler/login",
                files={"credential": ("creds.enc", cred_fd, "text/plain")},
            )
            print(r.content)

