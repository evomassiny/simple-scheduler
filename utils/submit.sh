#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
unset http_proxy

out=$(curl -s -X POST 'http://127.0.0.1:8000/rest/scheduler/submit/' -H 'content-type: application/xml' --data "@${DIR}/echo-workflow.xml")

[ $? ] || ( echo "${out}" && exit )

echo ${out}
job_id=$(echo "${out}" | jq .jobId)

while true
do
    date
    status_json=$(curl -s -X GET "http://127.0.0.1:8000/rest/scheduler/submit/${job_id}")
    [ $? ] || ( echo "${status_json}" && exit )
    echo "${status_json}" | jq .
    total=$(echo "${status_json}" | jq .jobInfo.totalNumberOfTasks )
    finished=$(echo "${status_json}" | jq .jobInfo.numberOfFinishedTasks )
    if [ "${finished}" = "${total}" ]; then
        echo "job ${job_id} succeed."
        break
    fi
    sleep 1
done
