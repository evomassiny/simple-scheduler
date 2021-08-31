#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
unset http_proxy
curl -X POST 'http://127.0.0.1:8000/rest/scheduler/submit/' -H 'content-type: application/xml' --data "@${DIR}/echo-workflow.xml"
