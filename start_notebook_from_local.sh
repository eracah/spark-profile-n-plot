#!/bin/bash -l
export CORI02=128.55.224.18
PORT=9076
ssh $CORI02  ipython "notebook"  "--no-browser" "--port" "$PORT" "$@"  > /dev/null 2>&1 &&  exit &
sleep 1
FIREFOX=/Applications/Firefox.app/Contents/MacOS
$FIREFOX/firefox --new-tab localhost:8181 &

sleep 1
ssh -L 8181:localhost:$PORT $CORI02
