#!/bin/bash

# MapLarge create a docker volume and mounts it at /tmp/config of the container
if [ $# -lt 1 ]; then
    connection_file=/tmp/config/connection-file.json
else
    connection_file=$1
fi

cat $connection_file

mkdir -p /maplarge/config

cp $connection_file /maplarge/config/connection.json

cp $connection_file /tmp/config/connection.json
# Modify the connection file to use proper IP address now
# sed -i 's;127.0.0.1;0.0.0.0;' ${connection_file}

python3 -m ipykernel_launcher --ip=0.0.0.0 -f /maplarge/config/connection.json --InteractiveShellApp.exec_lines="from maplargeclient import *; import maplarge; maplarge.Start();" &
pid=$!

trap 'kill -s TERM $pid; wait $pid' SIGTERM

wait $pid
