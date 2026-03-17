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

EXEC_LINE_MAPLARGE="from maplargeclient import *; import maplarge; maplarge.Start()"

python3 -m ipykernel_launcher --ip=0.0.0.0 -f /maplarge/config/connection.json "--InteractiveShellApp.exec_lines=${EXEC_LINE_MAPLARGE}" &
pid=$!

trap 'exit 0' SIGTERM

wait $pid
