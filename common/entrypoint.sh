#!/bin/bash
# MapLarge starts the container with the connection file path as the first argument.
# The file is mounted at /tmp/config/connection-file.json.

if [ $# -lt 1 ]; then
    connection_file=/tmp/config/connection-file.json
else
    connection_file=$1
fi

cat $connection_file

# The kernel runs in the working directory, which the image sets to the home of the kernel user.
cp $connection_file ./connection.json

EXEC_LINE_MAPLARGE="from maplargeclient import *; import maplarge; maplarge.Start()"
EXEC_LINE_MLFLOW="import mlflow; from mlflowmaplarge.maplarge_mlflow_client import MapLargeMLFlowClient; mlflow.set_tracking_uri('maplarge:/'); mlflow.set_registry_uri('maplarge:/'); maplarge_mlflow_client = MapLargeMLFlowClient.get_or_initialize_default_connection()"

# MapLarge sets ML_ENABLE_MLFLOW=0 when MLflow tracking is off for the workload.
[[ -z "${ML_ENABLE_MLFLOW}" || "${ML_ENABLE_MLFLOW}" != "0" ]] \
    && MLFLOW_EXEC="${EXEC_LINE_MLFLOW}" \
    || MLFLOW_EXEC="import mlflow"

python3 -m ipykernel_launcher --ip=0.0.0.0 -f ./connection.json \
    "--InteractiveShellApp.exec_lines=${EXEC_LINE_MAPLARGE}" \
    "--InteractiveShellApp.exec_lines=${MLFLOW_EXEC}" \
    &
pid=$!

trap 'exit 0' SIGTERM

wait $pid
