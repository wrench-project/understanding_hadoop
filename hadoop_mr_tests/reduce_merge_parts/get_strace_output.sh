#!/bin/bash

# container id of most recently run container (the container use din this test)
CONTAINER_ID=$(docker ps -a -l --no-trunc | sed -n '2p' | cut -d' ' -f1)
HOST_DEST_DIR="strace_output"
CONTAINER_STRACE_OUTPUT_DIR="/home/hadoop/strace_output"

echo "Copying $CONTAINER_STRACE_OUTPUT from $CONTAINER_ID to $HOST_DEST_DIR"

docker cp $CONTAINER_ID:$CONTAINER_STRACE_OUTPUT_DIR $HOST_DEST_DIR
