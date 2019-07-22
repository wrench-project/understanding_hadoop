#!/bin/bash

# Immediately after running this test, run this script to obtain
# strace output obtained from the call of shuffleConsumerPlugin.run()
# by the reduce task. 

# container id of most recently run container (the container used in this test)
CONTAINER_ID=$(docker ps -a -l --no-trunc | sed -n '2p' | cut -d' ' -f1)
HOST_DEST_DIR="strace_output"
CONTAINER_STRACE_OUTPUT_DIR="/home/hadoop/strace_output"

echo "Copying $CONTAINER_STRACE_OUTPUT from $CONTAINER_ID to $HOST_DEST_DIR"

docker cp $CONTAINER_ID:$CONTAINER_STRACE_OUTPUT_DIR $HOST_DEST_DIR

echo "File containing trace from merge located at:"

grep -rl "writing/merging records" strace_output
