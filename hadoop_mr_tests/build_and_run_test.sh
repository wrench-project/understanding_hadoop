#!/bin/bash
TEST_DIR_NAME=$1

[ $# -eq 0 ] && { echo "Usage: $0 <test_directory_name (without trailing '/')>"; exit 1; }

if [[ "$(docker images -q wrenchproject/understanding-hadoop:hadoop 2> /dev/null)" == "" ]]; then
  docker image pull wrenchproject/understanding-hadoop:hadoop-run-environment \
    && docker image build --no-cache -t wrenchproject/understanding-hadoop:hadoop ../hadoop_pseudodistributed_mode_container/hadoop
fi

docker image build --no-cache -t wrenchproject/understanding-hadoop:test-util . \
  && docker image build --no-cache -t wrenchproject/understanding-hadoop:$TEST_DIR_NAME $TEST_DIR_NAME/ \
  && docker container run wrenchproject/understanding-hadoop:$TEST_DIR_NAME
