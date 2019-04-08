#!/bin/bash
TEST_DIR_NAME=$1

[ $# -eq 0 ] && { echo "Usage: $0 <test_directory_name>"; exit 1; }

if [[ "$(docker images -q hadoop_test:base 2> /dev/null)" == "" ]]; then
  docker image build --no-cache -t hadoop_test:base ../hadoop_pseudodistributed_mode_container
fi

docker image build --no-cache -t hadoop_test:test_util . \
  && docker image build --no-cache -t hadoop_test:$TEST_DIR_NAME $TEST_DIR_NAME/
