#!/bin/bash

docker image rmi -f wrenchproject/understanding-hadoop:hadoop \
    && docker image rmi -f wrenchproject/understanding-hadoop:hadoop-run-environment
