#!/bin/bash

# run this script to build the docker image containing the hadoop source and then 
# push the newly created image to hub.docker.com
docker image build --no-cache -t wrenchproject/understanding-hadoop:hadoop . \
    && docker push wrenchproject/understanding-hadoop:hadoop 
