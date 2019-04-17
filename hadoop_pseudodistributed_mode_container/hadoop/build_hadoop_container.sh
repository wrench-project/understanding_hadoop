#!/bin/bash

mvn package -Pdist -Pdoc -Psrc -DskipTests \
    && docker image build --no-cache -t wrenchproject/understanding-hadoop:hadoop . 
