#!/bin/bash

# run this script to compile the hadoop source code
mvn package -Pdist -Psrc -DskipTests
