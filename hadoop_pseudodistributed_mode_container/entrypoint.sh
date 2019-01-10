#!/bin/bash

# start sshd
/etc/init.d/ssh restart

# setup environment
su hadoop -c "$HADOOP_HOME/bin/hdfs namenode -format \
              && $HADOOP_HOME/sbin/start-dfs.sh \
              && $HADOOP_HOME/sbin/start-yarn.sh \
              && $HADOOP_HOME/bin/hdfs dfs -mkdir /user \
              && $HADOOP_HOME/bin/hdfs dfs -mkdir /user/hadoop"

echo "Current Running Java Processes"
echo "------------------------------"
jps

# this is so that CMD can run after ENTRYPOINT 
exec "$@"
