FROM ubuntu:bionic

#################################################
# Hadoop 3.0.3 & Dependencies
#################################################

USER root
WORKDIR /tmp

# install tools
RUN apt-get update \
  && apt-get install -y wget vim python3 python3-dev

# install oracle java 8
ENV JAVA_HOME="/usr/local/java"
ENV PATH="$PATH:$JAVA_HOME/bin"
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3a%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjdk8-downloads-2133151.html; oraclelicense=accept-securebackup-cookie;" "https://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/jdk-8u191-linux-x64.tar.gz" \
  && tar xvzf jdk-8u191-linux-x64.tar.gz \
  && mv jdk1.8.0_191 /usr/local/java

# install ssh
RUN apt-get install -y openssh-server

# install pdsh
RUN apt-get install pdsh -y
ENV PDSH_RCMD_TYPE="ssh"

# install hadoop
ENV HADOOP_HOME="/usr/local/hadoop"
ENV PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

# install recent release
# RUN wget https://www-us.apache.org/dist/hadoop/common/hadoop-3.0.3/hadoop-3.0.3.tar.gz \
#  && tar xzf hadoop-3.0.3.tar.gz \
#  && rm hadoop-3.0.3.tar.gz \
#  && mv hadoop-3.0.3 $HADOOP_HOME \
#  && sed -i -e 's|# export JAVA_HOME=|export JAVA_HOME=/usr/local/java|g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# install hadoop 3.3.0 snapshot
COPY hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/ /usr/local/hadoop
RUN sed -i -e 's|# export JAVA_HOME=|export JAVA_HOME=/usr/local/java|g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# use configs that run hadoop in single node pseudodistributed mode
COPY configs/ /usr/local/hadoop/etc/hadoop/

# start up ssh server, hdfs, and yarn
COPY entrypoint.sh /etc/entrypoint.sh
RUN chmod u+x /etc/entrypoint.sh

# create hadoop user
RUN useradd --gid 100 --uid 550 --create-home hadoop
RUN passwd -d hadoop
RUN chown -R hadoop /usr/local/hadoop/
USER hadoop
WORKDIR /home/hadoop

# set up password less login with ssh since distributed mode may be used
RUN ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa \
  && cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys \
  && chmod 0600 /home/hadoop/.ssh/authorized_keys
