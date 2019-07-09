FROM ubuntu:bionic

#################################################
# Hadoop Dependencies
#################################################

USER root
WORKDIR /tmp

# install tools
RUN apt-get update \
  && apt-get install -y wget vim python3 python3-dev openssh-server pdsh strace sudo

# must set this env variable for pdsh
ENV PDSH_RCMD_TYPE="ssh"

# install oracle java 8
ENV JAVA_HOME="/usr/local/java"
ENV PATH="$PATH:$JAVA_HOME/bin"
COPY jdk-8u201-linux-x64.tar.gz /tmp
RUN tar xvzf jdk-8u201-linux-x64.tar.gz \
  && mv jdk1.8.0_201 /usr/local/java

# ssh server startup script
COPY entrypoint.sh /etc/entrypoint.sh
RUN chmod u+x /etc/entrypoint.sh

# set hadoop install location
ENV HADOOP_HOME="/usr/local/hadoop"

# create hadoop user
RUN useradd --gid 100 --uid 550 --create-home hadoop
RUN echo hadoop:hadoop | chpasswd
RUN usermod -aG sudo hadoop

# set up password less login with ssh since distributed mode may be used
USER hadoop
RUN ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa \
  && cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys \
  && chmod 0600 /home/hadoop/.ssh/authorized_keys
