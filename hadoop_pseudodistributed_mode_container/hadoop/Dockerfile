FROM wrenchproject/understanding-hadoop:hadoop-run-environment

USER root

# install hadoop 3.3.0 snapshot
COPY hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/ $HADOOP_HOME
RUN sed -i -e 's|# export JAVA_HOME=|export JAVA_HOME=/usr/local/java|g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# use custom configs that run hadoop in single node pseudodistributed mode
COPY custom_configs/ $HADOOP_HOME/etc/hadoop/

RUN chown -R hadoop $HADOOP_HOME

USER hadoop
