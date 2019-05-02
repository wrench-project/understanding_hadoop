FROM wrenchproject/understanding-hadoop:hadoop

USER root

COPY util.py /home/hadoop/util.py
RUN chmod u+x /home/hadoop/util.py

COPY zero_compress.py /home/hadoop/zero_compress.py
RUN chmod u+x /home/hadoop/zero_compress.py

WORKDIR /home/hadoop
