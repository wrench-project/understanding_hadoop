FROM hadoop_test:test

USER root

COPY run_test.py /home/hadoop/run_test.py
RUN chmod u+x /home/hadoop/run_test.py

COPY mapred-site.xml /usr/local/hadoop/etc/hadoop/

ENTRYPOINT ["/etc/entrypoint.sh"]
CMD ["python3", "run_test.py"]
