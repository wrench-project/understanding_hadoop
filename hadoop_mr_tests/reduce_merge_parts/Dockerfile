FROM wrenchproject/understanding-hadoop:test-util

USER root

COPY run_test.py /home/hadoop/run_test.py
RUN chmod u+x /home/hadoop/run_test.py

# create a directory where strace output will be written to 
RUN mkdir /home/hadoop/strace_output
RUN chmod go+rw /home/hadoop/strace_output

ENTRYPOINT ["/etc/entrypoint.sh"]
CMD ["python3", "run_test.py"]
