FROM wrenchproject/understanding-hadoop:test-util

USER root

COPY run_test.py /home/hadoop/run_test.py
RUN chmod u+x /home/hadoop/run_test.py

ENTRYPOINT ["/etc/entrypoint.sh"]
CMD ["python3", "run_test.py"]
