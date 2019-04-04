#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

"""
Test to see that our timestamps can be extracted from Yarn Logs for Map and Reduce tasks.
TODO: run this test on different input, possibly inserting a 'sleep' into the Map
and Reduce (in the user map and reduce implementations) to confirm that timestamps are
placed in the right location. 
"""
if __name__=="__main__":
    NUM_FILES = 3
    FILE_SIZE_IN_MiB = 20

    util.hadoop_start_up()

    # generate and add the input files into hdfs directory /user/hadoop/inputs
    util.print_blue("generating input files and adding them to /user/hadoop/input")
    generate_input = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs dfs -put {}".format(util.generate_inputs(NUM_FILES, FILE_SIZE_IN_MiB))],
                                                stderr=subprocess.DEVNULL)
    print(generate_input.decode())

    # run map reduce wordcount on input
    util.print_blue("run mapreduce wordcount")
    run_wordcount = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output"],
                                            stderr=subprocess.STDOUT)
    print(run_wordcount.decode())

    # for some reason, without this sleep, I get an error from yarn saying
    # that it can't find the logs for the application id ..
    time.sleep(5)

    # write logs to file
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    # print out all map and reduce timestamps
    util.print_blue("map and reduce task timestamps")
    with open(LOG_FILE_PATH, 'r') as file:
        for line in file:
            line = list(map(lambda x : x.strip(), line.split('|')))

            if line[0] == ">>":
                print(line[1:])

        print("")


    util.hadoop_tear_down()
