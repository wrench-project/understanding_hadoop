#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

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

    # TODO: figure out how to extract print statements from yarn logs

    util.hadoop_tear_down()
