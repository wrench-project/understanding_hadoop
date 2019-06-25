#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

if __name__=="__main__":
    MAPREDUCE_PROPERTIES = {
        "mapreduce.reduce.memory.mb": 1024, # amount of memory given to container for reduce task
        "mapreduce.reduce.java.opts": "-Xmx100m", # max reduce task jvm heap size
        "mapreduce.reduce.shuffle.input.buffer.percent": 0.1, # portion of the JVM heap to be used for reduce task buffer
        "mapreduce.reduce.shuffle.merge.percent" : 0.5, # the threshold usage proportion for the map outputs buffer (defined bymapred.job.shuffle.in put.buffer.percent) for starting the process of merging the outputs and spilling to disk.
        "mapreduce.task.io.sort.factor": 3, # the maximum number of streams to merge at once when sorting files
        "mapreduce.reduce.shuffle.parallelcopies": 5 # the number of threads used to copy map outputs to the reducer
    }

    # run trials with NUM_MAPPERS number of mappers and a single reducer
    #NUM_MAPPERS = [3,5,9]
    NUM_MAPPERS = [3]
    logs = []
    for num_mappers in NUM_MAPPERS:
        util.hadoop_start_up()

        # create input file(s) that result in map task output files being roughly 1 MB plus a few bytes
        util.hdfs_generate_custom_word_files([["r" for j in range((1 << 20) // 8)] for i in range(num_mappers)])

        util.execute_command(("/usr/local/hadoop/bin/hadoop jar "
                                "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar "
                                "wordcount {} input output").format(" ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])),
                                    stderr=subprocess.STDOUT)

        time.sleep(5)

        # write logs to file
        LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

        current_logs = []

        with open(LOG_FILE_PATH, 'r') as file:
            for line in file:
                if "org.apache.hadoop.mapred" in line:
                    current_logs.append(line)

        util.hadoop_tear_down()

        # for this test, we only care about logs that pertain to the single reducer
        reduce_start_index = 0
        sorted_current_logs = util.sort_log4j_by_timestamp(current_logs)
        for index, line in enumerate(sorted_current_logs):
            if "ReduceTask.run() start" in line:
                reduce_start_index = index
                break

        logs.append(sorted_current_logs[reduce_start_index:])

    for log in logs:
        print("******************************************************************")
        for line in log:
            if "org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl" in line:
                print(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.Fetcher" in line:
                print(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.ShuffleSchedulerImpl" in line:
                util.print_red(line)
            elif "org.apache.hadoop.mapred.Merger" in line:
                print(line)
            elif "org.apache.hadoop.mapred.ReduceTask" in line:
                print(line)
            else:
                continue
