#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

if __name__=="__main__":
    '''
    REDUCE SIDE PROPERTIES
    ----------------------
    mapreduce.reduce.shulffle.parallelcopies
        The number of threads (Fetcher.java or LocalFetcher.java in the source) used to copy
        map outputs to the reducer. Fetcher theads ask a scheduler (ShuffleSchedulerImpl.java)
        for a host to copy map outputs from by calling getHost() (this is synchronized). Therefore
        it will be useless (and wasteful) to set this value to be more than the number of hosts 
        available to run map tasks. 

    mapreduce.reduce.shuffle.maxfetchfailures
        The number of times a reducer tries to fetch a map output before reporting the
        error. (Not tested here)

    mapreduce.task.io.sort.factor
        The maximum number of streams to merge at once when sorting files. (Also used for map side merges)

    mapreduce.reduce.shuffle.input.buffer.percent
        The proportion of the total heap size to be allocated to the map outputs buffer during the
        copy phase of the shuffle. If "mapreduce.reduce.java.opts" = -Xmx100m, then the reduce task
        jvm process has a max heap size of 100 MB (will actually be a little less than this in practice
        when which can be observed by calling Runtime.getRuntime().maxMemory()) and if
        mapreduce.reduce.shuffle.input.buffer.percent = 0.1, then about 10 MB will be used for
        the map outputs buffer.

    mapreduce.reduce.shuffle.memory.limit.percent
        The maximum percentage of the input buffer that a single map output file being copied to
        a reducer can be for the file to be copied into the reducers main memory. If the
        map output file being copied exceeds this percentage, then the file is copied and written
        straight to disk. For example, if a reduce task has a heap size of about 100MB as set by
        "mapreduce.reduce.java.opts": "-Xmx100m", and "mapreduce.reduce.shuffle.input.buffer.percent" =
        0.1, then about 10MB of the reduce task's heap will serve as the input buffer for map outputs.
        Say that "mapreduce.reduce.shuffle.memory.limit.percent" = 0.5, then when the reduce task
        copies a map output of about 5MB or less, the map output will be copied to memory. If
        the map output doesn't fit into memory it's currently too full, its contents will be spilled
        and the shuffle should proceed after. If the map output is greater than about 5MB, then
        the output will be copied to disk. 

    mapreduce.reduce.shuffle.merge.percent
        The threshold usage proportion for the map outputs buffer for starting the process of
        merging the outputs and spilling to disk.

    mapreduce.reduce.merge.inmem.threshold
        The threshold number of map outputs for starting the process of merging the outputs and
        spilling to disk. A value of 0 or less means there is no threshold and the spill behavior
        is goverened by the merge.percent property above. 

    mapreduce.reduce.input.buffer.percent
        The proportion of the total heap size to be used for retaining map outputs in memory 
        during the reduce. For the reduce phase to begin, the size of map outputs in memory must 
        be no more than this size. By default, all map outputs are merged to disk before the reduce 
        begins, to give the reducers as much memory as possible. However, if your reducers require 
        less memory, this value may be increased to minimize the number of trips to disk.
    '''

    MAPREDUCE_PROPERTIES = {
        "mapreduce.reduce.memory.mb": 1024, # memory allotted for reduce task container
        "mapreduce.reduce.java.opts": "-Xmx100m", # reduce jvm process heap size 
        "mapreduce.reduce.shuffle.parallelcopies": 5, 
        "mapreduce.task.io.sort.factor": 3, 
        "mapreduce.reduce.shuffle.input.buffer.percent": 0.1, 
        "mapreduce.reduce.shuffle.memory.limit.percent": 0.2,
        "mapreduce.reduce.shuffle.merge.percent" : 0.5, 
        "mapreduce.reduce.merge.inmem.threshold": 1000, 
        "mapreduce.reduce.input.buffer.percent": 0.5
    }

    # run trials with NUM_MAPPERS number of mappers and a single reducer
    #NUM_MAPPERS = [3,5,9]
    NUM_MAPPERS = [8]
    logs = []
    for num_mappers in NUM_MAPPERS:
        util.hadoop_start_up()

        # create input file(s) that result in map task output files being roughly 1 MB plus a few bytes
        # using ((1<<20) // 8) because each KV pair in a map output file will use 8 bytes including metadata
        #util.hdfs_generate_custom_word_files([["r" for j in range((1 << 20) // 8)] for i in range(num_mappers)])

        # create input file(s) that result in map task output files being 1MB, 2MB, 3MB....
        util.hdfs_generate_custom_word_files([["r" for j in range(((1 << 20) // 8)* (i+1))] for i in range(num_mappers)])

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
                util.print_purple(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.MergeThread" in line:
                print(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput" in line:
                print(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.Fetcher" in line:
                print(line)
            elif "org.apache.hadoop.mapreduce.task.reduce.ShuffleSchedulerImpl" in line:
                # Fetcher threads appear to be assigned 1 per host and not one per Mapper
                print(line)
            elif "org.apache.hadoop.mapred.Merger" in line:
                util.print_red(line)
            elif "org.apache.hadoop.mapred.ReduceTask" in line:
                print(line)
            else:
                continue
