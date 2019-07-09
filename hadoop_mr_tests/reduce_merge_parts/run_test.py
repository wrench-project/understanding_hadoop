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

    '''
    Reduce Task Configuration based on MAPREDUCE_PROPERTIES
    -------------------------------------------------------
    allocated memory: 1024 MB
    jvm heap size: 100 MB (a little less in practice as Runtime.getRuntime.maxMemory() returns 93323264 bytes)
    input buffer size: (93323264 * 0.1) = 9332326.4 bytes or about 9.3 MB
    merge threshold: (9.3 MB * 0.5) = 4666163.2 bytes or about 4.6 MB
    max single shuffle limit: 1866465.28 bytes or about 1.9 MB
    reduce function input buffer: (9.3 MB * 0.5) = 4666163.2 bytes or about 4.6 MB
    '''

    # each KV pair will use  8 bytes in a map output file
    TOTAL_BYTES_PER_KV_PAIR = 8
    ONE_MB = 1 << 20

    ####################################################
    # Map Output File Sizes < Max Single Shuffle Limit #
    ####################################################
    '''
    Test: 2 mappers each output 1 MB files. 
    Result: 
        - Both files are read into memory.
        - 2 MB of input fits completely in memory and does not exceed the merge threshold, so only a single merge should be
          done from RAM straight into the reduce as 2 MB also fits in the reduce function input buffer size 
    '''
    T1 = [["r" for j in range(ONE_MB // TOTAL_BYTES_PER_KV_PAIR)] for i in range(2)]

    '''
    Test: 3 mappers each output 1 MB files. 
    Result:
        - All files are read into memory.
        - The total size of all files will be < the merge threshold so InMemoryMerger won't merge the files.
        - The 3 sorted files will be left in memory for the reduce as their total size is < the size of the reduce function input buffer
        - The 3 sorted files will not be merged into 1 as the sort factor is 3.
    '''
    T2 = [["r" for j in range(ONE_MB // TOTAL_BYTES_PER_KV_PAIR)] for i in range(3)]

    '''
    Test: 5 mappers each output 1 MB files.
    Result:
        - All files are read into memory. 
        - Merge threshold is met as 5 MB > 4.6 MB and InMemoryMerger merges the 5 files into one and spills it onto disk.
        - The single spill file is fed from disk to the reducer. 
    '''
    T3 = [["r" for j in range(ONE_MB // TOTAL_BYTES_PER_KV_PAIR)] for i in range(5)]

    '''
    Test: 10 mappers each output 1 MB files.
    Result:
        - 5 files are read into memory.
        - Merge threshold is met, then InMemoryMerger merges the 5 files into one and spills it onto disk.
        - Repeat the above 2 steps.
        - 2 spill files now reside on disk and are fed to the reducer. 
    '''
    T4 = [["r" for j in range(ONE_MB // TOTAL_BYTES_PER_KV_PAIR)] for i in range(10)]

    '''
    Test: 25 mappers each output 1 MB files.
    Result:
        - 5 files are read into memory.
        - Merge threshold is met, then InMemoryMerger merges the 5 files into one and spills it onto disk.
        - Repeat the above 4 more times.
        - On the fifth time the merge threshold is met, when the InMemoryMergeMerger calls closeOnDiskFile(compressAwarePath),
            the OnDiskMerger is called to merge the on disk files because onDiskMapOutputs.sizez() >= (2 * ioSortFactor - 1).
        - The OnDiskMerger merges 3 files, leaving two so that we end up with 3 total files in the end (ioSortFactor = 3).
        - The 3 files are fed to the reducer from disk. 
    '''
    T5 = [["r" for j in range(ONE_MB // TOTAL_BYTES_PER_KV_PAIR)] for i in range(25)]

    '''
    Test: 10 mappers each output 512 KB files.
    Result:
        - 9 files are read into memory.
        - Merge threshold is met, then InMemoryMerger merges the 9 files into one and spills it onto disk. 
            Merge factor is not considered for in memory segments, and so in one pass, the 9 files are merged
            into one. 
        - 1 file is read into memory. 
        - There is 1 file in memory that is 512 KB, and 1 file on disk that is about (512 KB * 9) KB. 
            These files are fed to the reducer as is. 
    '''
    T6 = [["r" for j in range((ONE_MB // 2) // TOTAL_BYTES_PER_KV_PAIR)] for i in range(10)]

    ####################################################
    # Map Output File Sizes > Max Single Shuffle Limit #
    ####################################################
    '''
    Test: 1 mapper outputs a file whose size is about 3 MB which is greater than the max shuffle limit.
    Result:
        - The file is copied from the mapper, to the disk of the reducer.
        - Even though 3 MB < input buffer size and 3 MB < reduce function input buffer, since 3 MB > max shuffle limit,
          the file is written to disk. 
    '''
    T7 = [["r" for j in range((3 * ONE_MB) // TOTAL_BYTES_PER_KV_PAIR)] for i in range(1)]

    '''
    Test: 2 mappers each output a file whose size is about 3 MB which is greater than the max shuffle limit.
    Result:
        - Both files are copied straight to disk.
        - Left with 2 sorted files since sort factor is 3.
    '''
    T8 = [["r" for j in range((3 * ONE_MB) // TOTAL_BYTES_PER_KV_PAIR)] for i in range(2)]

    '''
    Test: 10 mappers each output a 3 MB file.
    Result:
        - OnDiskMerger thread will start merges when onDiskMapOutputs.size() >= (2 * ioSortFactor - 1),
            so in this case when onDiskMapOutputs.size() >= 5
        - Initially 5 files are copied to the reducer.
        - OnDiskMerger starts, and 3 files are merged into 1 (2 ignored), therefore we end up with 
            3 (ioSortFactor) number of files.
        - 2 more files are copied, and now there are 5 total files on disk. OnDiskMerger starts
            and 3 files are merged into 1 while 2 are ignored. Now, there are 3 on disk files. 
            So far, 7 out of the 10 map output files have been copied. 
        - 2 more files are copied, and again there are a total of 5 files on disk. OnDiskMerger
            starts and 3 files are merged into 1 while 2 are ignored. Now there are 3 on disk files.
            So far, 9 out of the 10 map output files have been copied. 
        - The final file is copied leaving a total of 4 on disk files. ioSortFactor is 3, and so the
            final merge (initiated by the MergeManagerImpl) merges the file into 1 of the 3 files
            from the previous step thus leaving us with 3 on disk files for the reduce. 
    '''
    T9 = [["r" for j in range((3 * ONE_MB) // TOTAL_BYTES_PER_KV_PAIR)] for i in range(10)]

    # set the test 
    test = T1

    # ----------------------------------------------------------------------------------- 
    util.hadoop_start_up()

    util.hdfs_generate_custom_word_files(test)

    '''
    util.execute_command(("/usr/local/hadoop/bin/hadoop jar "
                            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar "
                            "wordcount {} input output").format(" ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])), 
                                stderr=subprocess.STDOUT)
    '''
    subprocess.check_output(("sudo -i -u hadoop /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount {} input output").format(" ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])), stderr=subprocess.STDOUT, shell=True)

    time.sleep(5)

    # write logs to file
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    logs = []

    with open(LOG_FILE_PATH, 'r') as file:
        for line in file:
            if "org.apache.hadoop.mapred" in line:
                logs.append(line)

    util.hadoop_tear_down()

    # for this test, we only care about logs that pertain to the single reducer
    # so any logs recorded before the reduce task started is discarded
    reduce_start_index = 0
    sorted_logs = util.sort_log4j_by_timestamp(logs)
    for index, line in enumerate(sorted_logs):
        if "ReduceTask.run() start" in line:
            reduce_start_index = index
            break

    sorted_logs = sorted_logs[reduce_start_index:]
    # ----------------------------------------------------------------------------------- 

    # print logs pertaining to shuffle and merge
    for line in sorted_logs:
        if "org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl" in line:
            util.print_purple(line)
        elif "org.apache.hadoop.mapreduce.task.reduce.MergeThread" in line:
            print(line)
        elif "org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput" in line:
            print(line)
        elif "org.apache.hadoop.mapreduce.task.reduce.OnDiskMapOutput" in line:
            print(line)
        elif "org.apache.hadoop.mapreduce.task.reduce.Fetcher" in line:
            print(line)
        elif "org.apache.hadoop.mapreduce.task.reduce.ShuffleSchedulerImpl" in line:
            print(line)
        elif "org.apache.hadoop.mapred.Merger" in line:
            util.print_red(line)
        elif "org.apache.hadoop.mapred.ReduceTask" in line:
            print(line)
        else:
            continue

