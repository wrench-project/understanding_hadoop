
import subprocess
import time
import math
import pass_factor
from collections import namedtuple

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

"""
Test to determine how map tasks merge spill files. This test generates an
input for word count that will cause map tasks to spill x number of spill
files and print logs relating to the merge that takes place following the
final spill.
"""

def generate_input_that_results_in_n_spills(desired_num_spills, mapreduce_task_io_sort_mb, mapreduce_map_sort_spill_percent):
    """
    Given the properties: mapreduce.io.sort.mb and mapreduce.map.sort.spill.percent, and assuming that keys are single
    ascii characters of type TextWritable and values are 4 byte integers of type IntWritable, and that no combiner
    is run, produce a list of words, that when used as input to WordCount, results in the desired number of spill
    files to be written by a single map task. Note that if you generate an input file that is larger than the block size,
    multiple mappers may be run.
    """
    # the number of bytes each key value pair will use in the map output buffer,
    # which includes metadata
    META_NUM_BYTES = 16
    KEY_LEN_NUM_BYTES = 1
    KEY_NUM_BYTES = 2
    VALUE_LEN_NUM_BYTES = 1
    VALUE_NUM_BYTES = 4
    KV_NUM_BYTES_IN_OUTPUT_BUFFER = META_NUM_BYTES + KEY_LEN_NUM_BYTES + KEY_NUM_BYTES + VALUE_LEN_NUM_BYTES + VALUE_NUM_BYTES

    num_single_char_words = math.ceil(((mapreduce_task_io_sort_mb << 20) *
                                mapreduce_map_sort_spill_percent *
                                desired_num_spills) // KV_NUM_BYTES_IN_OUTPUT_BUFFER)

    return [['r' for i in range(num_single_char_words)]]


if __name__=="__main__":
    '''
    MAP SIDE PROPERTIES
    -------------------
    mapreduce.task.io.sort.mb
        The size, in megabytes, of the memory buffer to use while sorting
        map output.

    mapreduce.map.sort.spill.percent
        The threshold usage portion for both the map output memory buffer and
        the record boundaries index to start the process of spilling to disk. For
        example, if "mapreduce.task.io.sort.mb"=1 and "mapreduce.map.sort.spill.percent"
        =0.5, then the process of spilling to disk will be started when about 512 KB of the
        1 MB buffer is used.

    mapreduce.task.io.sort.factor
        The maximum number of streams to merge at once when sorting files.
        This property is also use din the reduce. (Also used for reduce side
        merges)
    '''
    MAPREDUCE_PROPERTIES = {
        "mapreduce.task.io.sort.mb" : 1,
        "mapreduce.map.sort.spill.percent" : 0.5,
        "mapreduce.task.io.sort.factor" : 3
    }

    '''
    Map Task Configuration based on MAPREDUCE_PROPERTIES
    ----------------------------------------------------
    map task sort buffer size: 1 MB
    spilling to disk will start when 0.5 MB of the 1 MB buffer is used
    '''

    ###########################################
    # Test Specifying Number of Files Spilled #
    ###########################################
    '''
    Test: 1 spill file
    Result:
        - The single map task creats 1 spill file.
        - The reducer fetches and processes the single output file.
    '''
    T1 = 1

    '''
    Test: 3 spill files
    Result:
        - The single map task creates 3 spill files.
        - The map task merges these 3 spill files into a single file.
        - The reducer fetches and processes a single merged output file from the mapper.
    '''
    T2 = 3

    '''
    Test: 8 spill files
    Result:
        - The single map task creates 8 spill files.
        - The map task will then merge these 8 spill files into a single spill file.
            - the merges take place as follows:
            simulate_merges_using_variable_pass_factor(3, 8)
                pass: 1, current_num_segments: 8, pass_factor: 2, remaining_num_segments: 7
                pass: 2, current_num_segments: 7, pass_factor: 3, remaining_num_segments: 5
                pass: 3, current_num_segments: 5, pass_factor: 3, remaining_num_segments: 3
                pass: 4, current_num_segments: 3, pass_factor: 3, remaining_num_segments: 1
        - The reducer fetches and processes a single merged output file from the mapper.
    '''
    T3 = 8

    # set the test
    test = T2

    # -----------------------------------------------------------------------------------
    util.hadoop_start_up()
    util.hdfs_generate_custom_word_files(generate_input_that_results_in_n_spills(test,
            MAPREDUCE_PROPERTIES["mapreduce.task.io.sort.mb"],
            MAPREDUCE_PROPERTIES["mapreduce.map.sort.spill.percent"]))

    util.execute_command(("/usr/local/hadoop/bin/hadoop jar "
                            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar "
                            "wordcount {} input output").format(
                                " ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])
                                ), stderr=subprocess.STDOUT)

    time.sleep(5)

    # write logs to file
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    logs = []

    with open(LOG_FILE_PATH, 'r') as file:
        for line in file:
            if "org.apache.hadoop.mapred" in line:
                logs.append(line)

    sorted_logs = util.sort_log4j_by_timestamp(logs)
    util.hadoop_tear_down()
    # -----------------------------------------------------------------------------------

    # print logs pertaining to map side spills and merge
    for line in sorted_logs:
        if "Merger" in line:
            util.print_purple(line)
        elif "Finished spill" in line:
            util.print_blue(line)

    #print("------")
    #pass_factor.simulate_merges_using_variable_pass_factor(MAPREDUCE_PROPERTIES["mapreduce.task.io.sort.factor"], test)
