#!/usr/bin/env python3
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
    files to be written by a single map task.
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

    return ['r' for i in range(num_single_char_words)]


if __name__=="__main__":
    MAPREDUCE_PROPERTIES = {
        "mapreduce.task.io.sort.mb" : 1,
        "mapreduce.map.sort.spill.percent" : 0.5,
        "mapreduce.task.io.sort.factor" : 3
    }

    # list to record the number of spill files that are expected to
    # be generated and the number of spill files that are actually generated
    num_spills_list = []
    SpillFiles = namedtuple("SpillFiles", ["expected", "actual"])

    # list containing the amounts of spill files word count will create
    # by map tasks
    num_spills_to_test = [1, 3, 8]
    filtered_logs = []

    for i in num_spills_to_test:
        util.hadoop_start_up()
        util.hdfs_generate_custom_word_file(generate_input_that_results_in_n_spills(i,
                                                                                    MAPREDUCE_PROPERTIES["mapreduce.task.io.sort.mb"],
                                                                                    MAPREDUCE_PROPERTIES["mapreduce.map.sort.spill.percent"]))

        util.execute_command(("/usr/local/hadoop/bin/hadoop jar "
                                "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar "
                                "wordcount {} input output").format(" ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])),
                                    stderr=subprocess.STDOUT)

        time.sleep(5)

        # write logs to file
        LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

        num_spills = 0

        with open(LOG_FILE_PATH, 'r') as file:
            current_trial_logs = []
            for line in file:

                if "Merger" in line:
                    print(line)
                    current_trial_logs.append(line)

                if "Finished spill" in line:
                    print(line)
                    num_spills += 1
                    current_trial_logs.append(line)

        num_spills_list.append(SpillFiles(i, num_spills))
        filtered_logs.append(current_trial_logs)

        util.hadoop_tear_down()

    trials = list(zip(num_spills_list, filtered_logs))
    for trial in trials:
        pass_factor.simulate_merges_using_variable_pass_factor(MAPREDUCE_PROPERTIES["mapreduce.task.io.sort.factor"], trial[0].actual)
        print("logs:")
        print(*util.sort_log4j_by_timestamp(trial[1]), sep="\n")
        print("================")
