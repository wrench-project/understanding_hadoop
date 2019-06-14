#!/usr/bin/env python3
import subprocess
import time
import math
from collections import namedtuple

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

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
        "mapreduce.task.io.sort.factor" : 8
    }

    num_spills_list = list()
    SpillFiles = namedtuple("SpillFiles", ["expected", "actual"])

    for i in range(1,5):
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
            for line in file:

                if "SpillThread" in line:
                    print(line)

                if "[main]" in line and "Finished spill" in line:
                    print(line)

                if "path to spill file:" in line:
                    print(line)

                if "Finished spill" in line:
                    num_spills += 1

        #print("num_spills: {}".format(num_spills))
        num_spills_list.append(SpillFiles(i, num_spills))

        util.hadoop_tear_down()

    for num_spill_files in num_spills_list:
        print("expected sills: {0}, num spills: {1}".format(num_spill_files.expected, num_spill_files.actual))
