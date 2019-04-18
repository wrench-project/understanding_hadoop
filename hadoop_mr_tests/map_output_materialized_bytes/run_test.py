#!/usr/bin/env python3
import subprocess
import time
from collections import namedtuple

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

"""
Test to determine the number number of spill files and map output materialized bytes
based on map inputs, map output buffer size, and map sort spill percent.
"""
if __name__=="__main__":

    NUM_CHARACTERS_PER_WORD = 128
    TEXT_WRITABLE_LEN_SIZE = 2
    INT_WRITABLE_SIZE = 4

    KEY_VALUE_SIZE = NUM_CHARACTERS_PER_WORD + TEXT_WRITABLE_LEN_SIZE + INT_WRITABLE_SIZE

    KEY_LEN_SIZE = 2
    VALUE_LEN_SIZE = 1

    KEY_VALUE_META_DATA_SIZE = KEY_LEN_SIZE + VALUE_LEN_SIZE

    SPILL_FILE_TRAILING_BYTES = 6

    NUM_WORDS_LIST = [100, 500, 1000, 5000, 10000, 50000, 100000]

    compute_expected_map_output_bytes = lambda num_words : num_words * KEY_VALUE_SIZE
    compute_expected_map_output_materialized_bytes = lambda num_words : ((KEY_VALUE_META_DATA_SIZE + KEY_VALUE_SIZE) * num_words) + SPILL_FILE_TRAILING_BYTES

    Result = namedtuple("Result", ["map_output_bytes", "expected_map_output_bytes", "materialized_bytes", "expected_materialized_bytes", "output_to_materialized_ratio", "num_spill_files"])

    results = list()
    for num_words in NUM_WORDS_LIST:

        util.hadoop_start_up()

        """
        util.hadoop_print_configuration_property_values("mapreduce.map.sort.spill.percent",
                                                        "mapreduce.task.io.sort.mb",
                                                        "mapreduce.output.fileoutputformat.compress",
                                                        "dfs.blocksize")
        """

        util.hdfs_generate_word_file(NUM_CHARACTERS_PER_WORD, num_words)

        # things to record
        num_spill_files = 0
        map_output_bytes = 0
        expected_map_output_bytes = compute_expected_map_output_bytes(num_words)
        materialized_bytes = 0
        expected_materialized_bytes = compute_expected_map_output_materialized_bytes(num_words)

        util.print_blue("run mapreduce wordcount")
        run_wordcount = util.execute_command("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output",
                                                stderr=subprocess.STDOUT)

        for line in run_wordcount.decode().split('\n'):
            if "Map output bytes=" in line:
                map_output_bytes = int(line.split('=')[1])
                print(line)
                print(map_output_bytes)

            if "Map output materialized bytes=" in line:
                materialized_bytes = int(line.split('=')[1])
                print(line)
                print(materialized_bytes)

        # for some reason, without this sleep, I get an error from yarn saying
        # that it can't find the logs for the application id ..
        time.sleep(5)

        # write logs to file
        LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

        with open(LOG_FILE_PATH, 'r') as file:
            for line in file:

                if "Finished spill" in line:
                    num_spill_files += 1


        util.hadoop_tear_down()

        results.append(Result(map_output_bytes, expected_map_output_bytes, materialized_bytes, expected_materialized_bytes, materialized_bytes / map_output_bytes, num_spill_files))

    print(*results, sep='\n')
