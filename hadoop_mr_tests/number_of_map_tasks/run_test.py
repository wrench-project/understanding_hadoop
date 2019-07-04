#!/usr/bin/env python3
import subprocess
from collections import namedtuple

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

"""
Tests the number of map tasks launched by YARN based on number of input files, input file size,
and HDFS block size. When a test case runs, the number of map tasks launched can be viewed under
"run mapreduce wordcount" in the section titled "Job Counters". Note that tests are slow.
"""
if __name__=="__main__":
    TestCase = namedtuple("TestCase", ["num_files", "file_size_in_MiB", "description"])

    test_cases = [
        TestCase(1, 1, "single file smaller than block size"),          # Launched map tasks=1
        TestCase(1, 20, "single file larger than block size"),          # Launched map tasks=2
        TestCase(2, 1, "two small files that fit into a single block"), # Launched map tasks=2
        TestCase(2, 20, "two files each larger than a block")           # Launched map tasks=4
    ]

    for test_case in test_cases:
        util.print_purple("*" * len(str(test_case)))
        util.print_purple(test_case)
        util.print_purple("*" * len(str(test_case)))

        util.hadoop_start_up()

        util.hdfs_generate_word_files(test_case.num_files, test_case.file_size_in_MiB)

        util.hadoop_print_configuration_property_values("dfs.block.size")

        # run map reduce wordcount on input
        util.print_blue("run mapreduce wordcount")
        run_wordcount = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output"],
                                                stderr=subprocess.STDOUT)
        print(run_wordcount.decode())

        util.hadoop_tear_down()
