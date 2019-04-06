#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

"""
Test to determine the number of spill files created based on map inputs, map output buffer size,
and map sort spill percent.
"""
if __name__=="__main__":
    NUM_CHARACTERS = 24000
    """
    300000 :
    50000 : 3 spills
    35000 : 2 spills
    27500 : 2 spills
    24000 : 2 spills
    23875 : 2 spills
    23750 : 1 spill (materialized output bytes: 190006)
    23500 : 1 spill (materialized output bytes: 188006)
    23000 : 1 spill
    22000 : 1 spill
    20000 : 1 spill
    """

    util.hadoop_start_up()

    util.hadoop_print_configuration_property_values("mapreduce.map.sort.spill.percent",
                                                    "mapreduce.task.io.sort.mb",
                                                    "mapreduce.output.fileoutputformat.compress",
                                                    "dfs.blocksize")

    util.hdfs_generate_character_file(NUM_CHARACTERS)

    util.print_blue("run mapreduce wordcount")
    run_wordcount = util.execute_command("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output",
                                            stderr=subprocess.STDOUT)
    print(run_wordcount.decode())

    # for some reason, without this sleep, I get an error from yarn saying
    # that it can't find the logs for the application id ..
    time.sleep(5)

    # write logs to file
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    # print output from MapTask
    util.print_blue("number of map spills")
    with open(LOG_FILE_PATH, 'r') as file:
        for line in file:

            if "org.apache.hadoop.mapred.MapTask" in line:
                print(line)

            """
            if "Finished spill" in line:
                print(line)
            """

        print("")

    util.hadoop_tear_down()
