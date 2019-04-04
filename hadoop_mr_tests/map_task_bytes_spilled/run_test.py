#!/usr/bin/env python3
import subprocess
import time

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util

if __name__=="__main__":
    #NUM_CHARACTERS = 49932 #expect 2 spills, 2 from spill thread and then final spill from map task
    #NUM_CHARACTERS = 49931 #expect 2 spills, 2 from spill thread and then final spill from map task
    NUM_CHARACTERS = 300000 #600 / (6 * (806 / 600)) # 1 spill, 50000 was 3 spills
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

    util.print_blue("mapreduce properties")
    util.hadoop_print_configuration_property_value("mapreduce.map.sort.spill.percent")
    util.hadoop_print_configuration_property_value("mapreduce.task.io.sort.mb")
    util.hadoop_print_configuration_property_value("mapreduce.output.fileoutputformat.compress")

    util.print_blue("generating input files")
    generate_input = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs dfs -put {}".format(util.generate_character_file(NUM_CHARACTERS))],
                                                stderr=subprocess.DEVNULL)
    print(generate_input.decode())


    util.print_blue("run mapreduce wordcout")
    run_wordcount = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output"],
                                            stderr=subprocess.STDOUT)
    print(run_wordcount.decode())

    # for some reason, without this sleep, I get an error from yarn saying
    # that it can't find the logs for the application id ..
    time.sleep(5)

    # write logs to file
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    # print out all map and reduce timestamps
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
