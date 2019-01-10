#!/usr/bin/env python3
import argparse
import subprocess
import os
import sys

# get command line args
parser = argparse.ArgumentParser()
parser.add_argument("num_files", help="the number of separate files to generate", type=int)
parser.add_argument("file_size", help="size of each file in the range [1, 100] MB", type=int)
args = parser.parse_args()

# helper function to translate MiB to bytes
megabytes_to_bytes = lambda MiB : MiB * 1048576


# generate input files
INPUT_DIRECTORY = "/home/hadoop/input"
try:
    os.mkdir(INPUT_DIRECTORY)
except OSError:
    print("Unable to create directory at {}".format(INPUT_DIRECTORY))
    print("Script failed..")
    sys.exit()

os.chdir(INPUT_DIRECTORY)
WORD_SIZE = 1000
for i in range(args.num_files):
    with open("input_file_{}".format(i), "w") as f:
        words = [("w" * (WORD_SIZE - 1)) + '\n' for w in range(megabytes_to_bytes(args.file_size) // WORD_SIZE)]
        for word in words:
            f.write(word)

# add input files to hdfs
add_files_to_hdfs_cmd = 'su hadoop -c "$HADOOP_HOME/bin/hdfs dfs -put {}/ input"'.format(INPUT_DIRECTORY)
subprocess.Popen(add_files_to_hdfs_cmd, shell=True).wait()

# show block size
show_block_size_cmd = 'su hadoop -c "$HADOOP_HOME/bin/hdfs getconf -confKey dfs.block.size"'
subprocess.Popen(show_block_size_cmd, shell=True).wait()

# show number of blocks used
show_blocks_for_first_file = 'su hadoop -c "$HADOOP_HOME/bin/hdfs fsck /user/hadoop/input/input_file_0 -files -blocks"'
subprocess.Popen(show_blocks_for_first_file, shell=True).wait()

# run the mapreduce job
submit_job_cmd = 'su hadoop -c "$HADOOP_HOME/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output"'
subprocess.Popen(submit_job_cmd, shell=True).wait()

# print output
print_output_cmd = 'su hadoop -c "$HADOOP_HOME/bin/hdfs dfs -cat /user/hadoop/output/*"'
subprocess.Popen(print_output_cmd, shell=True).wait()
