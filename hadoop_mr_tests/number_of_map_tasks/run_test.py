#!/usr/bin/env python3
import subprocess
import os
import sys
import shutil
from collections import namedtuple

def print_purple(a): print("\033[95m{}\033[00m".format(a))
def print_red(a): print("\033[91m{}\033[00m" .format(a))

def MiB_to_bytes(MiB):
    return MiB * 1024 * 1024

def generate_inputs(num_files, file_size_in_MiB):
    """Generates an input directory with a specified number of same sized files and returns
    the path to that directory."""

    # generate input files
    INPUT_DIRECTORY = "/home/hadoop/input"
    try:
        # if the directory is already there, delete it and its contents
        if os.path.exists(INPUT_DIRECTORY):
            shutil.rmtree(INPUT_DIRECTORY)

        os.mkdir(INPUT_DIRECTORY)
    except OSError:
        print("Unable to create directory at {}".format(INPUT_DIRECTORY))
        print("Script failed..")
        sys.exit()

    os.chdir(INPUT_DIRECTORY)
    WORD_SIZE = 1000 # characters
    for i in range(num_files):
        with open("input_file_{}".format(i), "w") as f:
            words = [("w" * (WORD_SIZE - 1)) + '\n' for w in range(MiB_to_bytes(file_size_in_MiB) // WORD_SIZE)]
            for word in words:
                f.write(word)

    print("Input files generated: {}, File Size: {} MiB".format(num_files, file_size_in_MiB))
    print("Path to inputs: {}".format(INPUT_DIRECTORY))

    return INPUT_DIRECTORY

"""
Tests the number of map tasks launched by YARN based on number of input files, input file size,
and HDFS block size. When a test case runs, the number of map tasks launched can be viewed under
"run mapreduce wordcount" in the section titled "Job Counters". Note that tests are slow.
"""
if __name__=="__main__":
    TestCase = namedtuple("TestCase", ["num_files", "file_size_in_MiB", "description"])

    test_cases = [
        TestCase(1, 1, "single file smaller than block size"),
        TestCase(1, 20, "single file larger than block size"),
        TestCase(2, 1, "two small files that fit into a single block"),
        TestCase(2, 20, "two files each larger than a block")
    ]

    for test_case in test_cases:
        print_purple("*" * len(str(test_case)))
        print_purple(test_case)
        print_purple("*" * len(str(test_case)))

        # format the namenode
        print_red("formatting the namenode")
        format_namenode = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs namenode -format -y"],
                                                    stderr=subprocess.DEVNULL)
        print(format_namenode.decode())

        # start up hdfs
        print_red("starting hdfs")
        start_hdfs = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/sbin/start-dfs.sh"],
                                                stderr=subprocess.DEVNULL)
        print(start_hdfs.decode())

        # show hdfs block size
        print_red("hdfs block size")
        show_block_size = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs getconf -confKey dfs.blocksize"],
                                                    stderr=subprocess.DEVNULL)
        print("{} MiB \n".format(int(show_block_size.decode()) / (1024 * 1024)))

        # start yarn
        print_red("starting yarn")
        start_yarn = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/sbin/start-yarn.sh"],
                                            stderr=subprocess.DEVNULL)
        print(start_yarn.decode())

        # show currently running jvms
        # if we are correctly running in pseudodistributed mode, we should see the following: Jps, ResourceManager, SecondaryNameNode, NodeManager, DataNode, NameNode
        print_red("currently running jvms")
        jps = subprocess.check_output(["jps"])
        print(jps.decode())

        # create directory for hadoop user in hdfs
        print_red("creating hdfs folder for the hadoop user")
        dfs_mkdir_user = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs dfs -mkdir /user"],
                                                stderr=subprocess.DEVNULL)
        dfs_mkdir_user_hadoop = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs dfs -mkdir /user/hadoop"],
                                                        stderr=subprocess.DEVNULL)
        print("/user/hadoop created in hdfs \n")

        # generate and add the input files into hdfs directory /user/hadoop/inputs
        print_red("generating input files and adding them to /user/hadoop/input")
        generate_input = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hdfs dfs -put {}".format(generate_inputs(test_case.num_files, test_case.file_size_in_MiB))],
                                                    stderr=subprocess.DEVNULL)
        print(generate_input.decode())

        # run map reduce wordcount on input
        print_red("run mapreduce wordcount")
        run_wordcount = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount input output"],
                                                stderr=subprocess.STDOUT)
        print(run_wordcount.decode())

        # stop hdfs
        print_red("stopping hdfs")
        stop_hdfs = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/sbin/stop-dfs.sh"],
                                            stderr=subprocess.DEVNULL)
        print(stop_hdfs.decode())

        # stop yarn
        print_red("stopping yarn")
        stop_yarn = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/sbin/stop-yarn.sh"],
                                            stderr=subprocess.DEVNULL)
        print(stop_yarn.decode())

        # show currently running jvms
        # if we are correctly running in pseudodistributed mode, we should see the following: Jps, ResourceManager, SecondaryNameNode, NodeManager, DataNode, NameNode
        print_red("check that no jvms running hadoop daemons are present")
        jps = subprocess.check_output(["jps"])
        print(jps.decode())

        # cleanup
        print_red("cleaning up /tmp/hadoop-*")
        rm_tmp = subprocess.check_output(["rm", "-rf", "/tmp/hadoop-hadoop", "/tmp/hadoop-yarn-hadoop"])
