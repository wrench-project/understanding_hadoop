#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil
import re

def print_purple(a): print("\033[95m{}\033[00m".format(a))
def print_red(a): print("\033[91m{}\033[00m".format(a))
def print_blue(a): print("\033[94m{}\033[00m".format(a))

def MiB_to_bytes(MiB):
    return MiB * 1024 * 1024

def generate_inputs(num_files, file_size_in_MiB):
    """
    Generates an input directory with a specified number of same sized files
    and returns the path to that directory.
    """

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

def hadoop_start_up():
    """
    Starts up Hadoop in pseudodistributed mode.
    1. formats namenode
    2. starts HDFS
    3. starts YARN
    4. sets up a directory for user 'hadoop' in HDFS DFS
    """

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


def hadoop_tear_down():
    """
    Shuts down the Hadoop environment.
    1. stops HDFS
    2. stops YARN
    3. cleans up tmp files
    """
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

def yarn_get_application_id():
    """
    Returns the applicationId with state FINISHED.
    """
    # get the id of the application that was just run from yarn
    yarn_app_list = subprocess.check_output(["su", "hadoop", "-c", "/usr/local/hadoop/bin/yarn application -list -appStates FINISHED"],
                                            stderr=subprocess.DEVNULL)

    application_id = re.search(r'application_[0-9]+_[0-9]+', yarn_app_list.decode())
    return application_id.group()

def yarn_write_logs_to_file(application_id):
    """
    Writes all logs of the given application_id to /home/hadoop/yarn_logs.txt and returns that path.
    """
    LOG_FILE_PATH = "/home/hadoop/yarn_logs.txt"

    # get the logs of the finished application
    print_red("writing logs of {0} to {1}".format(application_id, LOG_FILE_PATH))

    with open(LOG_FILE_PATH, "wb") as log_file:
        subprocess.Popen(["su", "hadoop", "-c", "/usr/local/hadoop/bin/yarn logs -applicationId {0} -appOwner hadoop".format(application_id)],
                        stdout=log_file).wait()


    return LOG_FILE_PATH
