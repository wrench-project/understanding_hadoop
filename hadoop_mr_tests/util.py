#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil
import re
import datetime

def print_purple(a, **kwargs): print("\033[95m{}\033[00m".format(a), **kwargs)
def print_red(a, **kwargs): print("\033[91m{}\033[00m".format(a), **kwargs)
def print_blue(a, **kwargs): print("\033[94m{}\033[00m".format(a), **kwargs)

def MiB_to_bytes(MiB): return MiB * 1024 * 1024
def bytes_to_MiB(bytes): return bytes / (1024 * 1024)

# the directory to write any input files to before they are added to HDFS
INPUT_DIRECTORY = "/home/hadoop/input"

# paths to hadoop java apps
HADOOP_HOME = "/usr/local/hadoop"
HADOOP = HADOOP_HOME + "/bin/hadoop"
HDFS = HADOOP_HOME + "/bin/hdfs"
YARN = HADOOP_HOME + "/bin/yarn"

def make_input_directory():
    """
    Creates the directory INPUT_DIRECTORY, which is used to hold any
    input files.
    """
    try:
        # if the directory is already there, delete it and its contents
        if os.path.exists(INPUT_DIRECTORY):
            shutil.rmtree(INPUT_DIRECTORY)

        os.mkdir(INPUT_DIRECTORY)
    except OSError:
        print("Unable to create directory at {}".format(INPUT_DIRECTORY))
        print("Script failed..")
        sys.exit()

def hdfs_generate_custom_word_files(*word_lists):
    """
    Given a list of list of strings, generates a file with the contents
    of each list and adds them to HDFS. For example,
    calling hdfs_generate_word_files([["ab", "cd"], ["ef", "gh"]])
    will add two files with the contents "ab\ncd\n" and "ef\n\gh\n"
    respectively to hdfs.
    """
    make_input_directory()

    # create the file
    print_red("generating word {} file(s)".format(len(word_lists)))

    os.chdir(INPUT_DIRECTORY)
    for file_num, word_list in enumerate(*word_lists):
        FILE = "input_file_{}".format(file_num)
        with open(FILE, "w") as f:
            words = "\n".join(word_list)
            for word in words:
                f.write(word)

        print("Generated {} with {} words".format(FILE, len(word_list)))

    # add the file to hdfs
    generate_input = execute_command(HDFS + " dfs -put " + INPUT_DIRECTORY, stderr=subprocess.DEVNULL)
    print(generate_input.decode())

def hdfs_generate_word_file(num_characters_per_word, num_words):
    """
    Generates a single file named "input_file" containing num_words number of words
    where each character contains num_characters_per_word number of characters (not including \n) in
    INPUT_DIRECTORY, then adds this file to HDFS.
    For example, calling "path = generate_character_file(2,2)" will generate a file
    INPUT_DIRECTORY/input_file with the contents "rr\\nrr\\n", then place it in HDFS.
    """
    make_input_directory()

    # create the file
    print_red("generating word file")

    os.chdir(INPUT_DIRECTORY)
    FILE = "input_file"
    with open(FILE, "w") as f:
        words = [("r" * num_characters_per_word) + "\n" for w in range(num_words)]
        for word in words:
            f.write(word)

    print("Generated word file with {0} {1} character words".format(num_words, num_characters_per_word))

    # add the file to HDFS
    generate_input = execute_command(HDFS + " dfs -put " + INPUT_DIRECTORY, stderr=subprocess.DEVNULL)
    print(generate_input.decode())


def hdfs_generate_word_files(num_files, file_size_in_MiB):
    """
    Generates num_files file(s) each of size file_size_in_MiB in INPUT_DIRECTORY,
    then adds the files to HDFS. Each file contains the same word. For example
    "path = generate_word_files(2, 2)" will generate two files, each being 2 MiB
    and containing all the same word (each word is 'w' repeated some number of
    times such that we get the desired file size).
    """
    make_input_directory()

    # create the files
    print_red("generating word files")

    os.chdir(INPUT_DIRECTORY)
    WORD_SIZE = 1000 # characters
    for i in range(num_files):
        with open("input_file_{}".format(i), "w") as f:
            words = [("w" * (WORD_SIZE - 1)) + '\n' for w in range(MiB_to_bytes(file_size_in_MiB) // WORD_SIZE)]
            for word in words:
                f.write(word)

    print("Input files generated: {}, File Size: {} MiB".format(num_files, file_size_in_MiB))

    # add the files to HDFS
    generate_inputs = execute_command(HDFS + " dfs -put " + INPUT_DIRECTORY, stderr=subprocess.DEVNULL)
    print(generate_inputs.decode())


def execute_command(command, **kwargs):
    """
    Wrapper around subprocess.check_output().
    Executes command by spawning a process to be run as the "hadoop" user
    and waiting for it to return its output.
    """
    print_purple("executing: ", end='')
    print(command)
    return subprocess.check_output(["su", "hadoop", "-c", command], **kwargs)


def hadoop_start_up():
    """
    Starts up Hadoop in pseudodistributed mode.
    1. formats namenode
    2. starts HDFS
    3. starts YARN
    4. sets up a directory for user 'hadoop' in HDFS DFS
    """
    # format the namenode, the "master" in HDFS, which manages the filesystem namespace,
    # and knows what blocks reside at each datanode
    print_red("formatting the namenode")
    format_namenode = execute_command(HDFS + " namenode -format -y", stderr=subprocess.DEVNULL)
    print(format_namenode.decode())

    # start up hdfs
    print_red("starting hdfs")
    start_hdfs = execute_command(HADOOP_HOME + "/sbin/start-dfs.sh", stderr=subprocess.DEVNULL)
    print(start_hdfs.decode())

    # start yarn
    print_red("starting yarn")
    start_yarn = execute_command(HADOOP_HOME + "/sbin/start-yarn.sh", stderr=subprocess.DEVNULL)
    print(start_yarn.decode())

    # show currently running jvms
    # if we are correctly running in pseudodistributed mode, we should see the following:
    # Jps, ResourceManager, SecondaryNameNode, NodeManager, DataNode, NameNode
    print_red("currently running jvms")
    jps = execute_command("/usr/local/java/bin/jps", stderr=subprocess.DEVNULL)
    print(jps.decode())

    # create directory for hadoop user in hdfs at /user/hadoop
    print_red("creating hdfs folder for the hadoop user")
    execute_command(HDFS + " dfs -mkdir /user", stderr=subprocess.DEVNULL)
    execute_command(HDFS + " dfs -mkdir /user/hadoop", stderr=subprocess.DEVNULL)
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
    stop_hdfs = execute_command(HADOOP_HOME + "/sbin/stop-dfs.sh", stderr=subprocess.DEVNULL)
    print(stop_hdfs.decode())

    # stop yarn
    print_red("stopping yarn")
    stop_yarn = execute_command(HADOOP_HOME + "/sbin/stop-yarn.sh", stderr=subprocess.DEVNULL)
    print(stop_yarn.decode())

    # show currently running jvms
    # if we are correctly running in pseudodistributed mode, we should see the following: Jps, ResourceManager, SecondaryNameNode, NodeManager, DataNode, NameNode
    print_red("check that no jvms running hadoop daemons are present")
    jps = execute_command("/usr/local/java/bin/jps", stderr=subprocess.DEVNULL)
    print(jps.decode())

    # cleanup
    print_red("cleaning up /tmp/hadoop-*")
    rm_tmp = subprocess.check_output(["rm", "-rf", "/tmp/hadoop-hadoop", "/tmp/hadoop-yarn-hadoop"])


def hadoop_print_configuration_property_values(*properties):
    """
    Prints the configuration values for the given properties.
    """
    print_red("configuration property values")

    # get the property value and print it to stdout
    for property in properties:
        property_value = execute_command(HADOOP + " org.apache.hadoop.hdfs.tools.GetConf -confKey " + property, stderr=subprocess.DEVNULL)
        print("property: {}, value: {}".format(property, property_value.decode()))


def yarn_get_application_id():
    """
    Returns the applicationId with state FINISHED.
    """
    # get the list of applications with state FINISHED from YARN
    yarn_app_list = execute_command(YARN + " application -list -appStates FINISHED", stderr=subprocess.DEVNULL)

    # extract the only application_id that should be there
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

def log4j_get_iso8601_datetime(line):
    """
    Takes in as input a line generated from map reduce that is prefixed with
    an ISO8601 date and returns the date as a datetime object.

    This assumes the the date format set in hadoop/etc/log4j.properties is as follows:
    # Pattern format: Date LogLevel LoggerName LogMessage
    log4j.appender.RFA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n

    Example Line:
    2019-06-16 23:15:21,554 INFO [main] org.apache.hadoop.mapred.Merger: passNo: 2 numSegments: 3 factor: 3
    """
    tokens = line.split(' ')
    timestamp = tokens[0] + " " + tokens[1]

    # split on anything that isn't a digit
    pattern  = re.compile('\D')
    date_tokens = list(map(lambda num_str: int(num_str), pattern.split(timestamp)))

    #YEAR = 0
    #MONTH = 1
    #DAY = 2

    #HOUR = 3
    #MINUTE = 4
    #SECOND = 5
    FRACTION_OF_SECOND = 6

    date_tokens[FRACTION_OF_SECOND] = int((date_tokens[FRACTION_OF_SECOND] / 1000) * (10**6))

    return datetime.datetime(*date_tokens).isoformat()

def is_log4j_output_prefixed_with_date(line):
    """
    Takes in as input a line generated from map reduce and checks whether or not
    that line is prefixed with a date formatted in ISO8601.
    """
    tokens = line.split(' ')
    expected_date_string = tokens[0] + " " + tokens[1]

    date_pattern = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}')
    return True if date_pattern.match(expected_date_string) != None else False


def sort_log4j_by_timestamp(lines):
    """
    Takes in as input a list of lines generated from map reduce and returns a new
    list with the logs sorted by timestamp.
    """
    lines_by_timestamp = [(log4j_get_iso8601_datetime(line), line) for line in lines if is_log4j_output_prefixed_with_date(line)]
    lines_by_timestamp = sorted(lines_by_timestamp, key=lambda line: line[0])
    return [line[1] for line in lines_by_timestamp]
