# Understanding Hadoop 3.3.0

1. [General Overview](#general-overview)
2. [Hadoop Resources](#hadoop-resources)
3. [Repository Contents](#repository-contents)
4. [Tests and Current Findings](#tests-and-current-findings)
5. [Creating Your Own Test](#creating-your-own-test)
6. [Docker Image Structure](#docker-image-structure)

## General Overview

The purpose of this project is to better understand the inner workings of
Hadoop, primarily *MapReduce*(MR), the *Hadoop Distributed File System*(HDFS),
and its *resource negotiator* (YARN). Currently, the work of this project
focuses on reverse engineering MapReduce so that we can begin the development
of a faithful, WRENCH based, Hadoop MapReduce simulator.  

First, we list a number of resources (slides, literature, websites) that
provide high level explanations of the Hadoop components mentioned above.
Next, we outline the contents of this repository. Following that are
instructions on how to run a series of structural tests we have developed
to help understand MapReduce. This involves simulating a simple,
single node, Hadoop "cluster". Finally, we demonstrate how to use
the testing code to create your own tests if necessary.

## Hadoop Resources

Before diving into the low level implementation details of the Hadoop source code,
it can be worthwhile to read through the following material.

### Academic Articles
- MapReduce: simplified data processing on large clusters (ACM 10.1145/1327452.1327492)
- The Hadoop Distributed File System (978-1-4244-7153-9 IEEE)
- Apache Hadoop YARN: Yet Another Resource Manager (ACM 978-1-4503-2428-1)

### Books
- Hadoop: The Definitive Guide 4th Edition     
    - recommended reading:
        - Chapter 2: MapReduce
        - Chapter 3: The Hadoop Distributed File System
        - Chapter 4: YARN
        - Chapter 7: How MapReduce Works

### Web Sites
- [Official Apache Hadoop Site](https://hadoop.apache.org/docs/current/index.html)
- [Hadoop Internals](http://ercoppa.github.io/HadoopInternals/)
- [Slides: Hadoop Summit 2012 | Optimizing MapReduce Job Performance by Todd Lipcon](https://www.slideshare.net/cloudera/mr-perf)

## Repository Contents

This repository is organized as follows:

```
understanding_hadoop
├── hadoop_mr_tests
│   ├── Dockerfile
│   ├── build_and_run_test.sh       <-- script to run a test
│   ├── map_merge_parts             <-- a test
│   │   ├── Dockerfile
│   │   ├── pass_factor.py
│   │   └── run_test.py
│   ├── map_output_materialized_bytes       <-- a test
│   │   ├── Dockerfile
│   │   ├── mapred-site.xml
│   │   └── run_test.py
│   ├── number_of_map_tasks     <-- a test
│   │   ├── Dockerfile
│   │   └── run_test.py
│   ├── reduce_merge_parts      <-- a test
│   │   ├── Dockerfile
│   │   ├── reduce_merging_trace.svg
│   │   └── run_test.py
│   ├── remove_hadoop_env_container.sh
│   ├── util.py     <-- collection of functions/wrappers around hadoop calls used in tests
│   └── zero_compress.py        <-- some functions to see how zero compression works in hadoop
└── hadoop_pseudodistributed_mode_container
    ├── Dockerfile
    ├── build_and_push_hadoop_run_env_image.sh      <-- script to build a docker image that serves as the environment for running hadoop
    ├── entrypoint.sh
    ├── hadoop      <-- hadoop source
    │   ├── BUILDING.txt
    │   ├── Dockerfile
    │   ├── LICENSE.txt
    │   ├── NOTICE.txt
    │   ├── README.txt
    │   ├── build_and_push_hadoop_image.sh       <-- script to build the source code, and create a docker image of it locally
    │   ├── custom_configs
    │   ├── dev-support
    │   ├── hadoop-assemblies
    │   ├── hadoop-build-tools
    │   ├── hadoop-client-modules
    │   ├── hadoop-cloud-storage-project
    │   ├── hadoop-common-project
    │   ├── hadoop-dist     <-- compiled code ends up in here
    │   ├── hadoop-hdds
    │   ├── hadoop-hdfs-project
    │   ├── hadoop-main.iml
    │   ├── hadoop-mapreduce-project
    │   ├── hadoop-maven-plugins
    │   ├── hadoop-minicluster
    │   ├── hadoop-ozone
    │   ├── hadoop-project
    │   ├── hadoop-project-dist
    │   ├── hadoop-tools
    │   ├── hadoop-yarn-project
    │   ├── patchprocess
    │   ├── pom.xml
    │   ├── start-build-env.sh
    │   └── target
    ├── jdk-8u201-linux-x64.tar.gz
    └── notes.txt
```

- `hadoop_mr_tests` directory: contains tests and any source code used in those tests
    - each test is contained within its own directory and is structured as follows:
    ```
    test_name/
    ├── Dockerfile
    ├── other_related_files
    └── run_test.py
    ```
- `hadoop_pseudodistributed_mode_container`: contains the hadoop source code, and scripts
  to build the environment used to run the tests

## Tests and Current Findings

There are currently 4 tests in the `understanding_hadoop/hadoop_mr_tests`
directory. Each test was developed by first getting an understanding of what
happens during MR based on information provided from the resources above,
then reading through pertinent source code in
`understanding_hadoop/hadoop_pseudodistributed_mode_container/hadoop`
(which has been cloned from `https://github.com/apache/hadoop.git`), making
assumptions about what that source code actually does, then testing our
assumptions by running a **word count** MR program on some input, and finally
(`understanding_hadoop/hadoop_pseudodistributed_mode_container/hadoop/hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/WordCount.java`)
analyzing an aggregation of logs produced by Hadoop (some of which were added
into the source code by us to get more information than what was already logged).

Tests are set up such that they can be run by navigating to
`understanding_hadoop/hadoop_mr_tests`, then running the command
`./build_and_run_test.sh <name of directory that contains the test (but without trailing
forward slash)`.

The following subsections discuss each test, their findings,
and how to run them.

### Test: Number of Map Tasks

#### Description

Test to answer the question: **How many map tasks are started given
some input(s)?** The HDFS block size is fixed at 16 MiB, which is set in
`understanding_hadoop/hadoop_pseudodistributed_mode_container/hadoop/custom_configs/hdfs-site.xml`.
By varying the number of input files, and their sizes, we can see how many map
tasks are started based on output from the word count program.


#### Running the Test

1. Navigate to `hadoop_mr_tests`.
2. Run `./build_and_run_test number_of_map_tasks`.
  - This may take a few minutes to complete.
3. Visually inspect output.
  - We care about the line that says: "Launched map tasks="

The output from this test will resemble the following:

```
********************************************************************************************
TestCase(num_files=1, file_size_in_MiB=1, description='single file smaller than block size')
********************************************************************************************
formatting the namenode
executing: /usr/local/hadoop/bin/hdfs namenode -format -y
Formatting using clusterid: CID-b04b700b-3161-427d-9e80-cc2d225701c8

starting hdfs
executing: /usr/local/hadoop/sbin/start-dfs.sh
Starting namenodes on [localhost]
localhost: Warning: Permanently added 'localhost' (ECDSA) to the list of known hosts.
Starting datanodes
Starting secondary namenodes [e3538724a1e9]
e3538724a1e9: Warning: Permanently added 'e3538724a1e9,172.17.0.2' (ECDSA) to the list of known hosts.

starting yarn
executing: /usr/local/hadoop/sbin/start-yarn.sh
Starting resourcemanager
Starting nodemanagers

currently running jvms
executing: /usr/local/java/bin/jps
834 ResourceManager
1315 Jps
598 SecondaryNameNode
361 DataNode
954 NodeManager
237 NameNode

creating hdfs folder for the hadoop user
executing: /usr/local/hadoop/bin/hdfs dfs -mkdir /user
executing: /usr/local/hadoop/bin/hdfs dfs -mkdir /user/hadoop
/user/hadoop created in hdfs

generating word files
Input files generated: 1, File Size: 1 MiB
executing: /usr/local/hadoop/bin/hdfs dfs -put /home/hadoop/input

configuration property values
executing: /usr/local/hadoop/bin/hadoop org.apache.hadoop.hdfs.tools.GetConf -confKey dfs.block.size
property: dfs.block.size, value: 16777216

run mapreduce wordcount
2019-07-03 21:27:37,886 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2019-07-03 21:27:38,608 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
2019-07-03 21:27:39,235 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/hadoop/.staging/job_1562189247326_0001
2019-07-03 21:27:39,509 INFO input.FileInputFormat: Total input files to process : 1
2019-07-03 21:27:40,409 INFO mapreduce.JobSubmitter: number of splits:1
2019-07-03 21:27:40,470 INFO Configuration.deprecation: yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
2019-07-03 21:27:40,984 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1562189247326_0001
2019-07-03 21:27:40,986 INFO mapreduce.JobSubmitter: Executing with tokens: []
2019-07-03 21:27:41,200 INFO conf.Configuration: resource-types.xml not found
2019-07-03 21:27:41,200 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2019-07-03 21:27:41,579 INFO impl.YarnClientImpl: Submitted application application_1562189247326_0001
2019-07-03 21:27:41,632 INFO mapreduce.Job: The url to track the job: http://e3538724a1e9:8088/proxy/application_1562189247326_0001/
2019-07-03 21:27:41,633 INFO mapreduce.Job: Running job: job_1562189247326_0001
2019-07-03 21:27:49,779 INFO mapreduce.Job: Job job_1562189247326_0001 running in uber mode : false
2019-07-03 21:27:49,781 INFO mapreduce.Job:  map 0% reduce 0%
2019-07-03 21:27:54,817 INFO mapreduce.Job:  map 100% reduce 0%
2019-07-03 21:27:59,861 INFO mapreduce.Job:  map 100% reduce 100%
2019-07-03 21:28:01,883 INFO mapreduce.Job: Job job_1562189247326_0001 completed successfully
2019-07-03 21:28:01,996 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=1058486
                FILE: Number of bytes written=2564427
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=1048117
                HDFS: Number of bytes written=1005
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=21104
                Total time spent by all reduces in occupied slots (ms)=22960
                Total time spent by all map tasks (ms)=2638
                Total time spent by all reduce tasks (ms)=2870
                Total vcore-milliseconds taken by all map tasks=2638
                Total vcore-milliseconds taken by all reduce tasks=2870
                Total megabyte-milliseconds taken by all map tasks=2701312
                Total megabyte-milliseconds taken by all reduce tasks=2938880
        Map-Reduce Framework
                Map input records=1048
                Map output records=1048
                Map output bytes=1054288
                Map output materialized bytes=1058486
                Input split bytes=117
                Combine input records=0
                Combine output records=0
                Reduce input groups=1
                Reduce shuffle bytes=1058486
                Reduce input records=1048
                Reduce output records=1
                Spilled Records=2096
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=83
                CPU time spent (ms)=1230
                Physical memory (bytes) snapshot=468463616
                Virtual memory (bytes) snapshot=5266083840
                Total committed heap usage (bytes)=364904448
                Peak Map Physical memory (bytes)=286240768
                Peak Map Virtual memory (bytes)=2630541312
                Peak Reduce Physical memory (bytes)=182222848
                Peak Reduce Virtual memory (bytes)=2635542528
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=1048000
        File Output Format Counters
                Bytes Written=1005

stopping hdfs
executing: /usr/local/hadoop/sbin/stop-dfs.sh
Stopping namenodes on [localhost]
localhost: WARNING: namenode did not stop gracefully after 5 seconds: Trying to kill with kill -9
localhost: ERROR: Unable to kill 237
Stopping datanodes
localhost: WARNING: datanode did not stop gracefully after 5 seconds: Trying to kill with kill -9
localhost: ERROR: Unable to kill 361
Stopping secondary namenodes [e3538724a1e9]
e3538724a1e9: WARNING: secondarynamenode did not stop gracefully after 5 seconds: Trying to kill with kill -9
e3538724a1e9: ERROR: Unable to kill 598

stopping yarn
executing: /usr/local/hadoop/sbin/stop-yarn.sh
Stopping nodemanagers
localhost: WARNING: nodemanager did not stop gracefully after 5 seconds: Trying to kill with kill -9
localhost: ERROR: Unable to kill 954
Stopping resourcemanager

check that no jvms running hadoop daemons are present
executing: /usr/local/java/bin/jps
2938 Jps

cleaning up /tmp/hadoop-*
wrench-computer:hadoop_mr_tests ryan$
```

#### Result

In `hadoop_mr_tests/number_of_map_tasks/run_test.py`, we have the following
test cases and their results:
```python
TestCase = namedtuple("TestCase", ["num_files", "file_size_in_MiB", "description"])

test_cases = [
    TestCase(1, 1, "single file smaller than block size"),          # Launched map tasks=1
    TestCase(1, 20, "single file larger than block size"),          # Launched map tasks=2
    TestCase(2, 1, "two small files that fit into a single block"), # Launched map tasks=2
    TestCase(2, 20, "two files each larger than a block")           # Launched map tasks=4
]
```

### Test: Map Output Materialized Bytes

#### Description
Test to determine the number number of spill files and map output materialized bytes
based on map inputs, map output buffer size, and map sort spill percent
assuming a single mapper, 1 or more reducers, and that no combiners are run.

See `hadoop_mr_tests/map_output_materialized_bytes/run_test.py` for more
information and **results**.

Hadoop uses zero compressed Long and Int values which affect how much data
can be fit into buffers and affects file sizes. We have re-written their implementation of this
compression in `hadoop_mr_tests/zero_compress.py` and use it to estimate
the number of bytes a Long or Int will use whenever they are compressed. If you run `./zero_compress.py`,
it will also print some examples of how this works. The following is its output:
```
-9223372036854775808:  10000000 01111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 len: 9  expected len: 9
         -2147483648:                                      10000100 01111111 11111111 11111111 11111111 len: 5  expected len: 5
            -8388608:                                               10000101 01111111 11111111 11111111 len: 4  expected len: 4
              -32768:                                                        10000110 01111111 11111111 len: 3  expected len: 3
                -128:                                                                 10000111 01111111 len: 2  expected len: 2
                -112:                                                                          10010000 len: 1  expected len: 1
                  -1:                                                                          11111111 len: 1  expected len: 1
                   1:                                                                          00000001 len: 1  expected len: 1
                 128:                                                                 10001111 10000000 len: 2  expected len: 2
               32768:                                                        10001110 10000000 00000000 len: 3  expected len: 3
             8388608:                                               10001101 10000000 00000000 00000000 len: 4  expected len: 4
          2147483648:                                      10001100 10000000 00000000 00000000 00000000 len: 5  expected len: 5
 9223372036854775808:  10001000 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 len: 9  expected len: 9
```

The leftmost column is a signed number in decimal. The next column to the right
represents the binary representation of that number after it has been compressed.
For example, 1 as a Long could use 8 bytes, however with their compression, 1
uses a single byte. Note that, 9223372036854775808 uses 9 bytes, which is 1 byte
more than what it would use as a normal 8 byte Long (this is a tradeoff they made).

#### Running the Test
1. Navigate to `hadoop_mr_tests`.
2. Run `./build_and_run_test map_output_materialized_bytes`.
  - This may take a few minutes to complete.
3. Visually inspect output.

### Test: Map Merge Parts

#### Description
Test to determine how a map task merges its spill files. In order to observe what
happens during this process, this test runs only a single mapper and reducer.
The map task's sort buffer size is fixed at 1 MB, and the its spill percent is
set at 50% which will cause the map task to begin spilling its buffer when its
capacity reaches about 0.5 MB. Additionally, the sort factor (the maximum
number of streams to merge at once) is set at 3. Each test case changes the number
of spill files generated by the map task so that the merge behavior can be observed
through the logs.

#### Running the Test

1. Navigate to `hadoop_mr_tests`.
2. Run `./build_and_run_test map_merge_parts`.
  - This may take a few minutes to complete.
3. Visually inspect output.
  - Colored in blue are logs regarding the creation of spill files. This should match the
    number we have set for the test.
  - Colored in purple are logs regarding the merging of spill files. In all the test cases,
    there will first be logs regarding the merge being run by the map task. Following that
    are logs regarding the merge being run by the reduce task (which should always show
      that only a single file is being merged).

The output from this test will resemble the following if `test=T2` on line 105 of
`/hadoop_mr_tests/map_merge_parts/run_test.py`:

```
2019-07-12 01:14:32,075 INFO [SpillThread] org.apache.hadoop.mapred.MapTask: Finished spill 0, file path: attempt_1562894051760_0001_m_000000_0_spill_0.out

2019-07-12 01:14:32,171 INFO [SpillThread] org.apache.hadoop.mapred.MapTask: Finished spill 1, file path: attempt_1562894051760_0001_m_000000_0_spill_1.out

2019-07-12 01:14:32,254 INFO [main] org.apache.hadoop.mapred.MapTask: Finished spill 2, file path: attempt_1562894051760_0001_m_000000_0_spill_2.out

2019-07-12 01:14:32,261 INFO [main] org.apache.hadoop.mapred.Merger: Merging 3 sorted segments

2019-07-12 01:14:32,263 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 3 segments left of total size: 524294 bytes

2019-07-12 01:14:36,409 INFO [main] org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl: MergerManager: memoryLimit=535088320, maxSingleShuffleLimit=133772080, mergeThreshold=353158304, ioSortFactor=3, memToMemMergeOutputsThr
eshold=3

2019-07-12 01:14:36,598 INFO [main] org.apache.hadoop.mapred.Merger: Merging 1 sorted segments

2019-07-12 01:14:36,598 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 524286 bytes

2019-07-12 01:14:36,646 INFO [main] org.apache.hadoop.mapred.Merger: Merging 1 sorted segments

2019-07-12 01:14:36,647 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 524286 bytes
```

#### Result

Map side merges will always run once once the mapper has gone through all of its
input. The merge will generate a single, partitioned, sorted output file. At each
pass of the merge, at most "mapreduce.task.io.sort.factor" spill files are merged into a single
intermediate spill file. The logs (as shown above) show, at each pass, how many
files are being merged into one. Additionally, without running the test, we can
calculate this by using `/hadoop_mr_tests/map_merge_parts/pass_factor.py`. For
example, given that "mapreduce.task.io.sort.factor" is set at 5, and that
a mapper produces 23 output files, we expect the following merge behavior:
```
simulate_merges_using_variable_pass_factor(5, 23)
pass: 1, current_num_segments: 23, pass_factor: 3, remaining_num_segments: 21
pass: 2, current_num_segments: 21, pass_factor: 5, remaining_num_segments: 17
pass: 3, current_num_segments: 17, pass_factor: 5, remaining_num_segments: 13
pass: 4, current_num_segments: 13, pass_factor: 5, remaining_num_segments: 9
pass: 5, current_num_segments: 9, pass_factor: 5, remaining_num_segments: 5
pass: 6, current_num_segments: 5, pass_factor: 5, remaining_num_segments: 1
```

The merge algorithm uses a priority queue of InputBuffers at each pass, therefore
setting "mapreduce.task.io.sort.factor" gives the user a way to bound the size of
priority queue, which puts a bound on the cost of maintaining the priority queue.

### Test: Reduce Merge Parts

### Description

This test looks at how a reduce task perform's the merging of map output
files it receives. In this test, the number of map output files
and the size of those files that a single reducer receives is varied so that
the behavior of the merge by the reduce task can be observed from its logs.

The following properties are fixed throughout this test:
```
MAPREDUCE_PROPERTIES = {
    "mapreduce.reduce.memory.mb": 1024, # memory allotted for reduce task container
    "mapreduce.reduce.java.opts": "-Xmx100m", # reduce jvm process heap size
    "mapreduce.reduce.shuffle.parallelcopies": 5,
    "mapreduce.task.io.sort.factor": 3,
    "mapreduce.reduce.shuffle.input.buffer.percent": 0.1,
    "mapreduce.reduce.shuffle.memory.limit.percent": 0.2,
    "mapreduce.reduce.shuffle.merge.percent" : 0.5,
    "mapreduce.reduce.merge.inmem.threshold": 1000,
    "mapreduce.reduce.input.buffer.percent": 0.5
}
```

This means that reduce tasks are configured as follows:

```
allocated memory: 1024 MB
jvm heap size: 100 MB (a little less in practice as Runtime.getRuntime.maxMemory() returns 93323264 bytes)
input buffer size: (93323264 * 0.1) = 9332326.4 bytes or about 9.3 MB
merge threshold: (9.3 MB * 0.5) = 4666163.2 bytes or about 4.6 MB
max single shuffle limit: 1866465.28 bytes or about 1.9 MB
reduce function input buffer: (9.3 MB * 0.5) = 4666163.2 bytes or about 4.6 MB
```

When a reduce task is started, but before it has completed merging all map
output files assigned to it, it creates a number of child threads. First, it
creates two threads responsible for merging files: the InMemoryMerger and the
OnDiskMerger. Then, the main thread will create a number of Fetcher threads
that will copy output files from map tasks.
`/hadoop_mr_tests/reduce_merge_parts/reduce_merging_trace.svg` illustrates
the stack trace of `ReduceTask.run()`.

### Running the Test

1. Navigate to `hadoop_mr_tests`.
2. Run `./build_and_run_test reduce_merge_parts`.
  - This may take a few minutes to complete.
  - set the variable `test` to any of the 9 tests or create your own
3. Visually inspect output.
4. Navigate to `/hadoop_mr_tests/reduce_merge_parts`
5. Run `./get_strace_output.sh` to get strace output from whenever a merge is run. 
    This script will copy strace output files from the container to the host machine
    and print out the name of the file that contains output specifically from the thread
    that called the merge. To make modifications to the strace call, see
    `/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/ReduceTask.java`
    line 392.  

### Results

Results will vary by test. Go to `/hadoop_mr_tests/reduce_merge_parts/run_test.py`
for the results of each test.

## Creating Your Own Test

You may want to create your own test to observe some aspect of a MR execution
that has not yet been covered in the above tests. This involves writing a
script to run the test, and possibly modifying the hadoop source code.

Note that these tests are limited in that hadoop is run in pseudo distributed
mode where we have multiple jvm processes running on a single host (the container).
Multiple mappers and reducers may be started, however they are still running
on the same host and therefore these tests do not simulate a hadoop cluster.
Additionally, there is no HDFS file replication (typically a factor of 3) since
everything is running in a single host (Adding HDFS replication would be pointless
here).  

### Writing A Test

1. Create a directory in `/hadoop_mr_tests/<name_of_your_test>`
2. In `/hadoop_mr_tests/<name_of_your_test>`, create a `run_test.py`
    - for reference, take a look at the tests that currently exist
    - this file should look something like the following:
    ```python
    #!/usr/bin/env python3
    import subprocess
    import time
    import util

    if __name__=="__main__":
      # mapreduce properties that will be submitted along with the word count job
      MAPREDUCE_PROPERTIES = {
        "property_name": "value", ...
      }

      # starts up an environment to run hadoop in pseudo distributed mode, that
      # is a single node with separate jvm processes
      util.hadoop_start_up()

      # generate input file(s) that will serve as input to the word count job
      # and add them to hdfs
      util.hdfs_generate_custom_word_files(<list of lists of words>)

      # submit the word count job, a long with specific mapreduce properties
      util.execute_command(("/usr/local/hadoop/bin/hadoop jar "
                              "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar "
                              "wordcount {} input output").format(" ".join(["-D {}={}".format(property, value) for property, value in MAPREDUCE_PROPERTIES.items()])),
                                  stderr=subprocess.STDOUT)

    # this sleep needs to be here because moving to the next statement too fast
    # causes some errors...                              
    time.sleep(5)

    # write logs to file;
    # this function obtains the aggregated logs from all the processes created
    # during the most recent job (the one we just ran above), and writes them
    # to a file; note that they may be chronologically out of order
    LOG_FILE_PATH = util.yarn_write_logs_to_file(util.yarn_get_application_id())

    logs = []

    with open(LOG_FILE_PATH, 'r') as file:
        for line in file:
          logs.append(line)

    util.hadoop_tear_down()

    # loop through "logs" and pull out ones that are relevant to your test
    ```
3. In '/hadoop_mr_tests/<name_of_your_test>' create a 'Dockerfile'
    - for reference, take a look a the tests that currently exist
    - this file should look something like the following
    ```
    FROM wrenchproject/understanding-hadoop:test-util

    USER root

    COPY run_test.py /home/hadoop/run_test.py
    RUN chmod u+x /home/hadoop/run_test.p

    ENTRYPOINT ["/etc/entrypoint.sh"]
    CMD ["python3", "run_test.py"]
    ```
4. To run your test, navigate to '/hadoop_mr_tests' and run
    './build_and_run_test <name_of_your_test>'
    - this will build the docker image containing your test and run it

### Modifying the Hadoop Source Code

In some cases, you may want to edit the hadoop source code to see how
it affects the MR word count execution or to add new logs. If you edit the
source code, you need to recompile it, and rebuild the docker image. To
recompile the code:
1. Navigate to '/hadoop_pseudodistributed_mode_container/hadoop'
2. Run `./build_and_push_hadoop_run_env_image.sh`

## Docker Image Structure

```
+-----------------------------------------------------------+
| wrenchproject/understanding-hadoop:hadoop-run-environment | # dependencies needed to install/run hadoop
+-------+---------------------------------------------------+
        |
        |
        v
+-------+-----------------------------------+
| wrenchproject/understanding-hadoop:hadoop | # hadoop source code
+------------------+------------------------+
                   |
                   v
+-------------------+--------------------------+
| wrenchproject/understanding-hadoop:test-util | # contains code used by one or more tests 
+---------------------------+------------------+
                            |
                            v
+---------------------------+------------------+
| wrenchproject/understanding-hadoop:test-name | # tests
+----------------------------------------------+
```
