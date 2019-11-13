# Reverse Engineering Hadoop

Thanks to the work done by Ryan Tanaka, we have been able to extrapolate many
subtle details that go into running a Hadoop job.  In this section, all data is
sourced from the original results that can be found in the comments in the
[source code](https://github.com/wrench-project/understanding_hadoop/tree/master/hadoop_mr_tests),
as well as in the original README.

The following four tests were ran, and will be summarized:

1. [Number of map Tasks](#number-of-map-tasks)
2. [Map Output Materialized Bytes](#map-output-materialized-bytes)
3. [Map Merge Parts](#map-merge-parts)
4. [Reduce Merge Parts](#reduce-merged-parts)

What this section will attempt to do is consolidate the valuable information
obtained from running these tests, and integrate the results from code comments 
into a cohesive overview of the findings.

## [Number of map Tasks](./top_level_overview.markdown#Point-1-&-2:-Splitting-and-Assigning-Tasks)

<details>
  <summary>
    <b id="number-of-map-tasks">Click to see test output</b>
  </summary>

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
</details>

When a Hadoop job starts up, the input data is divided into
*N* splits, where *N* is equal to the `ceiling(file_size/HDFS_block_size)`.
The `HDFS_block_size` is a configurable property in the `hdfs-default.xml` file
(note that this is not the actual name of the property).
For this test, the block size was fixed at `16 MiB`.  The idea behind
this test was to process a number of inputs with varying input sizesÂ
to determine how many map tasks are created as a function of the input.

The results from running four separate test cases are as follows:

```python3
TestCase = namedtuple("TestCase", ["num_files", "file_size_in_MiB", "description"])

test_cases = [
    TestCase(1, 1, "single file smaller than block size"),          # Launched map tasks=1
    TestCase(1, 20, "single file larger than block size"),          # Launched map tasks=2
    TestCase(2, 1, "two small files that fit into a single block"), # Launched map tasks=2
    TestCase(2, 20, "two files each larger than a block")           # Launched map tasks=4
]
```

So we can represent this as the following simple block of pseudocode:

```
# Read input files
for file in input_files:
  if file.size() < block_size:
    map_task.launch(file)
  else:
    while file.size() > block_size:
      split = file.split(block_size) # split off a `block_size` sized chunk
      map_task.launch(split)
    map_task.launch(file)
```
While this isn't a true representation of how map tasks are delegated, it helps
us to see the relationship between file, file size, and number of map tasks.

Let's verify this checks out.
* When `input_files == 1 and file.size() < block_size`
we enter our first check a single time, and a single `map_task` is launched.
* The second case, `input_files == 1 and file.size() > block_size`, so we enter
the `else` block and split the file, a single map task is launched.  Our next
check on the `while` loop returns `False`, so we exit and launch one more map
task.  Thus, a total of two map tasks are launched.
* In the third test we iterate over two files, each entering the first `if`
block, resulting in two map tasks.
* For our final test case, we see the same result as in our second test case
repeated twice. So, we end up with four map tasks.

We can therefore represent the number of map tasks as a function of number of
files and file size:

`number_of_map_tasks = sum(ceil(s_i / b))`

Where `N` is the number of files, `s_i` is the size of the `ith` file, and `b`
is the block size.

## [Map Output Materialized Bytes](./top_level_overview.markdown#Point-3-&-4:-Mapping-&-Handling-Intermediate-map-Values)

<details>
  <summary>
    <b id="map-output-materialized-bytes">Click to see test output</b>
  </summary>

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
</details>

This test was built to check the following:

> Test to determine the number of spill files and map output materialized bytes
based on map inputs, map output buffer size, and map sort spill percent assuming
a single mapper, 1 or more reducers, and that no combiners are run.

The key take away from this test is how zero compression, along with the
partition size, affects the materialized bytes.  By using the following function:

```python3
def estimate_map_output_materialized_bytes(num_words, num_reducers, key_num_bytes, value_num_bytes)
```

which utilizes Hadoop's own logic for implementing a zero compression algorithm,
it is possible to estimate the materialized output given the supplied parameters
as shown in the above function signature.

## Map Merge Parts

<details>
  <summary>
    <b id="map-merge-parts">Click to see test output</b>
  </summary>

```
2019-07-12 01:14:32,075 INFO [SpillThread] org.apache.hadoop.mapred.MapTask: Finished spill 0, file path: attempt_1562894051760_0001_m_000000_0_spill_0.out

2019-07-12 01:14:32,171 INFO [SpillThread] org.apache.hadoop.mapred.MapTask: Finished spill 1, file path: attempt_1562894051760_0001_m_000000_0_spill_1.out

2019-07-12 01:14:32,254 INFO [main] org.apache.hadoop.mapred.MapTask: Finished spill 2, file path: attempt_1562894051760_0001_m_000000_0_spill_2.out

2019-07-12 01:14:32,261 INFO [main] org.apache.hadoop.mapred.Merger: Merging 3 sorted segments

2019-07-12 01:14:32,263 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 3 segments left of total size: 524294 bytes

2019-07-12 01:14:36,409 INFO [main] org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl: MergerManager: memoryLimit=535088320, maxSingleShuffleLimit=133772080, mergeThreshold=353158304, ioSortFactor=3, memToMemMergeOutputsThreshold=3

2019-07-12 01:14:36,598 INFO [main] org.apache.hadoop.mapred.Merger: Merging 1 sorted segments

2019-07-12 01:14:36,598 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 524286 bytes

2019-07-12 01:14:36,646 INFO [main] org.apache.hadoop.mapred.Merger: Merging 1 sorted segments

2019-07-12 01:14:36,647 INFO [main] org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 524286 bytes
```
</details>

Each map task [merges its spill files](./top_level_overview.markdown#Point-3-&-4:-Mapping-&-Handling-Intermediate-map-Values)
back into a single partitioned map output file. The tests here show that both
the map side merge, and the reduce side merge follow the same general approach.
I.e., that files are queued into memory, and consumed on each merge round,
minimizing the total number of merges required to achieve a `merge factor` 
number of files to be merged on the final round.  A sample of the behavior is
shown below:

```
simulate_merges_using_variable_pass_factor(5, 23)
pass: 1, current_num_segments: 23, pass_factor: 3, remaining_num_segments: 21
pass: 2, current_num_segments: 21, pass_factor: 5, remaining_num_segments: 17
pass: 3, current_num_segments: 17, pass_factor: 5, remaining_num_segments: 13
pass: 4, current_num_segments: 13, pass_factor: 5, remaining_num_segments: 9
pass: 5, current_num_segments: 9, pass_factor: 5, remaining_num_segments: 5
pass: 6, current_num_segments: 5, pass_factor: 5, remaining_num_segments: 1
```

## Reduce Merge Parts

// TODO