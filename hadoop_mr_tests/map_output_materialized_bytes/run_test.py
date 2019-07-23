#!/usr/bin/env python3
import subprocess
import time
import math
from collections import namedtuple

# this module will be placed in the same directory as this file by Dockerfile 'COPY'
import util
import zero_compress

"""
Test to determine the number number of spill files and map output materialized bytes
based on map inputs, map output buffer size, and map sort spill percent
assuming a single mapper, 1 or more reducers, and that no combiners are run.

Keys for this program are the java type TextWritable. A text writable type
contains a string followed by a zero compressed number specifying the length of
the string.

+------------------------------------------------------+-------------------------------------------------------+
|                     Text Length                      |                      Characters                       |
+------------------------------------------------------+-------------------------------------------------------+
| size_of_zero_compressed_int64(len(Characters)) bytes | len(Characters) bytes.. 1 byte per character if ASCII |
+------------------------------------------------------+-------------------------------------------------------+

Values are the type IntWritable, which uses 4 bytes.

Each (key, value) pair will take up X bytes in the map output buffer where X is the
sum of all the values in the second row of the table below.

+--------------------+----------------------------------------------------+------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------+
| KEY_VALUE_METADATA |                      KEY_LEN                       |                      VALUE_LEN                       |                                                       KEY_VALUE                                                        |
+--------------------+----------------------------------------------------+------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------+
| 16 bytes           | size_of_zero_compressed_int64(KEY_NUM_BYTES) bytes | size_of_zero_compressed_int64(VALUE_NUM_BYTES) bytes | NUM_ASCII_CHARACTERS_PER_WORD + size_of_zero_compressed_int64(NUM_ASCII_CHARACTERS_PER_WORD) + INT_WRITABLE_SIZE bytes |
+--------------------+----------------------------------------------------+------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------+

When (key, value) pairs are written to a spill file from the map output buffer, only [KEY_LEN, VALUE_LEN, KEY_VALUE]
is written. At this point, KEY_VALUE_METADATA is not needed.

Binary of spill file generated from a MapTask running wordcount on an input file with the
the following contents: rr\nrr\nrr\nrr\nrr\nrr\nrr\nrr\nrr\nrr\n

In this test, a single reducer is used.

Keys are type TextWritable, Values are type IntWritable

KEY LEN  VAL LEN  [LEN     CHAR     CHAR   ] [4 BYTE INT                       ]
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
00000011 00000100 00000010 01110010 01110010 00000000 00000000 00000000 00000001  ...rr....
11111111 11111111 10110100 00001100 01110001 10001110 00001010                    ....q.. <----- partition

------------------------------------------------------------------------------------------------------

Binary of spill file generated from a MapTask running wordcount on an input file with the
the following contents: aa\nbb\n

In this test, two reducers are used,

KEY LEN  VAL LEN  [LEN     CHAR     CHAR   ] [4 BYTE INT                       ]
00000010 00000100 00000001 01100001 01100001 00000000 00000000 00000000 00000001
00000010 00000100 00000001 01100001 01100001 00000000 00000000 00000000 00000001
11111111 11111111 10000010 01110010 11101101 01101101                   <----- partition
00000010 00000100 00000001 01100010 01100010 00000000 00000000 00000000 00000001
00000010 00000100 00000001 01100010 01100010 00000000 00000000 00000000 00000001
11111111 11111111 10100010 11100111 10011101 10001001 00001010          <----- partition

Sample Test Results
-------------------
Result(num_reducers=1, num_chars_per_word=1, num_words=10000, map_output_bytes=60000, expected_map_output_bytes=60000, materialized_bytes=80006, expected_materialized_bytes=80006, num_spill_files=1, expected_num_spill_files=1)
Result(num_reducers=1, num_chars_per_word=1, num_words=50000, map_output_bytes=300000, expected_map_output_bytes=300000, materialized_bytes=400006, expected_materialized_bytes=400006, num_spill_files=3, expected_num_spill_files=3)
Result(num_reducers=1, num_chars_per_word=128, num_words=10000, map_output_bytes=1340000, expected_map_output_bytes=1340000, materialized_bytes=1370006, expected_materialized_bytes=1370006, num_spill_files=3, expected_num_spill_files=3)
Result(num_reducers=1, num_chars_per_word=128, num_words=50000, map_output_bytes=6700000, expected_map_output_bytes=6700000, materialized_bytes=6850006, expected_materialized_bytes=6850006, num_spill_files=15, expected_num_spill_files=15)
Result(num_reducers=2, num_chars_per_word=1, num_words=10000, map_output_bytes=60000, expected_map_output_bytes=60000, materialized_bytes=80012, expected_materialized_bytes=80012, num_spill_files=1, expected_num_spill_files=1)
Result(num_reducers=2, num_chars_per_word=1, num_words=50000, map_output_bytes=300000, expected_map_output_bytes=300000, materialized_bytes=400012, expected_materialized_bytes=400012, num_spill_files=3, expected_num_spill_files=3)
Result(num_reducers=2, num_chars_per_word=128, num_words=10000, map_output_bytes=1340000, expected_map_output_bytes=1340000, materialized_bytes=1370012, expected_materialized_bytes=1370012, num_spill_files=3, expected_num_spill_files=3)
Result(num_reducers=2, num_chars_per_word=128, num_words=50000, map_output_bytes=6700000, expected_map_output_bytes=6700000, materialized_bytes=6850012, expected_materialized_bytes=6850012, num_spill_files=15, expected_num_spill_files=15)
"""

# TODO: make a function that can estimate output based on some distribution of keys/key lengths

def estimate_num_spill_files(num_words, key_num_bytes, value_num_bytes, mapreduce_task_io_sort_mb, mapreduce_map_sort_spill_percent):
    """
    Computes the number of spill files that will be created by a single mapper.
    """
    # extra bytes added when each (k,v) pair is added to output buffer
    KEY_VALUE_META_DATA_NUM_BYTES = 16

    key_len_num_bytes = zero_compress.size_of_zero_compressed_int64(key_num_bytes)
    value_len_num_bytes = zero_compress.size_of_zero_compressed_int64(value_num_bytes)

    return math.ceil((num_words * (KEY_VALUE_META_DATA_NUM_BYTES + key_len_num_bytes + key_num_bytes + value_len_num_bytes + value_num_bytes)) /
                    (util.MiB_to_bytes(mapreduce_task_io_sort_mb) * mapreduce_map_sort_spill_percent))

def estimate_map_output_bytes(num_words, key_num_bytes, value_num_bytes):
    """
    Computes "map output bytes", that is the total number of bytes all key, value pairs
    consume when serialized.
    """
    return num_words * (key_num_bytes + value_num_bytes)

def estimate_map_output_materialized_bytes(num_words, num_reducers, key_num_bytes, value_num_bytes):
    """
    Computes "map output materialized bytes", that is the total number of bytes written from the map output buffer
    to spill files.
    """
    SPILL_FILE_PARTITION_INDICATOR_NUM_BYTES = 6

    return (num_words * (zero_compress.size_of_zero_compressed_int64(key_num_bytes) +
                         key_num_bytes +
                         zero_compress.size_of_zero_compressed_int64(value_num_bytes) +
                         value_num_bytes) +
           (SPILL_FILE_PARTITION_INDICATOR_NUM_BYTES * num_reducers))

if __name__=="__main__":
    # mapreduce configuration set in mapred-site.xml
    MAPREDUCE_TASK_IO_SORT_MB = 1
    MAPREDUCE_MAP_SORT_SPILL_PERCENT = 0.5
    MAPREDUCE_JOB_REDUCES = [1,2] # run with 1, and 2 reducers

    # keys will be of type TextWritable
    key_num_bytes = lambda num_ascii_characters : zero_compress.size_of_zero_compressed_int64(num_ascii_characters) + num_ascii_characters

    # values will be fo type IntWritable
    VALUE_NUM_BYTES = 4

    # input sizes
    NUM_WORDS_LIST = [10000, 50000]
    NUM_ASCII_CHARACTERS_PER_WORD = [1, 128]

    Result = namedtuple("Result", ["num_reducers",
                                    "num_chars_per_word",
                                    "num_words",
                                    "map_output_bytes",
                                    "expected_map_output_bytes",
                                    "materialized_bytes",
                                    "expected_materialized_bytes",
                                    "num_spill_files",
                                    "expected_num_spill_files"])
    results = list()

    for num_reducers in MAPREDUCE_JOB_REDUCES:
        for num_chars in NUM_ASCII_CHARACTERS_PER_WORD:
            for num_words in NUM_WORDS_LIST:
                util.hadoop_start_up()
                util.hdfs_generate_custom_word_files([[chr(97 + (i % len(MAPREDUCE_JOB_REDUCES))) * num_chars for i in range(1, num_words + 1)]])

                # things to record
                num_spill_files = 0
                estimated_num_spill_files = estimate_num_spill_files(num_words,
                                                                    key_num_bytes(num_chars),
                                                                    VALUE_NUM_BYTES,
                                                                    MAPREDUCE_TASK_IO_SORT_MB,
                                                                    MAPREDUCE_MAP_SORT_SPILL_PERCENT)
                map_output_bytes = 0
                estimated_map_output_bytes = estimate_map_output_bytes(num_words,
                                                                        key_num_bytes(num_chars),
                                                                        VALUE_NUM_BYTES)
                map_output_materialized_bytes = 0
                estimated_map_output_materialized_bytes = estimate_map_output_materialized_bytes(num_words,
                                                                                                    num_reducers,
                                                                                                    key_num_bytes(num_chars),
                                                                                                    VALUE_NUM_BYTES)

                # use "-D property=value" to set mapreduce configuration properties from the command line
                run_wordcount = util.execute_command("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0-SNAPSHOT.jar wordcount -D mapreduce.job.reduces={} input output".format(num_reducers),
                                                        stderr=subprocess.STDOUT)

                for line in run_wordcount.decode().split('\n'):
                    if "Map output bytes=" in line:
                        map_output_bytes = int(line.split('=')[1])

                    if "Map output materialized bytes=" in line:
                        materialized_bytes = int(line.split('=')[1])

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

                results.append(Result(num_reducers,
                                        num_chars,
                                        num_words,
                                        map_output_bytes,
                                        estimated_map_output_bytes,
                                        materialized_bytes,
                                        estimated_map_output_materialized_bytes,
                                        num_spill_files,
                                        estimated_num_spill_files))

    print(*results, sep='\n')
