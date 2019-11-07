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