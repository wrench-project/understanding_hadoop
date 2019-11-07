# MapReduce

This page serves as an overview of what constitutes a MapReduce job.  We then
look into what's going on under the hood relating to the Hadoop implementation
of MapReduce.  The lower-level, implementation details realting to
[How Hadoop Handles MapReduce](#how-hadoop-handles-mapreduce) will be covered
in the __Roadmap__ section.

### What is MapReduce?
As per _Map Reduce: Simplified Data Processing on Large Clusters_:

MapReduce is a programming model inspired by the synonymous primitives present
in Lisp and many other functional programming languages.

Conceptually, MapReduce can be broken into the following:

1. __Computation__: Accepts a set of _input_ `(key, value)` pairs and produces a
set of _output_ `(key, value)` pairs.

2. __Map__: A user defined function that takes input pairs and produces a set of _intermediate_ `(key, value)` pairs. The MapReduce library groups all of these
intermediate values associated with the same intermediate key, _I_, and passes
them to the reduce function.

3. __Reduce__:  Is a user defined function that accepts an intermediate key,
`I`, and a set of values for that key. It merges the values together.  Typically
zero or one output values are produced by any _Reduced_ invocation.
(The list of values are passed by an iterator to deal with memory availability.)

The above computational process has the following type associations:

```
map:    (k1, v1)       -> list(k2, v2)

reduce: (k2, list(v2)) -> list(v2)
```

That is, a map task, which is a copy of a user-defined function, accepts
key-value pairs, `(k1, v1)`, and outputs `(k2, v2)`, representative of the
intermediate value pairs, `I`.  These intermediate pairs are not necessarily of
the same type as the input pairs.  The reducer then accepts all keys, `k2`, and
the corresponding list of values, `list(v2)`.  The reducer then performs some
user-defined operation on the list, and produces some output.


#### Steps in a MapReduce Process:

The following is a simplification of the steps outlined in
_Map Reduce: Simplified Data Processing on Large Clusters_.  After we break down
the process, we'll explore how Hadoop handles each of the following:

1. The MapReduce library splits the input into _M_ chunks, and starts up copies
of the program on multiple machines.

2. The _master_ fork assigns tasks to the _worker_ nodes who are just running
copies of the _user program_.  There are `M` map tasks, and `R` reduce tasks
that the _master_ must assign. Note that a worker node takes on the role of
either map, or reduce. Typically, there are many more map nodes than reduce
nodes.

3. A _worker_ assigned a map task, reads in the contents of the corresponding
input split.  The _worker_ parses (key, values) pairs and passes it to the map
function. The _intermediate_ (key, value) pairs produced by the mapping are
buffered into memory.

4. The local disk it partitioned into `R` regions by the _partitioning
function_. The buffered _intermediate_ pairs produced by a map worker are
periodically written to these partitions, with the particular location of these
writes sent back to `master` for use by the reduce workers.

5. When the `master` notifies a _reduce_ worker about a particular location of
_intermediate_ key-value pairs, it reads the buffered data from the local
disks of the map workers. When a reduce worker has read all intermediate data,
it sorts it by key so that all of the same keys are grouped together. The
sorting is needed because many different keys can map to the same reduce task.
If the amount of intermediate data is too large to fit in memory, an external
sort is used.

6. The reduce worker iterates over the sorted data and for each unique
intermediate key encountered, it passes the key and the corresponding set of
intermediate values to the user’s _reduce_ function. The output of the reduce
function is appended to a final output file for this reduce partition.

7. When all map tasks and reduce tasks have been completed, the call to
MapReduce reaches a return statement back to user code.

## How Hadoop Handles MapReduce
Now that we are comfortable with what MapReduce is, let's look into
understanding how Hadoop handles MapReduce.  Here, each point expands on the
corresponding number in [Steps in a MapReduce Process](#steps-in-a-map-reduce-process).

Before a job can be started, however, it must run through a few steps:
1. A job must be submitted via the user API.
2. The resource manager retrieves the job ID (application ID)
3. The job client checks output specification, computes splits (which can also
  be changed to be computed over the cluster, useful for a job with many splits),
  and then copy job resources over to the HDFS.

#### Point 1 & 2: Splitting and Assigning Tasks
HDFS (Hadoop Distributed File System) breaks down large files into configurable
sized blocks, defaulting to 64 MB.  It then stores copies of these blocks across
nodes in the cluster.  

Using YARN (YARN Applied Resource Negotiator), The resource manager acts as the
cluster resource manager (no way), as well as the job scheduling facility.  The
resource manager creates an Application Master daemon (AMD) to monitor the
life cycle of the job.  One of the initial things that the AMD does is distinguish
which HDFS blocks are needed for processing.  The AMD requests data from the
`NameNode` on where the relevant replicated blocks of data are stored. With this
information, the AMD then requests the resource manager to have map tasks
process the specified blocks on the same nodes where the data is stored. This
data locality helps to avoid potentially painful network bottlenecks.

The number of map tasks correspond to the number of created blocks, while the
number of reducers are configurable via the `mapreduce.jobs.reduces` property.
Thus for each input split, a map task is spawned.  The number of maps is
typically driven by the number of HDFS blocks in the input file. This block size
is configurable, and therefore allows for a workaround when wishing to adjust the
number of map tasks.

There does exists a `mapred.map.tasks` parameter, however this is simply a hint
to the `InputFormat` (which describes the input-specification for a Map-Reduce
job), regarding the number of map tasks.

Thus, e.g., if you expect 10TiB of input data and have 128MiB DFS blocks, you'll
end up with 81920 maps, unless your `mapred.map.tasks` is even larger.
Ultimately the `InputFormat` determines the number of maps.

```
Number of blocks = integer ceiling of input data over block size

= ceil( input_data / block_size )
= (10 * 2^40) / (2^7 * 2^20)
= (10 * 2^40) / 2^27
= 10 * 2^13
= 10 * 8192
= 81920
```

The question remains: What happens when a data record gets split into two
different blocks?  After all, Hadoop has no idea about what the input data is
going to look like, so it certainly can't distinguish where one record ends and
the other begins.

To deal with this, Hadoop uses something called _input splits_.  This is a
logical representation of that data.  When a job client first calculates the
input splits, it determines where the first complete record in a block begins,
and where the last record ends.  In cases where the last record within a block
has been split off, the input split includes location data on the remnants of
the record, including the byte offset of the partial record.


#### Point 3 & 4: Mapping & Handling Intermediate map Values

For each map task, Hadoop places intermediate values into a circular buffer,
located in memory, and is configurable under `mapreduce.task.io.sort.mb`
(defaults to 100MB). This value is the total amount of data that a particular
map task can output. If a configurable percentage of this limit is met, then the
data will be spilled to the disk as a _shuffle spill file_.  Spills are written
in a round-robin fashion.

On any given node, multiple map tasks can be run, and as a result many spill
files can be created.  Hadoop merges all of the spill files, sorts, and then
partitions the resulting file based upon the number of reducers.  However, before
an individual spill file is even written to disk, the spill thread does a few
things:

1. The spill file itself is partitioned based upon the number of reducers.
2. Within each of the partitions of a spill file, the thread runs an in-memory
sort by key.
3. Finally, if a combiner function exists, it is run on the output of the
resulting sort. The combiner allows for a more compact map output, leaving less
data to write locally, thus less data to transfer to the reducer.  It is
important to note that a combiner basically compresses the data, meaning it
will eventually have to be decompressed before reducing.

The circular buffer has a configurable threshold, defaulted to 80%. The buffer
below has `0.8 * 100 MB = 80 MB` of data written, which is the limit in this
example:

```
[XXXXXXXX--]
```

The data is spilled in a separate thread to allow continuation of the map task:

```
The block in || is being written to disk, x = 10 MB written to memory by mapper
[|XXXXXXXXX|x-]

Another x = 10 MB written to memory
[|XXXXXXXXX|xx]

Spill thread completes, clear 80 MB block. 10 MB written to memory by mapper.
[x-------xx]
```

Before the write to disk actually occurs, two things happen:
* The output is split into `R` partitions, again based on the number of
reduce tasks.
* The `(key, value)` pairs are sorted, in-memory, using a QuickSort algorithm
based on (partitionID, key).

In the above case, the rate at which the mapper was writing data to memory was
less than the rate that it took to spill the block to disk.  However,
it can be the case where the mapper is writing data faster than the
spill thread is able to free memory.  In such a scenario, the mapper would be
blocked until the spill thread has had a chance to clear the buffer.

It's important to note that Hadoop uses zero compressed Long and Int values for
map output.  Here is a quote from _Hadoop Definitive Guide_, (table 8-2, page 261)
relating to the tests ran in the _Reverse Engineering Hadoop_ section:

>"Map output materialized bytes" - The number of bytes of map output actually
written to disk. If map output compression is enabled, this is reflected in the
counter value.

>"Map output bytes" - The number of bytes of uncompressed output produced by all
the maps in the job. Incremented every time the collect() method is called on
the map's OutputCollector.

#### Point 5: Preparing for a reduce task
MapReduce has the guarantee that the input to every reducer is sorted by key. As
mentioned in _point 3 & 4_, the system sorts and merges the disjoint spill files
of individual map tasks and then transfers the map outputs to the reducers.  The
process where map outputs are fetched by reduce tasks in known as the _shuffle_.
In other contexts, the shuffle can be taken as the entire process from maps
producing output, to reducers consuming input.  For our purposes, it helps to
distinguish the shuffle phase as a discrete step.

An __Event Fetcher__ fetches map task completion events over a network, and then
adds them to the fetch queue to be consumed by the fetchers themselves.  These
fetchers are run in separate threads, configurable in
`mapred.reduce.parallel.copies`, which defaults to five.  The fetchers store the
map outputs in memory with its own configurable memory buffer.

With Memory-To-Memory enabled, when the amount of map tasks reaches a
configurable threshold, in-memory merge is initiated.  The outputs of the
`MemoryToMemory` merge are stored in memory, when the memory buffer storing
these outputs reaches a certain percentage, the `InMemory` merger is invoked.
The `OnDisk` merger is triggered each time the number of files increases  by
`2 * mapreduce.task.io.sorter - 1`, but it does not merger more than
`mapreduce.task.io.sort.factor` files.

```
[map_output_1]_________[mem_to_mem]____[mem_to_mem_output]_
[map_output_2]________/                                    \
                                                            \
[map_output_3]_________[mem_to_mem]____[mem_to_mem_output]___\
[map_output_4]________/                                       \__[InMemory_merger]
                                                                                  \
[map_output_5]___________________________________________________[InMemory_merger]_\____OnDisk_merger_
                                                                                                      \
[map_output_N-1]________________________________________________________________________OnDisk_merger__\__
[map_output_N]________________________________________________________________________/
```

After the `OnDisk` merge, there is a final merge that produces the reducer input files.

The goal of this, "merging in rounds," technique is to merge the minimum number of files
as a means to get the _merge factor_ for the final round.
_Figure 6-7_ on page 212 in _Hadoop: The Definitive Guide_ nicely exemplifies
efficiently merging 40 file segments.

```
                            Round 5
4 files --Round 1---> Merge ---> M \
10 files -Round 2---> Merge ---> E  \
10 files -Round 3---> Merge ---> R   --Reduce-->
10 files -Round 4---> Merge ---> G  /
6 files -----------------------> E /
```

Here we can see the merge factor as `10` so the algorithm is to merge 4 files,
producing a single merged file. And three rounds of 10 files, to produce three
merged files.  Leaving six unmerged, and four merged files for the final round:

`1 + 3 + 6 = 10 = merge factor`.

Resources:
* Hadoop Definitive Guide, 3rd edition; Tom White
* [Hadoop MapReduce Comprehensive Description](https://0x0fff.com/hadoop-mapreduce-comprehensive-description/)
* [Map Reduce: Simplified Data Processing on Large Clusters](https://www.usenix.org/legacy/events/osdi04/tech/full_papers/dean/dean.pdf)
* https://cwiki.apache.org/confluence/display/HADOOP2/HowManyMapsAndReduces
