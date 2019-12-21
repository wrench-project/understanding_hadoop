# Road map: MapReduce (MR) Job Life Cycle

The initial pass at implementation will be made referring to
[Hadoop Performance Models](https://arxiv.org/pdf/1106.0940.pdf).
This is a cluster compute cost computation analysis done by Herodotos Herodotou,
the analysis assumes an average cost across all performance calculations. This
model will serve as a reference for an implementation of a Hadoop simulation,
such that future iterations will achieve a more 'true to life' cost performance.

## Job Prerequisites

* Submitting a job
    - Client submits the application to the application manager which sits in
    the [ResourceManager](#aside:-resourcemanager).  The `ResourceManager` (RM)
    receives the Application ID and launches the `ApplicationMaster` daemon
    (AMD), which communicates with the RM via the following flow using the
    `ApplicationMasterProtocol`:
        1. The AMD registers itself with the `RM`.
        2. The AMD can request and receive containers.
        3. Once a container has been allocated, the `AMD` communicates with the
        `NodeManager` using `startContainer` via the `ContainerManager`, thus
        setting up the environment for running a task.
    - The `JobClient` is then responsible for the following:
        1. Checking the input and output specifications of the job.
        2. Computing the InputSplits for the job.
        3. Setup the requisite accounting information for the DistributedCache
        of the job, if necessary.
        4. Copying the job's jar and configuration to the map-reduce system
        directory on the distributed file-system.
        5. Submitting the job to the cluster and optionally monitoring it's
        status.

An `InputSplit` serves as the input to a single mapper task. The number of map
tasks is equal to the number of `InputSplits` created: `number_of_map_tasks = sum(ceil(s_i / b))`
Where `s_i` is the size of the ith file, and `b` is the block size, for some `N`
files in a set of input data.  See [number_of_map_tasks](../hadoop_mr_tests/number_of_map_tasks/run_test.py).

Map tasks are assigned in the following way:
1. The `AMD` requests data from the `NameNode` on where blocks of data are
stored.
2. The `AMD` pings the `RM` to have a map task process an `InputSplit` on the
same node where the corresponding physical data is stored.

## Map Node

The map task is divided into five phases:
1. __Read__: Read the input split and compute key-value pairs.
2. __Map__: Execute the user-defined map function.
3. __Buffer__: Placing the map output into the circular buffer and partitioning.
4. __Spill__: Sort, use a combiner if defined, perform compression as needed,
and writing to disk.
5. __Merge__: Merging the spill files into a single output file.

#### Read and Map phase

Input splits are computed and the corresponding input blocks are read,
uncompressed if needed.  The key-value pairs are generated and passed to the map
function that was defined by the user.

See section [2.1](https://arxiv.org/pdf/1106.0940.pdf)

#### Buffer and Spill phase

The map function generates the intermediate key-value pairs as places them in a
map-side circular buffer.

_Further research here involves finding out whether or not Hadoop implements two
 buffers: A serialization, __and__ accounting buffer. Where the
accounting buffer stores meta data pertaining to the intermediate
key-value pairs._

The number of spill files per job is based on input data.  This number will
be based on the estimation function written in the [materialized_bytes_test](../hadoop_mr_tests/map_output_materialized_bytes/run_test.py).

Spill files are sorted and merged back into a `Map Output File`.  Each map
task's generated spill files undergo a sort and merge process.  The map-side
merge follows the same rule as the reduce-side merge, i.e., produce the minimum
number of file merge operation, such that the `merge factor` number of files are
merged in the final pass, see [map_merge_parts](../hadoop_mr_tests/map_merge_parts/run_test.py).

See section [2.2](https://arxiv.org/pdf/1106.0940.pdf).

#### Merge phase

The purpose of the merge phase on the Map side is to merge all of the generated
spill files into a single Map output file, and write it to local disk to then
be fetched by the Reduce side.  This occurs only where more than one Map output
file is generated.

This is described in the [top level overview](./top_level_overview.markdown) file.

For a detailed description refer to section [2.3](https://arxiv.org/pdf/1106.0940.pdf).

## Reduce Node

The reduce task is divided into four phases:
1. __Shuffle__: Map output files are fetched from the Map nodes and copied onto
the Reduce nodes, files are possibly decompressed, and potentially (partially)
merged.
2. __Merge__: The reduce side is guaranteed that Map output files are sorted.
These constiuent files are merged into a single file for each individual
Reducer.
3. __Reduce__: The user-defined function is executed on the corresponding file.
4. __Write__: Each Reduce task writes an output file to the HDFS.

#### Shuffle phase

As data is copied to the Reducer, it is placed in a shuffle in-memory buffer.
When this buffers reaches a parameterized threshold, or the number of files
exceeds a certain limit, the files are merged and spilled to disk as a
`shuffleFile`.

If a file is greater the 25% of the buffer size, the file is sent straight to
disk, and hence not passed to the in-memory buffer.

There are a number of different cases that can occur at this stage, please
see section [3.1](https://arxiv.org/pdf/1106.0940.pdf).
for further details.

#### Merge phase

This is done similarly to how merging is handled on the [map side](#map-node).
However, instead of creating an output file, the data is sent directly to the
Reducer.

When this phase begins there are three possible types of file to be merged:
- Merged Shuffle Files
- Unmerged Shuffle Files
- A set of Map output files in memory

Merging is thus done in three steps:

1. Some Map output files in memory might be merged to satisfy the constraint
made by the parameter that specifies the allowable amount of memory to be
occupied before a reducer begins.
2. Any files on disk will go through a merging phase. A key difference on the
Reduce side is that the merge files have different sizes as they are not
partitioned like on the Map side.
3. Both on-disk and in-memory files are merged.

See section [3.2](https://arxiv.org/pdf/1106.0940.pdf).

#### Reduce and Write phase

The Reduce function is executed and the output file is written to the HDFS.

See section [3.3 and 3.4](https://arxiv.org/pdf/1106.0940.pdf).

#### Aside: ResourceManager
* The resource manager is primarily limited to scheduling.  That is, delegating
available resources in the system among applications running on competing nodes
and not with per-application state management. Refer
[here](https://blog.cloudera.com/apache-hadoop-yarn-resourcemanager/) for
further details regarding the components that constitute the `ResourceManager`.
