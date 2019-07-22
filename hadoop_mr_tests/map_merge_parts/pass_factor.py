#!/usr/bin/env python3

"""
Testing mergeParts() in
hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/MapTask.java.
mergeParts() is called at the end of a map task once all map output has been spilled onto disk
in a single or multiple spill files. The variable "factor" refers to the value set for the property, "mapreduce.task.io.sort.factor".
"""

def get_pass_factor(factor, pass_number, num_segments):
    """
    Helper function used when merging spill files.
    Source code copied from hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Merger.java.
    """
    if pass_number > 1 or num_segments <= factor or factor == 1:
        return factor

    mod = (num_segments - 1) % (factor - 1)
    if mod == 0:
        return factor;
    return mod + 1


def simulate_merges_using_variable_pass_factor(factor, initial_num_segments):
    """
    Adopted from the source code in
    hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Merger.java.
    Logs from Map Reduce jobs will show similar information printed in this function, however
    we can "fake" the merges here and avoid running lengthy Map Reduce jobs while still being
    able to observe how mapreduce.task.io.sort.factor affects the number of segments being merged
    at a time and the total number of passes done to merge all spill files into a single
    file.
    """
    print("simulate_merges_using_variable_pass_factor({}, {})".format(factor, initial_num_segments))
    pass_number = 1
    remaining_num_segments = initial_num_segments

    while True:
        current_num_segments = remaining_num_segments
        pass_factor = get_pass_factor(factor,
                                        pass_number,
                                        remaining_num_segments)
        remaining_num_segments -= pass_factor - 1

        print("pass: {}, current_num_segments: {}, pass_factor: {}, remaining_num_segments: {}".format(
            pass_number,
            current_num_segments,
            pass_factor,
            remaining_num_segments if remaining_num_segments > 0 else 1 
        ))

        pass_number += 1

        if remaining_num_segments <= 1:
            break

def simulate_merges_using_fixed_pass_factor(factor, initial_num_segments):
    """
    Does the same thing as simulate_merges_using_variable_pass_factor(factor, initial_num_segments) above
    however the pass factor is fixed at each pass except for the final pass where the pass factor
    will be 2 <= pass factor <= the entered pass factor.
    """
    print("simulate_merges_using_fixed_pass_factor({}, {})".format(factor, initial_num_segments))
    pass_number = 1
    remaining_num_segments = initial_num_segments

    while True:
        current_num_segments = remaining_num_segments
        if remaining_num_segments - factor - 1 >= 1:
            remaining_num_segments -= factor - 1
        else:
            remaining_num_segments = 1

        print("pass: {}, current_num_segments: {}, remaining_num_segments: {}".format(
            pass_number,
            current_num_segments,
            remaining_num_segments
        ))

        pass_number += 1

        if remaining_num_segments == 1:
            break

if __name__=="__main__":
    simulate_merges_using_variable_pass_factor(5, 23)
    simulate_merges_using_fixed_pass_factor(5, 23)

    """
    simulate_merges_using_variable_pass_factor(5, 23)
    pass: 1, current_num_segments: 23, pass_factor: 3, remaining_num_segments: 21
    pass: 2, current_num_segments: 21, pass_factor: 5, remaining_num_segments: 17
    pass: 3, current_num_segments: 17, pass_factor: 5, remaining_num_segments: 13
    pass: 4, current_num_segments: 13, pass_factor: 5, remaining_num_segments: 9
    pass: 5, current_num_segments: 9, pass_factor: 5, remaining_num_segments: 5
    pass: 6, current_num_segments: 5, pass_factor: 5, remaining_num_segments: 1

    simulate_merges_using_fixed_pass_factor(5, 23)
    pass: 1, current_num_segments: 23, remaining_num_segments: 19
    pass: 2, current_num_segments: 19, remaining_num_segments: 15
    pass: 3, current_num_segments: 15, remaining_num_segments: 11
    pass: 4, current_num_segments: 11, remaining_num_segments: 7
    pass: 5, current_num_segments: 7, remaining_num_segments: 3
    pass: 6, current_num_segments: 3, remaining_num_segments: 1

    mapreduce.task.io.sort.factor does not appear to have any effect on the
    number of passes done by map tasks. The number of passes
    is the same whether or not the pass factor is fixed at each pass. Furthermore,
    map task spill files are always written to disk, and so having a variable pass factor
    to account for in-memory spill files isn't relevant for Map Tasks. Map Tasks
    just use the same merge function as Reduce Tasks and the optimizations
    made in the merge function apply only to Reduce tasks.
    """
