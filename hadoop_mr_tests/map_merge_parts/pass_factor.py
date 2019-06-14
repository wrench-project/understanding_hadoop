#!/usr/bin/env python3

def get_pass_factor(factor, pass_number, num_segments):
    if pass_number > 1 or num_segments <= factor or factor == 1:
        return factor

    mod = (num_segments - 1) % (factor - 1)
    if mod == 0:
        return factor;
    return mod + 1


def simulate_merges_using_variable_pass_factor(factor, initial_num_segments):
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
            remaining_num_segments
        ))

        pass_number += 1

        if remaining_num_segments == 1:
            break

def simulate_merges_using_fixed_pass_factor(factor, initial_num_segments):
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
