#!/usr/bin/env python3

"""
Testing the behavior of writeVLong(DataOutput stream, long i) in
hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/WritableUtils.java
as it is used to serialize key value metadata and in other places such as TextWritable.
"""

def int64_to_string(i):
    """
    Get 64 bit binary string from int i.
    """
    return format(i & 0xffffffffffffffff, "064b")

def int64_to_string_by_bytes(i):
    """
    Get 64 bit binary string from int i with spaces between each byte.
    """
    no_spaces_byte_string = int64_to_string(i)
    return [no_spaces_byte_string[k*8 : (k*8)+8] for k in range(8)]

def zero_compress_int64(i):
    """
    Copied from the java implementation in 
    hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/WritableUtils.java.

    Takes in a a number from -(2^63) to ((2^63) - 1) inclusive and serializes it into a binary
    file with zero-compressed encoding.
    """
    FILE_PATH = "zero_compressed_long"

    if i >= -112 and i <= 127:
        with open(FILE_PATH, "wb") as binary_file:
            binary_file.write(bytes([255 & i]))

    else:
        length = -112

        # if i is < -112
        if i < 0:
            i ^= -1 # take one's complement by XOR with '11111111 11111111 11111111 11111111'
            length = -120

        # see how many bytes are needed for this number
        tmp = i
        while tmp != 0:
            tmp >>= 8
            length -= 1

        with open(FILE_PATH, "wb") as binary_file:
            # length is some positive number from 1 to 8
            length = -(length + 120) if (length < -120) else -(length + 112)
            binary_file.write(bytes([255 & length]))

            for idx in range(length, 0, -1):
                # find out how many shifts we need to do to get the bits 
                # at the byte at a given idx
                shift_bits = (idx - 1) * 8
                mask = 0xffffffffffffffff << shift_bits

                # shift those bits to the lowest order byte, then write that byte
                value_to_write = (i & mask) >> shift_bits
                # mask with 255 so we write only a single byte 
                binary_file.write(bytes([255 & value_to_write]))

    return FILE_PATH

def size_of_zero_compressed_int64(i):
    """
    Returns the number of bytes we expect to write when using
    writeVLong(DataOutput stream, long i ) from WritableUtils.java
    """
    if i >= -112 and i <= 127:
        return 1
    else:
        length = 0
        
        if i < 0:
            i ^= -1

        tmp = i
        while tmp != 0:
            tmp >>= 8
            length += 1

        return length + 1
        

def print_int64_range_binary(start, end):
    """
    Print a range of signed 64 bit integers and 2's complement binary representation.
    """
    for i in range(start, end+1, 1):
        print("{:4}".format(i), " ".join(int64_to_string_by_bytes(i)))


if __name__=="__main__":
    """
    # print sample numbers
    for i in [-256, -129, -128, -112, 7, 255, 256, 1024]:
        path = zero_compress_int64(i)

        with open(path, "rb") as binary_file:
            encoded_bytes = list()
            byte = binary_file.read(1)
            while byte:
                encoded_bytes.append(byte)
                byte = binary_file.read(1)

        encoded_bytes_as_string = " ".join([format(byte[0], "08b") for byte in encoded_bytes])

        print("{0:4}: ".format(i), "{:>70}".format(encoded_bytes_as_string), 
                "len: {} ".format(len(encoded_bytes)), "expected len: {}".format(size_of_zero_compressed_int64(i)))
        """
