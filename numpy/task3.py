import numpy as np
import random


def array_creation(n_size):
    return np.random.randint(0, 100, size=(n_size, n_size))

def transpose_array(ndarray):
    return np.transpose(ndarray, axes=(1,0))

def reshape_array(ndarray):
    return np.reshape(ndarray, (3, 12))

def split_array(array, num_splits, axis=0):
    return np.array_split(array, num_splits, axis=axis)

def combine_arrays(*arrays, axis=0):
    return np.concatenate(arrays, axis=axis)

def print_array(array, message=None):
    if message:
        print(message)
    print(array, end="\n\n")


if __name__ == '__main__':
    array = array_creation(6)
    array2 = array_creation(6)
    print_array(array, "Initial array:")

    transposed_array = transpose_array(array)
    print_array(transposed_array, "Transposed array:")

    reshaped_array = reshape_array(array)
    print_array(reshaped_array, "Reshaped array:")

    splitted_array = split_array(array, num_splits=3)
    print_array(splitted_array, "Splitted array:")

    combined_array = combine_arrays(array, array2)
    print_array(combined_array, "Combined array:")

    assert transposed_array.shape == (array.shape[1], array.shape[0]), "Transpose failed."
    assert reshaped_array.shape == (3, 12), "Reshape failed."
    assert all(sub_array.shape == (2, 6) for sub_array in splitted_array), "Split failed."
    assert combined_array.shape == (12, 6), "Combine failed."