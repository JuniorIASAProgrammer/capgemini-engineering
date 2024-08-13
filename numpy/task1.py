"""
Practical Task 1: Basic Array Creation and Manipulation with NumPy
"""
import numpy as np
from typing import Tuple


def array_creation() -> Tuple[np.ndarray, np.ndarray]:
    one_d_array = np.arange(1, 11)
    two_d_array = np.arange(1, 10).reshape(3, 3)
    return one_d_array, two_d_array

def indexing_and_slicing(one_dimentional_array, two_dimentional_array) -> Tuple[np.array, np.ndarray]:
    third_element = one_dimentional_array[2]
    first_two_rows_and_columns = two_dimentional_array[:2, :2]
    return third_element, first_two_rows_and_columns

def basic_arithmetic(one_dimentional_array, two_dimentional_array) -> Tuple[np.array, np.ndarray]:
    added_five = one_dimentional_array + 5
    multiplied_by_2 = two_dimentional_array*2
    return added_five, multiplied_by_2

def print_array(array, message=None):
    if message:
        print(message)
    print(array, end="\n\n")


if __name__ == '__main__':
    one_d_array, two_d_array = array_creation()
    print_array(one_d_array, message='1D array')
    print_array(two_d_array, message='2D array')

    third_element, first_two_rows_and_columns = indexing_and_slicing(one_d_array, two_d_array)
    print_array(third_element, message='Third element of 1D array')
    print_array(first_two_rows_and_columns, message='First 2 rows and 2 columns of 2D array')

    added_five, multiplied_by_2 = basic_arithmetic(one_d_array, two_d_array)
    print_array(added_five, message='Add 5 to 1D array')
    print_array(multiplied_by_2, message='2D array multiplied by 2')
