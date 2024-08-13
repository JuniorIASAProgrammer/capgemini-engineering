import numpy as np
np.random.seed(42)


def array_creation(shape=(10, 10), low=0, high=100):
    return np.random.randint(low, high, size=shape)

def save_to_files(array, path='./numpy'):
    np.savetxt(f"{path}/array.txt", array)
    np.savetxt(f"{path}/array.csv", array, delimiter=",")
    np.save(f"{path}/array.npy", array)

def load_txt(path):
    return np.loadtxt(path)

def load_csv(path):
    return np.genfromtxt(path, delimiter=',')

def load_npy(path):
    return np.load(path)

def summation(array, axis=None):
    return np.sum(array, axis=axis)

def mean(array, axis=None):
    return np.mean(array, axis=axis)

def median(array, axis=None):
    return np.median(array, axis=axis)

def std_deviation(array, axis=None):
    return np.std(array, axis=axis)

def print_array(array, message=None):
    if message:
        print(message)
    print(array, end="\n\n")


if __name__ == "__main__":
    array = array_creation()
    print_array(array, "Initial array:")
    assert array.shape == (10,10)

    save_to_files(array)

    array_from_txt = load_txt('./numpy/array.txt')
    print_array(array_from_txt, "Loaded from txt:")
    assert np.array_equal(array_from_txt, array)

    array_from_csv = load_csv('./numpy/array.csv')
    print_array(array_from_csv, "Loaded from csv:")
    assert np.array_equal(array_from_csv, array)

    array_from_npy = load_npy('./numpy/array.npy')
    print_array(array_from_npy, "Loaded from npy:")
    assert np.array_equal(array_from_npy, array)

    array_sum = summation(array)
    print_array(array_sum, "Summed array:")

    array_mean = mean(array)
    print_array(array_sum, "Mean array:")

    array_median = median(array)
    print_array(array_median, "Median array:")

    array_std = std_deviation(array)
    print_array(array_std, "Std array:")

    array_sum_rows = summation(array, axis=1)
    print_array(array_sum_rows, "Summed array by rows:")

    array_mean_cols = mean(array, axis=0)
    print_array(array_mean_cols, "Mean array by columns:")

    array_median_rows = median(array, axis=1)
    print_array(array_median_rows, "Median array by rows:")

    array_std_cols = std_deviation(array, axis=0)
    print_array(array_std_cols, "Std array by columns:")
