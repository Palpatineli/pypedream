##
"""bz2 is thread safe from python doc. But we should test it to be safe. Turns out it's totally not working"""
from pytest import mark
import h5py
from multiprocessing import Pool
from pathlib import Path
import numpy as np

save_path = Path("data/mp-test.h5").resolve()

def worker(number: int):
    a = np.arange(100000) * number
    with h5py.File(save_path, mode='w') as h5file:
        h5file[str(number)] = a
    with h5py.File(save_path, mode='r') as h5file:
        b = h5file[str(number)]
    return number, b

@mark.skip(reson="it is known to not work so we are not bothering with zipfile approach."
           "Expect problems when you have many cases and are on lustre fs")
def test_thread_safety():
    with Pool(processes=10) as pool:
        res = pool.map(worker, range(50))
        pool.join()
    results = [x for x in res if x is not None]
    for i, x in sorted(results, key=lambda x: x[0]):
        if not np.allclose(x, np.arange(100000)):
            print(f"error at {i}: {np.flatnonzero(x - np.arange(100000))}")
##
if __name__ == '__main__':
    test_thread_safety()
##
