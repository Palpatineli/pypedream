from pypedream import process as pr
from time import perf_counter, sleep

def slow_add1(x):
    if x < 4:
        sleep(x * 0.1)
    return x + 1

def slow_add2(x):
    if x > 4:
        sleep((10 - x) * 0.1)
    return x + 2

def test_linear():
    old_time = perf_counter()
    a = (range(10) | pr.map(slow_add1, workers=4, maxsize=4) | pr.map(slow_add2, workers=4))
    print(list(a))
    assert(perf_counter() - old_time < 0.7)

test_linear()
##
def slow_add3(x, y):
    sleep(0.5)
    return x + y

def test_bifurcate():
    old_time = perf_counter()
    step1 = pr.map(slow_add1, range(10), workers=4, maxsize=4)
    step1_2 = pr.map(slow_add2, range(10, 20), workers=4)
    _ = pr.map(slow_add3, [step1, step1_2], workers=4) | list
    assert(perf_counter() - old_time < 1.0)
##
