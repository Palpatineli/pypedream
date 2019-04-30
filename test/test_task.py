##
from typing import TypeVar, Optional
from pathlib import Path
import logging
import logging.handlers
import logging.config
import pickle as pkl
from multiprocessing import Pool
from pypedream import Task, getLogger, FileObj

T = TypeVar("T")

## Linear Graph
def test_linear():
    Task.__save_folder__ = Path("data").resolve()
    step1 = Task("step-1", lambda x: float(x) + 2, "2019-04-26T17:15")  # type: ignore
    step2 = Task("step-2", lambda x: int(round(x)) + 3, "2019-04-26T08:47")  # type: ignore
    step3 = Task("step-3", lambda x: str(x) + "!", "2019-04-26T08:45")  # type: ignore
    c = step1()
    b = step2(c)
    a = step3(b)
    res = [a.run({"step-1": x, "name": 'id-' + x}) for x in ['5', '9', '10']]
    assert(res[0] == '10!')
    with open("data/step-2/id-9.pkl", 'rb') as fp:
        interim = pkl.load(fp)
    assert(interim == 14)

## Merging Graph
def listener(q):
    while True:
        record = q.get()
        if record is None:
            break
        logger = logging.getLogger()
        logger.handle(record)

def step_1(x):
    return float(x) + 2

def step_2(x):
    return int(float(x)) + 3

def step_3(x):
    return x * x

def step_4(x, y, z):
    return str(x) * 2 + str(y) + "!" + str(z)

def step_5(x, y):
    return x * y

def step_6(x, y):
    return str(y) + "_" + x

from os.path import getmtime, split
from glob import glob

class AlignedImageCache(FileObj):
    def exists(self) -> bool:
        return len(glob(self.file_path + "+")) > 0

    def load(self, time: float) -> Optional[T]:
        files = glob(self.file_path + "+")
        if len(files) == 0:
            return None
        entries = sorted([(getmtime(entry), entry) for entry in files])
        return split(entries[-1][1])[1].split("+")[1] if entries[-1][0] > time else None

    def save(self, obj: T):
        folder = Path(self.file_path).resolve().parent
        if not folder.exists():
            folder.mkdir(parents=True)
        with open(self.file_path + "+" + str(obj), 'w'):
            pass

##
def test_merge():
    Task.__save_folder__ = Path("merge_data").resolve()
    step1 = Task("step-1", step_1, "2019-04-26T17:12")
    step2 = Task("step-2", step_2, "2019-04-26T08:47", AlignedImageCache)
    step3 = Task("step-3", step_3, "2019-04-26T08:47")
    step4 = Task("step-4", step_4, "2019-04-26T17:38")
    step5 = Task("step-5", step_5, "2019-04-26T17:12")
    step6 = Task("step-6", step_6, "2019-04-26T17:10")
    s1 = step1()
    s2 = step2(s1)
    s3 = step3()
    s4 = step4([s1, s2, s3])
    s5 = step5([s2, s3])
    s6 = step6([s4, s5])
    logger = getLogger("pypedream", "tasklog2.log")
    pool = Pool(10)
    param_dict = [{"step-1": x, "step-3": y, "name": 'id-' + x, "logger": logger}
                  for x, y in [('5', 2), ('9', 5), ('10', 11)]]
    res = pool.map(s6.run, param_dict)

    assert(res[0] == '40_7.07.010!4')
    with open("merge_data/step-5/id-10.pkl", 'rb') as bfp:
        interim = pkl.load(bfp)
    assert(interim == 1815)

##
