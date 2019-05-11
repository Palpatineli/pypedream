##
from typing import Optional
from pathlib import Path
import logging
import logging.handlers
import logging.config
import pickle as pkl
from multiprocessing import Pool
from pypedream import Task, getLogger, FileObj

## Linear Graph
def test_linear():
    Task.save_folder = Path("data").resolve()
    step1 = Task(lambda x: float(x) + 2, "2019-04-26T17:15", "step-1")  # type: ignore
    step2 = Task(lambda x: int(round(x)) + 3, "2019-04-26T08:47", "step-2")  # type: ignore
    step3 = Task(lambda x: str(x) + "!", "2019-04-26T08:45", "step-3")  # type: ignore
    c = step1()
    b = step2(c)
    a = step3(b)
    logger = logging.getLogger("linear")
    res = [a.run(f'id-{x}', logger, {"step-1": x}) for x in ['5', '9', '10']]
    assert(res[0] == '10!')
    with open("data/step-2/id-9.pkl", 'rb') as fp:
        interim = pkl.load(fp)
    assert(interim == 14)

## Merging Graph
from os.path import getmtime, split
from glob import glob

class AlignedImageCache(FileObj):
    def exists(self) -> bool:
        return len(glob(self.file_path + "+*")) > 0

    def load(self, time: float) -> Optional[str]:
        files = glob(self.file_path + "+*")
        if len(files) == 0:
            return None
        entries = sorted([(getmtime(entry), entry) for entry in files])
        return split(entries[-1][1])[1].split("+")[1] if entries[-1][0] > time else None

    def time(self) -> float:
        files = glob(self.file_path + "+*")
        if len(files) == 0:
            return 0
        return sorted([(getmtime(entry), entry) for entry in files])[-1][0]

    def save(self, obj: str):
        folder = Path(self.file_path).resolve().parent
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        with open(self.file_path + "+" + str(obj), 'w'):
            pass

def step_1(x):
    return float(x) + 2
task_1 = Task(step_1, "2019-04-26T17:12")

def step_2(x):
    return int(float(x)) + 3
task_2 = Task(step_2, "2019-04-26T17:12", file_cacher=AlignedImageCache)

def step_3(x):
    return x * x
task_3 = Task(step_3, "2019-04-26T17:12")

def step_4(x, y, z):
    return str(x) * 2 + str(y) + "!" + str(z)
task_4 = Task(step_4, "2019-04-26T17:12")

def step_5(x, y):
    return x * y
task_5 = Task(step_5, "2019-04-26T17:12")

def step_6(x, y):
    return str(y) + "_" + x
task_6 = Task(step_6, "2019-04-26T17:12")

##
def test_merge():
    logger = getLogger("pypedream", "tasklog2.log")
    Task.save_folder = Path("merge_data").resolve()
    s1 = task_1()
    s2 = task_2(s1)
    s3 = task_3()
    s4 = task_4([s1, s2, s3])
    s5 = task_5([s2, s3])
    s6 = task_6([s4, s5])
    s6.__fn__
    pool = Pool(10)
    param_dict = [('id-' + x, logger, {"step_1": x, "step_3": y}) for x, y in [('5', 2), ('9', 5), ('10', 11)]]
    res = pool.starmap(s6.run, param_dict)

    assert(res[0] == '40_7.07.010!4')
    with open("merge_data/step-5/id-10.pkl", 'rb') as bfp:
        interim = pkl.load(bfp)
    assert(interim == 1815)

##
