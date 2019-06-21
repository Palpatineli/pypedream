##
from typing import Any
from pathlib import Path
import pickle as pkl
from multiprocessing import Pool
from pypedream import Task, Input, getLogger, FileObj, InputObj

save_folder = Path("merge_data").resolve()
Task.save_folder = save_folder
Input.save_folder = save_folder

class AlignedImageCache(FileObj):
    def load(self) -> str:
        files = self.file_path.parent.glob(self.file_path.stem + "+*")
        mtime, entry = sorted((entry.stat().st_mtime, entry) for entry in files)[-1]
        return entry.stem.split("+")[1]

    def time(self) -> float:
        files = list(self.file_path.parent.glob(self.file_path.stem + "+*"))
        if len(files) == 0:
            return 0
        return sorted(entry.stat().st_mtime for entry in files)[-1]

    def save(self, obj: str):
        folder = self.file_path.parent
        if not folder.exists():
            folder.mkdir(parents=True)
        with open(str(self.file_path) + "+" + str(obj), 'w'):
            pass
names = ('5', '9', '10')

## Setup input files
target_1 = save_folder.joinpath("input-1")
target_1.mkdir(parents=True, exist_ok=True)
for name in names:
    target_1.joinpath("id-" + name + '+' + str(int(name) ** 2)).touch()
target_3 = save_folder.joinpath("input-3")
target_3.mkdir(parents=True, exist_ok=True)
for name in names:
    target_3.joinpath("id-" + name + 'x' + str(int(name) * 2)).touch()
##
class Input1(InputObj):
    def load(self, *args) -> Any:
        targets = list(self.file_path.joinpath("input-1").glob(self.name + "+*"))
        return sorted((entry.stat().st_mtime, entry.name.split('+')[1]) for entry in targets)[-1][1]

    def time(self) -> float:
        targets = list(self.file_path.joinpath("input-1").glob(self.name + "+*"))
        if len(targets) == 0:
            return 0
        return sorted(entry.stat().st_mtime for entry in targets)[-1]
input_1 = Input(Input1, "2019-04-26T17:12")

def step_2(x):
    return int(float(x)) + 3
task_2 = Task(step_2, "2019-04-26T17:12", file_cacher=AlignedImageCache)

class Input3(InputObj):
    def load(self, *args) -> Any:
        targets = list(self.file_path.joinpath("input-3").glob(self.name + "x*"))
        return sorted((entry.stat().st_mtime, int(entry.name.split('x')[1])) for entry in targets)[-1][1]

    def time(self) -> float:
        targets = list(self.file_path.joinpath("input-3").glob(self.name + "x*"))
        if len(targets) == 0:
            return 0
        return sorted(entry.stat().st_mtime for entry in targets)[-1]
input_3 = Input(Input3, "2019-04-26T17:12")

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

    s2 = task_2(input_1)
    s4 = task_4([input_1, s2, input_3])
    s5 = task_5([s2, input_3])
    s6 = task_6([s4, s5])
    pool = Pool(10)
    params = [('id-' + x, logger) for x in names]
    res = pool.starmap(s6.run, params)

    assert(res[0] == '40_7.07.010!4')
    with open("merge_data/step-5/id-10.pkl", 'rb') as bfp:
        interim = pkl.load(bfp)
    assert(interim == 1815)

##
