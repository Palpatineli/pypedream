# Pypedream

Persistence in a pipeline to be used with another pipelining package such as [pypeln](https://cgarciae.github.io/pypeln/).

```python3
from pypedream import Task, getLogger

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

Task.__save_folder__ = Path("merge_data").resolve()  # set project cache folder
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
logger = getLogger("pypedream", "tasklog2.log")  # set logger name and file name
pool = Pool(10)
param_dict = [{"step-1": x, "step-3": y, "name": 'id-' + x, "logger": logger}
              for x, y in [('5', 2), ('9', 5), ('10', 11)]]
res = pool.map(s6.run, param_dict)
```
