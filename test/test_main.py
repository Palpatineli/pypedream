from pypeln import process as pr
from pypedream import Task

class Step1(Task):
    __name__ = "step-1"
    __time__ = "2019-04-25T10:18"

    def func(self, obj):
        return obj * 2

class Step2(Task):
    __name__ = "step-2"
    __time__ = "2019-04-25T10:19"

def test_basic():
    pr.map(Step2)
##
