from datetime import datetime
from time import mktime

class Task(object):
    def __init__(self, name: str, time: str, **kwargs):
        self.__name__ = name
        self._set_time(time)
        self.kwargs = kwargs

    def _set_time(self, time: str) -> float:
        self.__time__ = mktime(datetime.fromisoformat(time).timetuple())
        return self.__time__

    def _get_path(self) -> Path:
        self.

    def run(self):
        if 
