from typing import Any, List, Callable, Optional, Tuple, Type, Union
from inspect import getfullargspec
from pathlib import Path
from datetime import datetime
from time import mktime
from multiprocessing import Pool, cpu_count
from logging import Logger
from .fileobj import FileObj, Zip7Cacher, InputObj
from .logger import getLogger

class TaskMixin(object):
    save_folder = Path("")
    __name__ = ""

    def _set_time(self, time: str) -> float:
        self.__time__ = mktime(datetime.fromisoformat(time).timetuple())
        return self.__time__

    def __hash__(self) -> int:
        return hash((self.save_folder, self.__name__, self.__fn__))  # type: ignore

    def path(self) -> Path:
        return self.save_folder.resolve().joinpath(self.__name__)

    def _needs_update(self, name: str, logger: Logger) -> Tuple[bool, float]:
        raise NotImplementedError

    def run(self, name: str, logger: Logger) -> Any:
        raise NotImplementedError

class Task(TaskMixin):
    def __init__(self, fn: Callable, time: str, name: Optional[str] = None, file_cacher: type = Zip7Cacher,
                 extra_args: tuple = tuple()):
        self.__name__ = name if name is not None else fn.__name__
        self._set_time(time)
        self.__fn__ = fn
        assert issubclass(file_cacher, FileObj), "A file cacher class must be a subtype of FileObj"
        self.file_cacher = file_cacher
        self.extra_args = extra_args
        self.dependencies: List[TaskMixin] = list()

    @property
    def arg_types(self) -> List[type]:
        arg_types = getfullargspec(self.__fn__)
        return [arg_types.annotations.get(x, Any) for x in arg_types.args]

    @property
    def return_type(self) -> type:
        return getfullargspec(self.__fn__).annotations.get('return', Any)

    def __call__(self, dependency=None) -> "Task":
        """
        Args:
            dependency: a previous Task, a list of Tasks or None or input node
        """
        self.dependencies = [dependency] if isinstance(dependency, TaskMixin) else dependency
        return self

    def __str__(self) -> str:
        return self.__name__ + ": " + datetime.fromtimestamp(self.__time__).isoformat()

    def _needs_update(self, name, logger) -> Tuple[bool, float]:
        cache = self.file_cacher(self.path().joinpath(name))
        truth_flag = False
        dep_time = 0.
        for x in self.dependencies:
            dep_update, _dep_time = x._needs_update(name, logger)
            if dep_update:
                truth_flag = True
            else:
                dep_time = max(_dep_time, dep_time)
        if truth_flag:
            logger.debug(f"[Update Needed] due to dependency. {name}: {self.__name__}")
            return True, 0
        own_time = cache.time()
        if (own_time < self.__time__):
            logger.debug(f"[Update Needed] self. {name}: {self.__name__} <{own_time} < {self.__time__}>")
            return True, own_time
        if dep_time > own_time:
            logger.debug(f"[Update Needed] dependency newer than self."
                         f" {name}: {self.__name__} <{own_time} < {dep_time}>")
            return True, own_time
        logger.debug(f"[Update Not Needed] {name}: {self.__name__}")
        return False, own_time

    def run(self, name: str, logger: Logger) -> Any:
        """Runs the self.__fn__ if not have up-to-date cache, else return cached data.
        Update-to-date cache has mtime larger then the Task object.
        Use data input from args dict passed from the backend if supplied else calculate dependencies.
        If has supplied input then skip dependencies. If has cache then skip self.
        If has no dependencies and input not supplied, raise a ValueError.
        """
        cache = self.file_cacher(self.path().joinpath(name))
        logger.debug(f"check update from {self.__name__}")
        if not self._needs_update(name, logger)[0]:
            logger.info(f"[Cache Hit] loading interim data. {self.__name__}: {name}")
            result = cache.load()
        elif self.dependencies is not None:
            logger.debug(f"[Cache Miss] {self.__name__}: {name} "
                         f"| time: {cache.time()} -> {self.__time__}")
            prev_args = [task.run(name, logger) for task in self.dependencies]
            logger.info(f"[Cache Miss] using dependencies. {self.__name__}: {name}")
            try:
                result = self.__fn__(*(tuple(prev_args) + self.extra_args))
            except Exception as e:
                logger.error(f"[Exception] stage: {self.__name__}, case: {name}")
                raise e
            cache.save(result)
        else:
            raise ValueError(f"Input Node '{self.__name__}' lacks input.")
        return result

class Input(TaskMixin):
    def __init__(self, loader: Type[InputObj], time: str, fn_name: Optional[str] = None, extra_args: tuple = tuple()):
        self.__name__ = fn_name if fn_name is not None else loader.__name__
        self._set_time(time)
        assert hasattr(loader, "isInputObj"), "A file cacher class must be a subtype of InputObj"
        self.__loader__ = loader
        self.extra_args = extra_args

    @property
    def arg_types(self) -> List[type]:
        arg_types = getfullargspec(self.__loader__.load)
        return [str] + [arg_types.annotations.get(x, Any) for x in arg_types.args]  # type: ignore

    @property
    def return_type(self) -> type:
        return getfullargspec(self.__loader__.load).annotations.get('return', Any)

    def __str__(self) -> str:
        return self.__name__ + "[input]: " + datetime.fromtimestamp(self.__time__).isoformat()

    def _needs_update(self, name: str, logger: Logger) -> Tuple[bool, float]:
        return False, (max(self.__time__, self.__loader__(self.save_folder, name).time()))

    def run(self, name: str, logger: Logger) -> Any:
        loader = self.__loader__(self.save_folder, name)
        logger.debug(f"[Input] read input file. {self.__name__}: {name}")
        return loader.load(*self.extra_args)

def get_result(cases: list, tasks: Union[Task, List[Task]], name: str = "default") -> list:
    logger = getattr(get_result, "logger", None)
    if logger is None:
        logger = getLogger(name, name + ".log")
        get_result.logger = logger  # type: ignore
    pool = Pool(max(1, cpu_count() - 5))
    params = [(case, logger) for case in cases]
    if isinstance(tasks, Task):
        return pool.starmap(tasks.run, params)
    else:
        output = list()
        for task in tasks:
            output.append(pool.starmap(task.run, params))
        return output
