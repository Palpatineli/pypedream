from typing import Any, List, Callable, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
from time import mktime
from logging import Logger
from .fileobj import FileObj, Zip7Cacher

class Task(object):
    save_folder = Path("")

    def __init__(self, fn: Callable, time: str, name: Optional[str] = None, file_cacher: type = Zip7Cacher,
                 extra_args: tuple = tuple()):
        self.__name__ = name if name is not None else fn.__name__
        self._set_time(time)
        self.__fn__ = fn
        if not issubclass(file_cacher, FileObj):
            raise ValueError("A file cacher class must be a subtype of FileObj")
        self.file_cacher = file_cacher
        self.extra_args = extra_args
        self.dependencies: Optional[List[Task]] = list()

    def _set_time(self, time: str) -> float:
        self.__time__ = mktime(datetime.fromisoformat(time).timetuple())
        return self.__time__

    def __hash__(self) -> int:
        return hash((self.save_folder, self.__name__, self.__fn__))

    def path(self) -> Path:
        return self.save_folder.resolve().joinpath(self.__name__)

    def __call__(self, dependency=None) -> "Task":
        """
        Args:
            dependency: a previous Task, a list of Tasks or None or input node
        """
        if type(dependency) == type(self):
            self.dependencies = [dependency]
            if dependency.__time__ > self.__time__:
                self.__time__ = dependency.__time__
        else:
            self.dependencies = dependency
            if dependency is not None:
                self.__time__ = max(max(x.__time__ for x in dependency), self.__time__)
        return self

    def __str__(self, level: int = 0) -> str:
        if self.dependencies is None:
            return self.__name__ + "[input]: " + datetime.fromtimestamp(self.__time__).isoformat()
        else:
            return self.__name__ + ": " + datetime.fromtimestamp(self.__time__).isoformat()

    def _update_time(self, name: str) -> float:
        cache = self.file_cacher(self.path().joinpath(name))
        if self.dependencies is not None:
            self._cache_time = min(cache.time(), min(x._update_time(name) for x in self.dependencies))
        else:
            self._cache_time = cache.time()
        return self._cache_time

    def run(self, name: str, logger: Logger, args: Dict[str, Any]) -> Any:
        self._update_time(name)
        return self._run(name, logger, args)

    def _run(self, name: str, logger: Logger, args: Dict[str, Any]) -> Any:
        """Runs the self.__fn__ if not have up-to-date cache, else return cached data.
        Update-to-date cache has mtime larger then the Task object.
        Use data input from args dict passed from the backend if supplied else calculate dependencies.
        If has supplied input then skip dependencies. If has cache then skip self.
        If has no dependencies and input not supplied, raise a ValueError.
        """
        cache = self.file_cacher(self.path().joinpath(name))
        result = cache.load(self.__time__) if (self.__time__ < self._cache_time) else None
        if result is not None:  # if we have cached result then use result
            logger.info(f"loading interim data for {self.__name__}: {name}")
        elif self.__name__ in args:  # if has named input in args dict then don't calculate from dependencies
            logger.info(f"using input data for {self.__name__}: {name}")
            result = self.__fn__(args[self.__name__], *self.extra_args)
            cache.save(result)
        elif self.dependencies is not None:  # use input calculated from dependencies
            prev_args = [task._run(name, logger, args) for task in self.dependencies]
            logger.info(f"computing output data for {self.__name__}: {name}")
            result = self.__fn__(*(tuple(prev_args) + self.extra_args))
            cache.save(result)
        else:
            raise ValueError(f"Input Node '{self.__name__}' lacks input.")
        return result

try:
    from print_tree import print_tree

    class print_task(print_tree):
        def get_children(self, task: Task) -> List[Task]:
            return [] if task.dependencies is None else task.dependencies

        def get_node_str(self, task: Task) -> str:
            node_type = "[Input]" if task.dependencies is None else ""
            time_str = datetime.fromtimestamp(task.__time__).strftime("%m/%dT%H:%M")
            return f"{{{task.__name__}: {node_type}<{time_str}>}}"
except ImportError:
    def print_task(task: Task) -> str:  # type: ignore
        raise NotImplementedError
