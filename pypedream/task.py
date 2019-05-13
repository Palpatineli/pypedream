from typing import Any, List, Callable, Dict, Optional
from pathlib import Path
from datetime import datetime
from time import mktime, time as now
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
        self.dependencies = [dependency] if type(dependency) == type(self) else dependency
        return self

    def __str__(self, level: int = 0) -> str:
        if self.dependencies is None:
            return self.__name__ + "[input]: " + datetime.fromtimestamp(self.__time__).isoformat()
        else:
            return self.__name__ + ": " + datetime.fromtimestamp(self.__time__).isoformat()

    def _needs_update(self, name, logger) -> bool:
        cache = self.file_cacher(self.path().joinpath(name))
        truth_flag = False
        if self.dependencies is not None:
            for x in self.dependencies:
                if x._needs_update(name, logger):
                    truth_flag = True
        if truth_flag:
            logger.info(f"[Cache Miss] due to dependency. {name}: {self.__name__}")
            return True
        if (cache.time() < self.__time__):
            logger.info(f"[Cache Miss] self. {name}: {self.__name__}")
            return True
        return False

    def run(self, name: str, logger: Logger, args: Dict[str, Any]) -> Any:
        """Runs the self.__fn__ if not have up-to-date cache, else return cached data.
        Update-to-date cache has mtime larger then the Task object.
        Use data input from args dict passed from the backend if supplied else calculate dependencies.
        If has supplied input then skip dependencies. If has cache then skip self.
        If has no dependencies and input not supplied, raise a ValueError.
        """
        cache = self.file_cacher(self.path().joinpath(name))
        if not self._needs_update(name, logger):
            logger.info(f"[Cache Hit] loading interim data. {self.__name__}: {name}")
            result = cache.load(self.__time__)
        elif self.__name__ in args:
            logger.debug(f"[Cache Miss] {self.__name__}: {name} "
                         f"| time: {cache.time()} -> {self.__time__}")
            logger.info(f"[Cache Miss] using input args. {self.__name__}: {name}")
            result = self.__fn__(args[self.__name__], *self.extra_args)
            cache.save(result)
        elif self.dependencies is not None:
            logger.debug(f"[Cache Miss] {self.__name__}: {name} "
                         f"| time: {cache.time()} -> {self.__time__}")
            prev_args = [task.run(name, logger, args) for task in self.dependencies]
            logger.info(f"[Cache Miss] using dependencies. {self.__name__}: {name}")
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
