from typing import Any, List, Callable, Dict, Optional
from pathlib import Path
from datetime import datetime
from time import mktime
from .fileobj import FileObj, Zip7Cacher

class Task(object):
    __save_folder__ = Path("")

    def __init__(self, name: str, fn: Callable, time: str, file_cacher: type = Zip7Cacher, extra_args: tuple = tuple()):
        self.__name__ = name
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
        return hash((self.__save_folder__, self.__name__, self.__fn__))

    def path(self) -> Path:
        return self.__save_folder__.resolve().joinpath(self.__name__)

    def __call__(self, dependency=None) -> "Task":
        """
        Args:
            dependency: a previous Task, a list of Tasks or None or input node
        """
        if type(dependency) == type(self):
            if dependency.__time__ > self.__time__:
                self.__time__ = dependency.__time__
            self.dependencies = [dependency]
        else:
            if dependency is not None:
                for one_dependency in dependency:
                    if one_dependency.__time__ > self.__time__:
                        self.__time__ = one_dependency.__time__
            self.dependencies = dependency
        return self

    def _run(self, args: Dict[str, Any], logger) -> Any:
        """Runs the self.__fn__ if not have up-to-date cache, else return cached data.
        Update-to-date cache has mtime larger then the Task object.
        Use data input from args dict passed from the backend if supplied else calculate dependencies.
        If has supplied input then skip dependencies. If has cache then skip self.
        If has no dependencies and input not supplied, raise a ValueError.
        """
        if self.dependencies is None:
            if self.__name__ in args:
                logger.info(f"using input data for {self.__name__}: {args['name']}")
                return self.__fn__(args[self.__name__])
            else:
                raise ValueError(f"Input Node '{self.__name__}' lacks input.")
        else:
            cache = self.file_cacher(self.path().joinpath(args["name"]))
            if self.__name__ not in args:
                cached_result = cache.load(self.__time__)
                if cached_result is None:
                    prev_args = [task._run(args, logger) for task in self.dependencies]
                else:
                    logger.info(f"loading interim data for {self.__name__}: {args['name']}")
                    return cached_result
            else:
                logger.info(f"using input data for {self.__name__}: {args['name']}")
                prev_args = args[self.__name__]
            logger.info(f"computing output data for {self.__name__}: {args['name']}")
            result = self.__fn__(*(tuple(prev_args) + self.extra_args))
            cache.save(result)
            return result

    def run(self, args: Dict[str, Any]) -> Any:
        """prep info and get ready to be run"""
        logger = args["logger"]
        return self._run(args, logger)
