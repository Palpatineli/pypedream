from typing import Union, Optional, Any, Tuple
from os.path import getmtime, splitext, split
from os import remove, devnull
import pickle as pkl
import subprocess as sp
from pathlib import Path
from glob import glob

SIZE_THRESHOLD = 204800

class FileObj(object):
    """Save and load cached data. Implement this class to cache file in non-pickle format."""
    def __init__(self, file_path: Union[str, Path]):
        if isinstance(file_path, Path):
            self.file_path = splitext(str(file_path))[0]
        else:
            self.file_path = splitext(file_path)[0]

    def load(self, time: float) -> Optional[Any]:
        raise NotImplementedError

    def save(self, obj: Any):
        raise NotImplementedError

    def time(self) -> float:
        """if no cache return 0"""
        raise NotImplementedError

    @staticmethod
    def _get_recent(file_path: str) -> Optional[Tuple[float, str]]:
        files = glob(file_path + ".*")
        if len(files) == 0:
            return None
        entries = sorted([(getmtime(entry), entry) for entry in files])
        return entries[-1]

class Zip7Cacher(FileObj):
    """Pickle the file and then 7z it if it's too big."""
    def time(self) -> float:
        result = self._get_recent(self.file_path)
        return 0 if result is None else result[0]

    def load(self, time: float) -> Optional[Any]:
        """Check if a file after `time` exists."""
        result = self._get_recent(self.file_path)
        if result is None:
            return None
        return None if result[0] < time else self._load(result[1])

    @staticmethod
    def _load(file_path: str) -> Optional[Any]:
        basename, ext = splitext(file_path)
        if ext == ".7z":
            sp.run(["7z", "e", file_path, f"-o{split(basename)[0]}"])
        elif ext != ".pkl":
            return None
        with open(basename + ".pkl", 'rb') as bfp:
            result = pkl.load(bfp)
        if ext != ".pkl":
            remove(basename + ".pkl")
        return result

    def save(self, obj: Any):
        save_path = Path(self.file_path + ".pkl")
        folder = save_path.parent
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as fp:
            pkl.dump(obj, fp)
        if save_path.stat().st_size > SIZE_THRESHOLD:
            fnull = open(devnull, 'w')
            sp.run(["7z", "a", "-mx=1", save_path.with_suffix(".7z"), save_path], stdout=fnull)
            save_path.unlink()
