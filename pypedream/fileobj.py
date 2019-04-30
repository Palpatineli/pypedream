from typing import Union, Optional, Any
from os.path import getmtime, splitext, split
from os import remove
import pickle as pkl
import subprocess as sp
from pathlib import Path
from glob import glob

SIZE_THRESHOLD = 20

class FileObj(object):
    """Save and load cached data. Implement this class to cache file in non-pickle format."""
    def __init__(self, file_path: Union[str, Path]):
        if isinstance(file_path, Path):
            self.file_path = splitext(str(file_path))[0]
        else:
            self.file_path = splitext(file_path)[0]

    def exists(self) -> bool:
        raise NotImplementedError

    def load(self, time: float) -> Optional[Any]:
        raise NotImplementedError

    def save(self, obj: Any):
        raise NotImplementedError

class Zip7Cacher(FileObj):
    """Pickle the file and then 7z it if it's too big."""
    def exists(self) -> bool:
        return len(glob(self.file_path + ".*")) > 0

    @staticmethod
    def _get_recent(file_path: str, time: float) -> Optional[str]:
        files = glob(file_path + ".*")
        if len(files) == 0:
            return None
        entries = sorted([(getmtime(entry), entry) for entry in files])
        return entries[-1][1] if entries[-1][0] > time else None

    def load(self, time: float) -> Optional[Any]:
        """Check if a file after `time` exists."""
        file_name = self._get_recent(self.file_path, time)
        return None if file_name is None else self._load(file_name)

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
            folder.mkdir(parents=True)
        with open(save_path, 'wb') as fp:
            pkl.dump(obj, fp)
        if save_path.stat().st_size > SIZE_THRESHOLD:
            sp.run(["7z", "a", "-mx=1", save_path.with_suffix(".7z"), save_path])
            save_path.unlink()
