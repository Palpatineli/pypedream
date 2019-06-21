from typing import Optional, Any
from os import devnull
import pickle as pkl
import subprocess as sp
from pathlib import Path

SIZE_THRESHOLD = 204800

class FileObj(object):
    """Save and load cached data. Implement this class to cache file in non-pickle format."""
    def __init__(self, file_path: Path):
        self.file_path = file_path.resolve().with_suffix("")

    def load(self) -> Any:
        raise NotImplementedError

    def save(self, obj: Any):
        raise NotImplementedError

    def time(self) -> float:
        """if no cache return 0"""
        raise NotImplementedError

    @staticmethod
    def _get_recent(file_path: Path) -> Optional[Path]:
        entries = list(file_path.parent.glob(file_path.name + ".*"))
        if len(entries) == 0:
            return None
        entries = sorted(entries, key=lambda x: x.stat().st_mtime)
        return entries[-1]

class InputObj(object):
    isInputObj = True

    def __init__(self, data_folder: Path, name: str):
        self.file_path = data_folder
        self.name = name

    def load(self, *args) -> Any:
        raise NotImplementedError

    def time(self) -> float:
        raise NotImplementedError

class Zip7Cacher(FileObj):
    """Pickle the file and then 7z it if it's too big."""
    def time(self) -> float:
        result = self._get_recent(self.file_path)
        return 0 if result is None else result.stat().st_mtime

    def load(self) -> Any:
        """Check if a file after `time` exists."""
        file_path = self._get_recent(self.file_path)
        assert file_path is not None
        return self._load(file_path)

    @staticmethod
    def _load(file_path: Path) -> Any:
        ext = file_path.suffix
        if ext == ".7z":
            with open(devnull, 'w') as fnull:
                sp.run(["7z", "e", file_path, f"-o{file_path.parent}"], stdout=fnull)
        elif ext != ".pkl":
            raise IOError(f"cannot find cache: {file_path}")
        with open(file_path.with_suffix(".pkl"), 'rb') as bfp:
            result = pkl.load(bfp)
        if ext != ".pkl":
            file_path.with_suffix(".pkl").unlink()
        return result

    def save(self, obj: Any):
        save_path = self.file_path.with_suffix(".pkl")
        folder = save_path.parent
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as fp:
            pkl.dump(obj, fp)
        if save_path.stat().st_size > SIZE_THRESHOLD:
            with open(devnull, 'w') as fnull:
                sp.run(["7z", "a", "-mx=1", save_path.with_suffix(".7z"), save_path], stdout=fnull)
            save_path.unlink()
