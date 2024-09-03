from pathlib import Path


def mkdirs(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)
