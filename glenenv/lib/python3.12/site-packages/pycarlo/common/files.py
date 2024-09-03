import json
from pathlib import Path
from typing import Callable, Dict, Generic, TypeVar, Union

# file reader return type
T = TypeVar("T")


class FileReader(Generic[T]):
    """
    Utility for reading a local file. Return type is determined by the given `decoder` function.
    """

    def __init__(self, path: Union[Path, str], decoder: Callable[[bytes], T]):
        """
        :param path: local file path
        :param decoder: function that translates file content as bytes into an object of type T
        """
        self._path = to_path(path)
        self._decoder = decoder

    def read(self) -> T:
        """
        Read local file.

        :return: contents of file, represented as type T
        """
        with open(self._path, "rb") as file:
            content = file.read()
            return self._decoder(content)


class BytesFileReader(FileReader[bytes]):
    """
    Utility for reading a local file as bytes.
    """

    def __init__(self, path: Union[Path, str]):
        """
        :param path: local file path
        """
        super().__init__(path=path, decoder=lambda b: b)


class JsonFileReader(FileReader[Dict]):
    """
    Utility for reading a local JSON file as a dictionary.
    """

    def __init__(self, path: Union[Path, str]):
        """
        :param path: local file path
        """
        super().__init__(path=path, decoder=lambda b: json.loads(b))


class TextFileReader(FileReader[str]):
    """
    Utility for reading a local file as a string.
    """

    def __init__(self, path: Union[Path, str], encoding: str = "utf-8"):
        """
        :param path: local file path
        :param encoding: character encoding to use when translating file content to a string
        """
        super().__init__(path=path, decoder=lambda b: b.decode(encoding))


def to_path(path: Union[Path, str]) -> Path:
    """
    Simple function to normalize a file path passed as either a string or pathlib.Path to a
    pathlib.Path instance.

    :param path: local file path
    :return: local file path represented as an instance of pathlib.Path
    """
    return path if isinstance(path, Path) else Path(path)
