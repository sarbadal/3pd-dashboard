import os
import chardet as chardet
import pandas as pd
from pandera.typing import DataFrame
from abc import ABC, abstractmethod
from pathlib import Path
from dataclasses import dataclass, field

from project_dirs.dirs import BASE_DIR
from settings import INPUT_CSV_PATH


@dataclass
class LoadDataABC(ABC):
    file_dir: Path = field(default=os.path.join(BASE_DIR, INPUT_CSV_PATH))

    @staticmethod
    def encoding_is(file_name=None):
        """It takes a csv file and figures the file encoding.
        Returns encoding
        """
        with open(file_name, mode='rb') as file:
            data = file.read()
            result = chardet.detect(data)
            charenc = result['encoding']
        return charenc

    @staticmethod
    def _clean_number(number: str | float | int) -> str:
        number = str(number) if str(number) else '0'
        return ''.join([i for i in number if i in '0987654321.-'])

    def __file_path(self, file_name: str):
        return os.path.join(self.file_dir, file_name)

    def _load_base_data(self, file_name, encoding=None, delimiter='\t', on_bad_lines='skip', skipfooter: int=0) -> DataFrame:
        """It loads the data as is and make it available for furthur processing."""
        file_: str = self.__file_path(file_name=file_name)
        encoding = self.encoding_is(file_) if encoding is None else encoding

        df: pd.DataFrame = pd.read_csv(
            file_,
            encoding=encoding,
            dtype=str,
            delimiter=delimiter,
            on_bad_lines=on_bad_lines,
            skipfooter=skipfooter,
            engine='python'
        )
        return df

    @abstractmethod
    def load_data(self, *args, **kwargs):
        """This method will be implemented in the respective classes.
        The main objective of this method is to load the needed data in a clean format.
        """
