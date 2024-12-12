import os
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from pandera.typing import DataFrame

from load_data._load_data_template import LoadDataABC
from project_dirs.dirs import BASE_DIR


CATEGORY_MAPPING_FIELDS = ['standard_3pd_brand', 'category']
CategoryDataFrame = DataFrame[CATEGORY_MAPPING_FIELDS]


@dataclass
class LoadCategoryData(LoadDataABC):
    file_dir: Path = field(default=os.path.join(BASE_DIR, 'data/mappings'))

    def load_data(self, encoding=None, file_name: str=None, delimiter=',') -> CategoryDataFrame:
        if file_name is None: file_name = 'category_mapping.csv'
        df = self._load_base_data(
            file_name=file_name,
            encoding=encoding,
            delimiter=delimiter
        )

        return df[CATEGORY_MAPPING_FIELDS]


if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 35)

    def main():
        df_loader = LoadCategoryData()
        df = df_loader.load_data()

        print(df.head(n=50))

    main()
