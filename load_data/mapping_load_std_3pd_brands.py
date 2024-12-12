import os
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from pandera.typing import DataFrame

from load_data._load_data_template import LoadDataABC
from project_dirs.dirs import BASE_DIR


STD_3PD_BRAND_MAPPING_FIELDS = ['original_3pd_brand', 'standard_3pd_brand']
STD3PDBrandDataFrame = DataFrame[STD_3PD_BRAND_MAPPING_FIELDS]


@dataclass
class LoadSTD3PDBrandData(LoadDataABC):
    file_dir: Path = field(default=os.path.join(BASE_DIR, 'data/mappings'))

    def load_data(self, encoding=None, file_name: str=None, delimiter=',') -> STD3PDBrandDataFrame:
        if file_name is None: file_name = 'std_3pd_brand.csv'
        df = self._load_base_data(
            file_name=file_name,
            encoding=encoding,
            delimiter=delimiter
        )

        return df[STD_3PD_BRAND_MAPPING_FIELDS]


if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 35)


    def main():
        df_loader = LoadSTD3PDBrandData()
        df = df_loader.load_data()

        # print(df.head(n=50))
        brands = sorted(list(df['standard_3pd_brand'].unique()), key=lambda x: x[0])
        for brand in enumerate(brands):
            print(brand[1])

    main()
