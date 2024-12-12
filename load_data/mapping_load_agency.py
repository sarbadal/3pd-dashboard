import os
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from pandera.typing import DataFrame

from load_data._load_data_template import LoadDataABC
from project_dirs.dirs import BASE_DIR
from settings import MAPPING_CSV_PATH


AGENCY_MAPPING_FIELDS = ['advertiser', 'sf_advertiser', 'agency', 'region']
DEFAULT_AGENCY_MAPPING_FILE = 'agency_mapping.csv'
AgencyMappingDataFrame = DataFrame[AGENCY_MAPPING_FIELDS]


@dataclass
class LoadAgencyMappingData(LoadDataABC):
    file_dir: Path = field(default=os.path.join(BASE_DIR, MAPPING_CSV_PATH))

    def load_data(self, encoding=None, file_name: str=None, delimiter=',') -> AgencyMappingDataFrame:
        if file_name is None: file_name = DEFAULT_AGENCY_MAPPING_FILE
        df = self._load_base_data(
            file_name=file_name,
            encoding=encoding,
            delimiter=delimiter
        )

        return df[AGENCY_MAPPING_FIELDS]


if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 35)


    def main():
        df_loader = LoadAgencyMappingData()
        df = df_loader.load_data()

        print(df.head(n=500))

    main()
