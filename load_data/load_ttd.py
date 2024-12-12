from functools import partial
import pandas as pd
from pandera.typing import DataFrame
from load_data._load_data_template import LoadDataABC
from funcs.daterelated import get_last_date


_MONTH_FIELD = 'month'
_ADVERTISER = 'advertiser'

TTD_DF_FIELDS = [
    _MONTH_FIELD,
    _ADVERTISER,
    'campaign',
    'original_3pd_brand',
    'data_element',
    'impressions',
    'advertiser_cost',
    'charged_data_element_cost',
    'media_cost',
    'partner_cost',
    'ttd_cost',
]

TTDDataFrame = DataFrame[TTD_DF_FIELDS]


class LoadTTDData(LoadDataABC):

    def load_data(self, encoding=None, file_name: str=None, delimiter=',', year: int=None) -> TTDDataFrame:
        df = self._load_base_data(
            file_name=file_name,
            encoding=encoding,
            delimiter=delimiter
        )
        rename_columns = {
            'Month': 'month',
            'Advertiser': 'advertiser',
            'Campaign': 'campaign',
            '3rd Party Data Brand': 'original_3pd_brand',
            'Data Element': 'data_element',
            'Impressions': 'impressions',
            'Advertiser Cost (USD)': 'advertiser_cost',
            'Charged Data Element Cost (USD)': 'charged_data_element_cost',
            'Media Cost (USD)': 'media_cost',
            'Partner Cost (USD)': 'partner_cost',
            'TTD Cost (USD)': 'ttd_cost',
        }

        number_fields = [
            'impressions',
            'advertiser_cost',
            'charged_data_element_cost',
            'media_cost',
            'partner_cost',
            'ttd_cost'
        ]

        df = df.rename(columns=rename_columns, inplace=False)
        df[_ADVERTISER] = df[_ADVERTISER].apply(lambda x: str(x).strip())

        for col in number_fields:
            df[col] = df[col].apply(self._clean_number)
            if col == 'impressions':
                df[col] = df[col].astype(int)
            else:
                df[col] = df[col].astype(float)

        last_day = partial(get_last_date, year=year)
        month_field = {
            str(v).casefold(): v for v in df.columns
            if str(v).casefold() == _MONTH_FIELD
        }
        month_field_name = month_field.get(_MONTH_FIELD, '-1')

        if month_field_name in df.columns:
            df[month_field_name] = df[month_field_name].apply(last_day)

        return df[TTD_DF_FIELDS]


if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 35)

    db_fields = [
        'category',
        'region',  # 'advertiser'
        'agency',  # 'advertiser'
        'sfadvertiser',  # 'advertiser'
        'audience_list',
        'standard_3pd_provider',
        'data_provider',
        'insertion_order',  # campaign
        'advertiser',
        'dsp',
        'month',
        'data_fee',
        'impressions',
    ]
