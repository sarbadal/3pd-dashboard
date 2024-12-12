import pandas as pd
from pandera.typing import DataFrame
from load_data._load_data_template import LoadDataABC
from funcs.daterelated import get_last_date


_MONTH_FIELD = 'month'
_ADVERTISER = 'advertiser'

DV360_DF_FIELDS = [
    _MONTH_FIELD,
    _ADVERTISER,
    'partner',
    'partner_id',
    'partner_currency',
    'advertiser_id',
    'advertiser_currency',
    'insertion_order',
    'insertion_order_id',
    'targeted_data_providers',
    'audience_list',
    'audience_list_id',
    'audience_list_cost',
    'audience_list_type',
    'impressions',
    'charged_data_element_cost',
]

DV360DataFrame = DataFrame[DV360_DF_FIELDS]

class LoadDV360Data(LoadDataABC):

    def load_data(self, encoding=None, file_name: str=None, delimiter=',', year: int=None, skipfooter: int=20) -> DV360DataFrame:
        df = self._load_base_data(
            file_name=file_name,
            encoding=encoding,
            delimiter=delimiter,
            skipfooter=skipfooter
        )

        rename_columns = {
            'Partner': 'partner',
            'Partner ID': 'partner_id',
            'Partner Currency': 'partner_currency',
            'Advertiser': 'advertiser',
            'Advertiser ID': 'advertiser_id',
            'Advertiser Currency': 'advertiser_currency',
            'Insertion Order': 'insertion_order',  # campaign
            'Insertion Order ID': 'insertion_order_id',
            'Targeted Data Providers': 'targeted_data_providers',  # original_3pd_brand
            'Audience List': 'audience_list',
            'Audience List ID': 'audience_list_id',
            'Audience List Cost (USD)': 'audience_list_cost',
            'Audience List Type': 'audience_list_type',
            'Month': 'month',
            'Impressions': 'impressions',
            'Data Fees (Adv Currency)': 'charged_data_element_cost',
        }

        number_fields = ['impressions', 'charged_data_element_cost']

        df = df.rename(columns=rename_columns, inplace=False)
        df[_ADVERTISER] = df[_ADVERTISER].apply(lambda x: str(x).strip())

        for col in number_fields:
            df[col] = df[col].apply(self._clean_number)
            if col == 'impressions':
                df[col] = df[col].astype(int)
            else:
                df[col] = df[col].astype(float)

        month_field = {
            str(v).casefold(): v for v in df.columns
            if str(v).casefold() == _MONTH_FIELD
        }
        month_field_name = month_field.get(_MONTH_FIELD, '-1')

        if month_field_name in df.columns:
            df[month_field_name] = df[month_field_name].apply(
                lambda x: get_last_date(
                    month=str(x).split('/')[-1],
                    year=str(x).split('/')[0] if year is None else year
                )
            )

        return df[DV360_DF_FIELDS]


if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 50)

    def main():
        df_load = LoadDV360Data()
        df = df_load.load_data(
            file_name='DV360_MonthlyAudienceFee_Report_20231025.csv'
        )

        df = df[(df['impressions'] > 0) | (df['charged_data_element_cost'] > 0)].reset_index(drop=True).copy()

        print(df.tail(n=100))
        print(df.shape)

    main()
