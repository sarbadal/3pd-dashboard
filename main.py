"""The main programme / entry point."""
import pandas as pd
import os
import datetime
from pandera.typing import DataFrame

from processing.api import process
from load_data.api import LoadSTD3PDBrandData, LoadAgencyMappingData, LoadCategoryData
from processing.api import ApplyMapping
from project_dirs.dirs import BASE_DIR

from sql.redshift import cursor
from sql.alchemy import df_to_sql



def main() -> None:
    today = datetime.date.today()
    outfile = os.path.join(BASE_DIR, f'export/combined_processed_data_{today}.csv')

    df = process()

    # brand_loader, agency_loader, category_loader = (
    #     LoadSTD3PDBrandData(), LoadAgencyMappingData(), LoadCategoryData()
    # )

    # std_df, agn_df, cat_df = (
    #     brand_loader.load_data(),
    #     agency_loader.load_data(),
    #     category_loader.load_data()
    # )

    brand_loader = LoadSTD3PDBrandData()
    agency_loader = LoadAgencyMappingData()
    category_loader = LoadCategoryData()

    std_df = brand_loader.load_data()
    agn_df = agency_loader.load_data()
    cat_df = category_loader.load_data()

    apply_brand_mapping = ApplyMapping(
        data=df,
        mapping_data=std_df,
        left_on='original_3pd_brand',
        right_on='original_3pd_brand',
    )
    final_df = apply_brand_mapping.apply()

    apply_agn_mapping = ApplyMapping(
        data=final_df,
        mapping_data=agn_df,
        left_on='advertiser',
        right_on='advertiser',
    )
    final_df = apply_agn_mapping.apply()

    apply_cat_mapping = ApplyMapping(
        data=final_df,
        mapping_data=cat_df,
        left_on='standard_3pd_brand',
        right_on='standard_3pd_brand',
    )

    final_df: DataFrame = apply_cat_mapping.apply()
    final_df.to_csv(outfile, index=False)

    print(final_df.head(50))
    print(final_df.shape)

    # df_to_sql(
    #     final_df,
    #     tablename='three_pd_processed_test',
    #     schema='annalect_busops',
    #     if_exists='append'
    # )




if __name__ == '__main__':
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', 35)

    main()
