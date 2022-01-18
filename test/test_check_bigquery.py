import unittest
import logging
from datetime import datetime

import pandas as pd
from pandas._testing import assert_frame_equal, assert_series_equal

from data_quality.src.data_quality_holder import DataQualitySession
from data_quality.src.utils import FISCALCODE_REGEX
from data_quality.src.check import TAG_CHECK_DESCRIPTION

from google.cloud import bigquery


def run_query_bigquery(query: str) -> pd.DataFrame:
    client = bigquery.Client.from_service_account_json(r'service_account.json')

    df = client.query(query).to_dataframe()
    return df

def get_dataframe_for_test(sheet_name):
    df = pd.read_excel(r"test_df.xlsx", sheet_name=sheet_name)
    return df

def create_csv():
    sheet_list = [
        "index_null",
        "duplicated_index",
        "not_empthy_column",
        "datetime_format1",
        "datetime_format2",
        "columns_between_values",
        "columns_between_dates",
        "dates_order",
        "dates_strictly_order",
        "values_order",
        "values_strictly_order",
        "values_in_list_cs",
        "values_in_list",
        "match_regex",
        "custom_condition"
    ]
    for sheet in sheet_list:
        df = pd.read_excel(r"test_df.xlsx", sheet_name=sheet)
        df.to_csv("test_data/" + sheet + ".csv", index=False)

create_csv()


def check_results(df, table):
    df1 = df[df["check_description"].notnull()]
    df2 = table.check_list[0].ko_rows
    df1.sort_values(list(df1.columns), inplace=True)
    df2.sort_values(list(df1.columns), inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    assert_frame_equal(df1,
                       df2,
                       check_names=False, check_dtype=False
                       )
    return

DBBIGQUERY = "data-quality-338521.test_data."


class TestCheckSQL(unittest.TestCase):

    def test_get_source_type(self):
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery)

    def test_index_null(self):
        db_name = "index_null"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           index_column="index"
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_index_not_null(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_duplicated_index(self):
        db_name = "duplicated_index"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           index_column="index"
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_duplicates_index(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_not_empthy_column(self):
        db_name = "not_empthy_column"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           not_empthy_columns="A"
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_not_empthy_column(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_datetime_format1(self):
        db_name = "datetime_format1"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           datetime_columns="A"
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_datetime_format(format="dd-MM-yyyy", get_rows_flag=True)
        check_results(result_df, test_table)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    unittest.main()
