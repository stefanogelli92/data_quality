import unittest
import logging

import pandas as pd
from pandas._testing import assert_frame_equal

from data_quality.data_quality_holder import DataQualitySession
from data_quality.src.utils import FISCALCODE_REGEX

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
        "custom_condition",
        "dimension_table",
        "fact_table"
    ]
    for sheet in sheet_list:
        df = pd.read_excel(r"test_df.xlsx", sheet_name=sheet)
        df.to_csv("test_data/" + sheet + ".csv", index=False)

#create_csv()


def check_results(df, table):
    df1 = df[df["check_description"].notnull()]
    df2 = table.check_list[0].ko_rows
    df1.sort_values(list(df1.columns), inplace=True)
    df2.sort_values(list(df1.columns), inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    a = pd.isna(df1)
    b = pd.isna(df2)
    df1 = df1.astype(str)
    df1[a] = None
    df2 = df2.astype(str)
    df2[b] = None
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
                                           datetime_columns="A",
                                           datetime_formats="dd-MM-yyyy"
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_datetime_format(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_datetime_format2(self):
        db_name = "datetime_format2"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           datetime_columns="A",
                                           datetime_formats="yyyy/MM/dd",
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_datetime_format(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_datetime_format3(self):
        db_name = "datetime_format2"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           datetime_columns="A",
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_datetime_format(get_rows_flag=True)
        check_results(result_df, test_table)

    def test_columns_between_values(self):
        db_name = "columns_between_values"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_columns_between_values("A", min_value=0, max_value=100, max_included=False, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_columns_between_dates(self):
        db_name = "columns_between_dates"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name
                                           )
        result_df = get_dataframe_for_test(db_name)
        test_table.check_columns_between_dates("A", datetime_formats="yyyy/MM/dd", min_date="2020-01-01", max_date="2022-01-01", get_rows_flag=True)
        check_results(result_df, test_table)

    def test_dates_order(self):
        db_name = "dates_order"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           datetime_columns=["A", "B", "C", "D"],
                                           datetime_formats=["yyyy-MM-dd HH24:MI:SS", "yyyy-MM-dd", None, None])
        result_df = get_dataframe_for_test(db_name)
        test_table.check_dates_order(["A", "B", "C", "D"], get_rows_flag=True)
        check_results(result_df, test_table)

    def test_dates_strictly_order(self):
        db_name = "dates_strictly_order"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name,
                                           datetime_columns=["A", "B", "C", "D"],
                                           datetime_formats=["yyyy-MM-dd HH24:MI:SS", None, None, None])
        result_df = get_dataframe_for_test(db_name)
        test_table.check_dates_order(["A", "B", "C", "D"], strictly_ascending=True, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_values_order(self):
        db_name = "values_order"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_values_order(["A", "B", "C", "D"], get_rows_flag=True)
        check_results(result_df, test_table)

    def test_values_strictly_order(self):
        db_name = "values_strictly_order"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_values_order(["A", "B", "C", "D"], strictly_ascending=True, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_values_in_list(self):
        db_name = "values_in_list_cs"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_values_in_list("A", values_list=["a", "b"], get_rows_flag=True)
        check_results(result_df, test_table)

    def test_values_in_list_case_insensitive(self):
        db_name = "values_in_list"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_values_in_list("A", values_list=["a", "b"], case_sensitive=False, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_match_regex(self):
        db_name = "match_regex"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_custom_condition(self):
        db_name = "custom_condition"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        result_df = get_dataframe_for_test(db_name)
        test_table.check_custom_condition("A = 3", get_rows_flag=True)
        check_results(result_df, test_table)

    def test_match_dimension_table_sql_sql1(self):
        db_name = "fact_table"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        dimension_table = bigquery.create_table(DBBIGQUERY + "dimension_table", output_name="dimension_table", index_column="id")
        result_df = get_dataframe_for_test(db_name)
        test_table.check_match_match_dimension_table("dimension_id", dimension_table, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_match_dimension_table_sql_sql2(self):
        db_name = "fact_table"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        dimension_table = bigquery.create_table(DBBIGQUERY + "dimension_table", output_name="dimension_table", index_column="id")
        result_df = get_dataframe_for_test(db_name)
        test_table.check_match_match_dimension_table(["dimension_id", "dimension_code"], dimension_table, primary_keys=["id", "code"], get_rows_flag=True)
        check_results(result_df, test_table)

    def test_match_dimension_table_sql_df(self):
        db_name = "fact_table"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        test_table = bigquery.create_table(DBBIGQUERY + db_name)
        dimension_table = get_dataframe_for_test("dimension_table")
        dimension_table = dq_session.create_table_from_dataframe(dimension_table, output_name="dimension_table", index_column="id")
        result_df = get_dataframe_for_test(db_name)
        test_table.check_match_match_dimension_table("dimension_id", dimension_table, get_rows_flag=True)
        check_results(result_df, test_table)

    def test_match_dimension_table_df_sql(self):
        db_name = "dimension_table"
        dq_session = DataQualitySession()
        bigquery = dq_session.create_sources(run_query_bigquery, type_sources="bigquery")
        dimension_table = bigquery.create_table(DBBIGQUERY + db_name, index_column="id", output_name="dimension_table")
        test_table = get_dataframe_for_test("fact_table")
        test_table = dq_session.create_table_from_dataframe(test_table, output_name="fact_table")
        result_df = get_dataframe_for_test("fact_table")
        test_table.check_match_match_dimension_table("dimension_id", dimension_table, get_rows_flag=True)
        check_results(result_df, test_table)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    unittest.main()
