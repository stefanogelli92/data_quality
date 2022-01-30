import unittest
import logging

import pandas as pd
from pandas._testing import assert_frame_equal

from data_quality.data_quality_holder import DataQualitySession
from data_quality.src.utils import FISCALCODE_REGEX


def get_dataframe_for_test(sheet_name):
    df = pd.read_excel(r"test_df.xlsx", sheet_name=sheet_name)
    return df

def check_results(df, table, same_columns=True):
    df1 = df[df["check_description"].notnull()]
    df2 = table.check_list[0].ko_rows
    df1.sort_values(list(df1.columns), inplace=True)
    df2.sort_values(list(df1.columns), inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    if not same_columns:
        df2 = df2[df1.columns]
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


class TestCheckDataframe(unittest.TestCase):

    def test_index_null(self):

        df = get_dataframe_for_test("index_null")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            index_column="index")
        test_table.check_index_not_null()
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_duplicated_index(self):

        df = get_dataframe_for_test("duplicated_index")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            index_column="index")
        test_table.check_duplicate_index()
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_not_empthy_column(self):

        df = get_dataframe_for_test("not_empthy_column")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            not_empthy_columns="A")
        test_table.check_not_empthy_column()
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_datetime_format1(self):

        df = get_dataframe_for_test("datetime_format1")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            datetime_columns="A",
                                                            datetime_formats="%d-%m-%Y")
        test_table.check_datetime_format()
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_datetime_format2(self):

        df = get_dataframe_for_test("datetime_format2")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            datetime_columns="A",
                                                            datetime_formats="%Y-%m-%d")
        test_table.check_datetime_format()
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_columns_between_values(self):

        df = get_dataframe_for_test("columns_between_values")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_columns_between_values("A", min_value=0, max_value=100, max_included=False)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_columns_between_dates(self):

        df = get_dataframe_for_test("columns_between_dates")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_columns_between_dates("A", min_date="2020-01-01", max_date="2022-01-01")
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_dates_order(self):

        df = get_dataframe_for_test("dates_order")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_dates_order(["A", "B", "C", "D"])
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_dates_strictly_order(self):

        df = get_dataframe_for_test("dates_strictly_order")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_dates_order(["A", "B", "C", "D"], strictly_ascending=True)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_values_order(self):

        df = get_dataframe_for_test("values_order")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_values_order(["A", "B", "C", "D"])
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_values_strictly_order(self):

        df = get_dataframe_for_test("values_strictly_order")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_values_order(["A", "B", "C", "D"], strictly_ascending=True)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_values_in_list(self):

        df = get_dataframe_for_test("values_in_list_cs")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_values_in_list("A", values_list=["a", "b"])
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_values_in_list_case_insensitive(self):
        df = get_dataframe_for_test("values_in_list")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_values_in_list("A", values_list=["a", "b"], case_sensitive=False)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_match_regex(self):
        df = get_dataframe_for_test("match_regex")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_custom_condition(self):
        df = get_dataframe_for_test("custom_condition")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_custom_condition("A == 3")
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_match_dimension_table1(self):
        df = get_dataframe_for_test("fact_table")
        dimension_table = get_dataframe_for_test("dimension_table")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        dimension_table = dq_session.create_table_from_dataframe(dimension_table, output_name="dimension_table", index_column="id")
        test_table.check_match_dimension_table("dimension_id", dimension_table)
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_match_dimension_table2(self):
        df = get_dataframe_for_test("fact_table")
        dimension_table = get_dataframe_for_test("dimension_table")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        dimension_table = dq_session.create_table_from_dataframe(dimension_table, output_name="dimension_table", index_column="id")
        test_table.check_match_dimension_table(["dimension_id", "dimension_code"], dimension_table, primary_keys=["id", "code"])
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_dates_order_dimension_table(self):
        df = get_dataframe_for_test("products")
        dimension_table = get_dataframe_for_test("user_list")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        dimension_table = dq_session.create_table_from_dataframe(dimension_table, output_name="dimension_table", index_column="id")
        test_table.check_dates_order_dimension_table("user_id", dimension_table, "selling_date", "registration_date", ">=")
        check_results(df, test_table, same_columns=False)

    def test_values_order_dimension_table(self):
        df = get_dataframe_for_test("products2")
        dimension_table = get_dataframe_for_test("user_list")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        dimension_table = dq_session.create_table_from_dataframe(dimension_table, output_name="dimension_table", index_column="id")
        test_table.check_values_order_dimension_table("user_id", dimension_table, "n_products", "max_products", "<=")
        check_results(df, test_table, same_columns=False)

    def test_period_intersection_rows1(self):

        df = get_dataframe_for_test("period_intersection")

        dq_session = DataQualitySession()
        df = df[df["user_id"] == 2]
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_period_intersection_rows(start_date="subscription_start",
                                                  end_date="subscription_end")
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_period_intersection_rows2(self):
        df = get_dataframe_for_test("period_intersection")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1))
        test_table.check_period_intersection_rows(id_columns="user_id",
                                                  start_date="subscription_start",
                                                  end_date="subscription_end")
        assert_frame_equal(test_table.check_list[0].ko_rows,
                           df[df["check_description"].notnull()],
                           check_names=False, check_dtype=False
                           )

    def test_create_result_df(self):
        df = get_dataframe_for_test("fact_table")
        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1), index_column="index",
                                                            not_empthy_columns=["dimension_id"])
        test_table.run_basic_check()
        test_table.check_values_in_list("dimension_code", ["a", "b", "c", "d"])
        test_table.get_ko_rows()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
