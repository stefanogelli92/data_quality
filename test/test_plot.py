import unittest
import logging

import pandas as pd

from data_quality.data_quality_holder import DataQualitySession
from data_quality.src.utils import FISCALCODE_REGEX


def get_dataframe_for_test(sheet_name):
    df = pd.read_excel(r"test_df.xlsx", sheet_name=sheet_name)
    return df


class TestPlot(unittest.TestCase):

    def test_plot_table(self):
        df = get_dataframe_for_test("match_regex")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            output_name="Test Table", index_column="index")
        test_table.run_basic_check()
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        test_table.create_html_output(save_in_path=r"test_plot_table.html")

    def test_plot_table_warning(self):
        df = get_dataframe_for_test("match_regex")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            output_name="Test Table", index_column="index")
        test_table.run_basic_check(flag_warning=True)
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        test_table.create_html_output(save_in_path=r"test_plot_table_warning.html")


    def test_plot_session_unique(self):
        df = get_dataframe_for_test("match_regex")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            output_name="Test Table", index_column="index")
        test_table.run_basic_check()
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        dq_session.create_html_output(save_in_path=r"test_plot_session_unique.html")

    def test_plot_session(self):
        df1 = get_dataframe_for_test("match_regex")
        df1["A"].fillna("", inplace=True)
        dq_session = DataQualitySession()
        test_table1 = dq_session.create_table_from_dataframe(df1.drop(["check_description"], axis=1),
                                                            output_name="Test Table1", index_column="index")
        test_table1.run_basic_check()
        test_table1.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        test_table1.check_not_empthy_column(columns="A")

        df2 = get_dataframe_for_test("values_in_list")
        test_table2 = dq_session.create_table_from_dataframe(df2.drop(["check_description"], axis=1),
                                                                      output_name="Test Table2")
        test_table2.check_values_in_list("A", values_list=["a", "b"], case_sensitive=False)
        dq_session.create_html_output(save_in_path=r"test_plot_session.html")




if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()