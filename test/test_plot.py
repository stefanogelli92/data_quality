import unittest
import logging

import pandas as pd

from data_quality.src.data_quality_holder import DataQualitySession
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
        test_table.create_html_output(save_in_path=r"plot_table.html")

    def test_plot_table_warning(self):
        df = get_dataframe_for_test("match_regex")

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df.drop(["check_description"], axis=1),
                                                            output_name="Test Table", index_column="index")
        test_table.run_basic_check(flag_warning=True)
        test_table.check_column_match_regex("A", regex=FISCALCODE_REGEX)
        test_table.create_html_output(save_in_path=r"plot_table.html")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()