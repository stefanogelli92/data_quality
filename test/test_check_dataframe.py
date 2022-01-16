import unittest
import logging
from datetime import datetime

import pandas as pd

from data_quality.src.data_quality_holder import DataQualitySession


def get_dataframe_for_test():
    df = pd.read_excel(r"test_df.xlsx")
    return df


class TestCheckDataframe(unittest.TestCase):

    def test1(self):

        df = get_dataframe_for_test()

        dq_session = DataQualitySession()
        test_table = dq_session.create_table_from_dataframe(df,
                                                            index_column="index",
                                                            not_empthy_columns=["A", "B"],
                                                            datetime_columns=["C", "D", "E", "F"])
        test_table.run_basic_check()
        test_table.check_columns_between_values("A", min_value=0, max_value=100)
        test_table.check_columns_between_dates("C", min_date="2020-01-01", max_date=datetime(2022, 1, 1))
        test_table.check_dates_order(["D", "E", "F"])
        test_table.check_values_order(["G", "H", "I"])
        test_table.check_values_in_list("L", values_list=["A", "b"], case_sensitive=False)
        prova = ""


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
