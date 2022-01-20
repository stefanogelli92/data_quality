import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
from data_quality.src.utils import _create_filter_columns_not_null, _create_filter_columns_null


class IndexNull(Check):

    def __init__(self,
                 table):
        self.table = table
        self.check_description = "Index null"
        self.index_column = table.index_column

        self.custom_check = None

    def _get_number_ko_sql(self) -> int:
        negative_filter = _create_filter_columns_null(self.index_column)
        self.custom_check = Custom(self.table,
                                  negative_filter,
                                  self.check_description)
        self.custom_check.n_max_rows_output = self.n_max_rows_output
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.index_column].isna() | (df[self.index_column].astype(str) == "")]
        return df




