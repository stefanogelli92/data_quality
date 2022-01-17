import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
from data_quality.src.utils import _create_filter_columns_not_null


class NotEmpthyColumn(Check):

    def __init__(self,
                 table,
                 column_name: str):
        self.table = table
        self.check_description = f"Missing value in column {column_name}"
        self.column_name = column_name
        negative_filter = _create_filter_columns_not_null(column_name)
        self.custom_check = Custom(table,
                                   negative_filter,
                                   self.check_description)

    def _get_number_ko_sql(self) -> int:
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].isna() | (df[self.column_name].astype(str) == "")]
        return df




