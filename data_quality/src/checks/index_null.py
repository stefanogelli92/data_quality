import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom


class IndexNull(Check):

    def __init__(self,
                 table):
        self.table = table
        self.check_description = "Index null"
        self.index_column = table.index_column
        negative_filter = f"({table.index_column} is null) or (cast({table.index_column} as string) = '')"
        self.custom_check = Custom(table,
                                   negative_filter,
                                   self.check_description)

    def _get_number_ko_sql(self) -> int:
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.index_column].isna() | (df[self.index_column].astype(str) == "")]
        return df




