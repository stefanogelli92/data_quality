import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _create_filter_columns_null


class IndexNull(Check):

    def __init__(self,
                 table):
        super().__init__(table,
                         "Index null",
                         [table.index_column])
        self.index_column = table.index_column

    def _get_number_ko_sql(self) -> int:
        negative_filter = _create_filter_columns_null(self.index_column)
        n_ko = self.standard_get_number_ko_sql(negative_filter)
        if n_ko > 0:
            self.table.index_problem = True
        return n_ko

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        negative_filter = _create_filter_columns_null(self.index_column)
        df_ko = self.standard_rows_ko_sql(negative_filter)
        if df_ko.shape[0] > 0:
            self.table.index_problem = True
        return df_ko

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.index_column].isna() | (df[self.index_column].astype(str) == "")]
        if df.shape[0] > 0:
            self.table.index_problem = True
        return df




