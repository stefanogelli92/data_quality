from typing import Union, List

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _aggregate_sql_filter


class Custom(Check):

    def __init__(self,
                 table,
                 negative_filter: str,
                 check_description: str
                 ):
        super().__init__(table, check_description)
        self.negative_filter = negative_filter

    def _get_number_ko_sql(self) -> int:
        return self.standard_get_number_ko_sql(self.negative_filter)

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        # TODO rivedere query trovare una funzione che prenda codice sql
        ignore_filters = _aggregate_sql_filter(self.ignore_filters)
        df = self.table.df
        if isinstance(self.columns_not_null, str):
            df = df[df[self.columns_not_null].notnull() & (df[self.columns_not_null].astype(str) != "")]
        elif isinstance(self.columns_not_null, list):
            for col in self.columns_not_null:
                df = df[df[col].notnull() & (df[col].astype(str) != "")]

        if (ignore_filters is not None) and (len(ignore_filters)>0):
            df = df.query(ignore_filters)

        #n_tot = df.shape[0]
        df = df.query(self.negative_filter)
        return df

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.standard_rows_ko_sql(self.negative_filter)



