from typing import Union, Optional

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _create_filter_columns_not_null


class ValuesInList(Check):

    def __init__(self,
                 table,
                 column_name: str,
                 values_list: list,
                 case_sensitive: bool = True
                 ):
        self.table = table
        self.column_name = column_name
        self.values_list = values_list
        self.case_sensitive = case_sensitive
        self.highlight_columns = [column_name]

        self.check_description = f"Value in column {column_name} not admitted"


    def _create_filter(self):

        if self.case_sensitive:
            values_list = [str(v) for v in self.values_list]
            list_values_sql = "('" + "','".join(values_list) + "')"
            return f"cast({self.column_name} as STRING) not in {list_values_sql}"
        else:
            values_list = [str(v).lower() for v in self.values_list]
            list_values_sql = "('" + "','".join(values_list) + "')"
            return f"lower(cast({self.column_name} as STRING)) not in {list_values_sql}"

    def _get_number_ko_sql(self) -> int:
        self.add_ignore_filter(_create_filter_columns_not_null(self.column_name))
        negative_filter = self._create_filter()
        return self.standard_get_number_ko_sql(negative_filter)

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        self.add_ignore_filter(_create_filter_columns_not_null(self.column_name))
        negative_filter = self._create_filter()
        return self.standard_rows_ko_sql(negative_filter)

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].notnull() & (df[self.column_name].astype(str) != "")]
        if self.case_sensitive:
            values_list = [str(v) for v in self.values_list]
            df = df[~df[self.column_name].astype(str).isin(values_list)]
        else:
            values_list = [str(v).lower() for v in self.values_list]
            df = df[~df[self.column_name].astype(str).str.lower().isin(values_list)]
        return df




