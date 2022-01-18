from typing import Union, List

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _aggregate_sql_filter, _output_column_to_sql, _query_limit, \
    _create_filter_columns_not_null


class Custom(Check):

    def __init__(self,
                 table,
                 negative_filter: str,
                 check_description: str,
                 columns_not_null: Union[str, list] = None,
                 ignore_filters: Union[List[str], str] = None,
                 ):
        self.table = table
        self.check_description = check_description
        self.negative_filter = negative_filter
        self.ignore_filters = ignore_filters
        self.columns_not_null = columns_not_null

    def _get_number_ko_sql(self) -> int:
        ignore_filters = [self.ignore_filters]
        ignore_filters.append(_create_filter_columns_not_null(self.columns_not_null))
        ignore_filters.append(self.table.table_filter)
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        query = f"""
        SELECT 
            CASE WHEN {self.negative_filter} THEN "KO" ELSE "OK" END as check,
            count(*) as n_rows
        from {self.table.db_name}
        {ignore_filters}
        group by check
        """
        df = self.table.source.run_query(query)
        n_ok = df.loc[df["check"] == "OK", "n_rows"].values
        if len(n_ok) > 0:
            n_ok = n_ok[0]
        else:
            n_ok = 0
        n_ko = df.loc[df["check"] == "KO", "n_rows"].values
        if len(n_ko) > 0:
            n_ko = n_ko[0]
        else:
            n_ko = 0
        return n_ko

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
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
        # TODO rivedere query trovare una funzione che prenda codice sql
        sql_filter = [self.ignore_filters]
        sql_filter.append(self.table.table_filter)
        sql_filter.append(self.negative_filter)
        sql_filter = _aggregate_sql_filter(sql_filter)
        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.table.max_rows)
        query = f"""
        SELECT 
            {output_columns}
        from {self.table.db_name}
        {sql_filter}
        {sql_limit}
        """
        df = self.table.source.run_query(query)
        return df



