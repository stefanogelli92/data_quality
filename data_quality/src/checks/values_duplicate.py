import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _aggregate_sql_filter, _output_column_to_sql, _query_limit, \
    _create_filter_columns_not_null


class ValuesDuplicate(Check):

    def __init__(self,
                 table,
                 column_name):
        self.table = table
        self.check_description = "Duplicated index"
        self.column = column_name

    def _get_number_ko_sql(self) -> int:
        ignore_filters = [_create_filter_columns_not_null(self.column),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        query = f"""
                SELECT 
                    count(*) as n_rows,
                    count(DISTINCT {self.column}) as n_distinct_index   
                from {self.table.db_name}
                {ignore_filters}
                """
        df = self.table.source.run_query(query)
        n_not_null_index = df["n_rows"].values[0]
        n_distinct_index = df["n_distinct_index"].values[0]
        n_ok = n_distinct_index
        n_ko = n_not_null_index - n_distinct_index
        if n_ko > 0:
            self.table.index_problem = True
        return n_ko

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        ignore_filters = [_create_filter_columns_not_null(self.column),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.n_max_rows_output)
        query = f"""
            SELECT
                *
            FROM (
                SELECT 
                    {output_columns},
                    count(*) OVER (PARTITION BY cast({self.column} as string)) as n_distinct_index
                from {self.table.db_name}
                {ignore_filters}
            ) as cast_table 
            WHERE a.n_distinct_index > 1
            {sql_limit}
        """
        df = self.table.source.run_query(query)
        df.drop(["n_distinct_index"], axis=1, inplace=True)
        if df.shape[0] > 0:
            self.table.index_problem = True
        return df

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column].notnull() & (df[self.column].astype(str) != "")]
        tag_count_index = "n_distinct_index"
        df[tag_count_index] = df.groupby(self.column)[self.column].transform("count")
        df = df[df[tag_count_index] > 1]
        df.drop([tag_count_index], axis=1, inplace=True)
        if df.shape[0] > 0:
            self.table.index_problem = True
        return df




