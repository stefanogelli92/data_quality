from typing import Union, Optional
from datetime import date, datetime

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _aggregate_sql_filter, _output_column_to_sql, _query_limit

# TODO prevedere un troncamento della data per arrotondare
# TODO formatting datetime custom

class DatesOrder(Check):

    def __init__(self,
                 table,
                 ascending_columns: list,
                 strictly_ascending: bool = False
                 ):
        self.table = table
        self.ascending_columns = ascending_columns
        self.strictly_ascending = strictly_ascending

        self.check_description = "Dates {} are not in the correct order".format(", ".join(ascending_columns))

    def _create_negative_filter(self):
        filter = "((1=0)"
        for i in range(1, len(self.ascending_columns)):
            for j in range(i):
                if self.strictly_ascending:
                    filter += f" OR coalesce(({self.ascending_columns[j]} >= {self.ascending_columns[i]}), false)"
                else:
                    filter += f" OR coalesce(({self.ascending_columns[j]} > {self.ascending_columns[i]}), false)"
        filter += ")"
        return filter

    def _cast_datetime_sql(self):
        sql_query = ""
        for col in self.ascending_columns:
            sql_query += f"CAST({col} as timestamp) as {col},"

        sql_query = sql_query[:-1]
        return sql_query

    def _get_number_ko_sql(self) -> int:
        ignore_filters = self.table.table_filter
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        sql_filter = self._create_negative_filter()
        sql_cast_datetime = self._cast_datetime_sql()
        query = f"""
                SELECT 
                    CASE WHEN {sql_filter} THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from (
                    SELECT
                        {sql_cast_datetime}
                    from {self.table.db_name}
                    {ignore_filters}
                    ) a
                group by check
                """
        df = self.table.run_query(query)
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

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        ignore_filters = self.table.table_filter
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        negative_filter = self._create_negative_filter()
        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.table.max_rows)
        sql_cast_datetime = self._cast_datetime_sql()
        query = f"""
        SELECT 
            {output_columns}
        from (
            SELECT
                {sql_cast_datetime}
            from {self.table.db_name}
            {ignore_filters}
            ) a
        WHERE 
        {negative_filter}
        {sql_limit}
        """
        df = self.table.run_query(query)
        return df

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        tag_check = "current_check_data_quality"
        df[tag_check] = False
        for col in self.ascending_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        for i in range(1, len(self.ascending_columns)):
            for j in range(i):
                if self.strictly_ascending:
                    df[tag_check] = df[tag_check] | (df[self.ascending_columns[j]] >= df[self.ascending_columns[i]])
                else:
                    df[tag_check] = df[tag_check] | (df[self.ascending_columns[j]] > df[self.ascending_columns[i]])
        df = df[df[tag_check]]
        df.drop(tag_check, axis=1, inplace=True)
        return df




