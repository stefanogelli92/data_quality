from typing import Union

import pandas as pd
import numpy as np

from data_quality.src.check import Check
from data_quality.src.utils import _aggregate_sql_filter, _output_column_to_sql, _query_limit, \
    _create_filter_columns_not_null, _clean_string_float_inf_columns_df

TAG_FORMATTED = "_custom_formatted"


class PeriodIntersection(Check):

    def __init__(self,
                 table,
                 start_date: str,
                 end_date: str,
                 id_columns: Union[str, list, None] = None,
                 extremes_exclude: bool = False
                 ):
        super().__init__(table,
                         f"Rows intersection on period from {start_date} to {end_date}",
                         [start_date, end_date])
        self.id_columns = id_columns
        self.start_date = start_date
        self.end_date = end_date
        self.extremes_exclude = extremes_exclude
        self.operator = ">=" if extremes_exclude else ">"

    def _sql_check_previus_query(self):
        if self.id_columns is not None:
            query = f"""
            coalesce(case when lag(sql_id_columns) over (order by sql_id_columns, {self.start_date}{TAG_FORMATTED}) = sql_id_columns
                THEN lag({self.end_date}{TAG_FORMATTED}) over (order by sql_id_columns, {self.start_date}{TAG_FORMATTED})
                else null end {self.operator} {self.start_date}{TAG_FORMATTED}, false)
            """
        else:
            query = f"""
            coalesce(lag({self.end_date}{TAG_FORMATTED}) over (order by {self.start_date}{TAG_FORMATTED})
                {self.operator} {self.start_date}{TAG_FORMATTED}, false)
            """
        return query

    def _sql_check_next_query(self):
        if self.id_columns is not None:
            query = f"""check OR (LEAD(check) over (order by sql_id_columns, {self.start_date}{TAG_FORMATTED}))"""
        else:
            query = f"""check OR (LEAD(check) over (order by {self.start_date}{TAG_FORMATTED}))"""
        return query

    def _get_number_ko_sql(self) -> int:
        ignore_filters = [_create_filter_columns_not_null(self.columns_not_null),
                          _create_filter_columns_not_null([self.start_date, self.end_date]),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        cast_starting_date = self.table.source.cast_datetime_sql(self.start_date,
                                                                 self.table.datetime_columns[self.start_date])
        cast_ending_date = self.table.source.cast_datetime_sql(self.end_date,
                                                               self.table.datetime_columns[self.end_date])
        if self.id_columns is None:
            sql_id_columns = ""
        elif isinstance(self.id_columns, str):
            sql_id_columns = f"{self.id_columns} as sql_id_columns,"
        else:
            sql_id_columns = [f"cast({col} as string)" for col in self.id_columns]
            sql_id_columns = "CONCAT(" + ", '-', ".join(sql_id_columns) + ") as sql_id_columns,"

        query = f"""
                SELECT 
                    CASE WHEN double_check THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from (
                    SELECT
                        *,
                        {self._sql_check_next_query()}  as double_check,
                    from (
                        SELECT 
                            {"sql_id_columns," if self.id_columns is not None else ""}
                            {self.start_date}{TAG_FORMATTED},
                            {self.end_date}{TAG_FORMATTED},
                            {self._sql_check_previus_query()} as check
                        from (
                            SELECT
                                {sql_id_columns}
                                {cast_starting_date} as {self.start_date}{TAG_FORMATTED},
                                {cast_ending_date} as {self.end_date}{TAG_FORMATTED}
                            from {self.table.db_name}
                            {ignore_filters}
                            ) as cast_table
                    ) as check_table_data_quality
                ) as check_table_data_quality
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

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        ignore_filters = [_create_filter_columns_not_null(self.columns_not_null),
                          _create_filter_columns_not_null([self.start_date, self.end_date]),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.n_max_rows_output)
        cast_starting_date = self.table.source.cast_datetime_sql(self.start_date,
                                                                 self.table.datetime_columns[self.start_date])
        cast_ending_date = self.table.source.cast_datetime_sql(self.end_date,
                                                               self.table.datetime_columns[self.end_date])
        if self.id_columns is None:
            sql_id_columns = ""
        elif isinstance(self.id_columns, str):
            sql_id_columns = f"{self.id_columns} as sql_id_columns,"
        else:
            sql_id_columns = [f"cast({col} as string)" for col in self.id_columns]
            sql_id_columns = "CONCAT(" + ", '-', ".join(sql_id_columns) + ") as sql_id_columns,"
        query = f"""
        SELECT
            {output_columns}
        from (
            SELECT 
                 *,
                 {self._sql_check_next_query()}  as double_check
            FROM (   
                SELECT 
                    *,
                    {self._sql_check_previus_query()} as check
                from (
                    SELECT
                        *,
                        {sql_id_columns}
                        {cast_starting_date} as {self.start_date}{TAG_FORMATTED},
                        {cast_ending_date} as {self.end_date}{TAG_FORMATTED}
                    from {self.table.db_name}
                    {ignore_filters}
                    ) as cast_table
            ) as check_table_data_quality
        ) as check_table_data_quality
        WHERE double_check
        {sql_limit}
        """
        df = self.table.source.run_query(query)
        drop_column = ["double_check", "check", "sql_id_columns", f"{self.start_date}{TAG_FORMATTED}", f"{self.end_date}{TAG_FORMATTED}"]
        for col in drop_column:
            if col in df.columns:
                df.drop([col], axis=1, inplace=True)
        return df

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df[self.start_date] = pd.to_datetime(df[self.start_date], errors="coerce")
        df[self.end_date] = pd.to_datetime(df[self.end_date], errors="coerce")
        df = df[df[self.start_date].notnull() & df[self.end_date].notnull()]
        if self.id_columns is not None:
            if isinstance(self.id_columns, str):
                id_columns = [self.id_columns]
            else:
                id_columns = self.id_columns
            index_column = "id_column_data_quality"
            df[index_column] = ""
            for col in id_columns:
                df[index_column] += _clean_string_float_inf_columns_df(df[col])

            df = df.sort_values([index_column, self.start_date])
            prev_column = "previous_ending_data_quality"
            next_column = "next_starting_data_quality"
            df[prev_column] = np.where(df[index_column].shift(1) == df[index_column],
                                       df[self.end_date].shift(1),
                                       None)
            df[next_column] = np.where(df[index_column].shift(-1) == df[index_column],
                                       df[self.start_date].shift(-1),
                                       None)
        else:
            df = df.sort_values([self.start_date])
            prev_column = "previous_ending_data_quality"
            next_column = "next_starting_data_quality"
            df[prev_column] = df[self.end_date].shift(1)
            df[next_column] = df[self.start_date].shift(-1)

        df[prev_column] = pd.to_datetime(df[prev_column], errors="coerce")
        df[next_column] = pd.to_datetime(df[next_column], errors="coerce")

        if self.extremes_exclude:
            df = df[(df[prev_column] >= df[self.start_date]) | (df[self.end_date] >= df[next_column])]
        else:
            df = df[(df[prev_column] > df[self.start_date]) | (df[self.end_date] > df[next_column])]
        df.drop([prev_column, next_column], axis=1, inplace=True)
        if self.id_columns is not None:
            df.drop([index_column], axis=1, inplace=True)
        return df




