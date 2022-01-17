from typing import Union, Optional
from datetime import date, datetime

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
from data_quality.src.utils import _create_filter_columns_not_null, _aggregate_sql_filter, _output_column_to_sql, \
    _query_limit


class MatchDImensionTable(Check):

    def __init__(self,
                 table,
                 foreign_keys: Union[str, list],
                 dimension_table,
                 primary_keys: Union[str, list] = None
                 ):
        self.table = table
        if isinstance(foreign_keys, str):
            self.foreign_keys = [foreign_keys]
        else:
            self.foreign_keys = foreign_keys
        if primary_keys is None:
            self.primary_keys = [dimension_table.index_column]
        elif isinstance(primary_keys, str):
            self.primary_keys = [primary_keys]
        else:
            self.primary_keys = primary_keys

        self.dimension_table = dimension_table

        self.check_description = f"Unable to find a match with table {dimension_table.output_name}"

    def _get_number_ko_sql_dimension_table_sql(self):
        ignore_filters = _create_filter_columns_not_null(self.foreign_keys)
        ignore_filters.append(self.table.table_filter)
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"f.{self.foreign_keys[i]} = d.{self.primary_keys[i]}" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        query = f"""
                SELECT 
                    CASE WHEN d.{self.primary_keys[0]} is null THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) f
                left join {self.dimension_table.name} d
                    on {join_keys}
                GROUP BY check   
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

    def _get_number_ko_sql_dimension_table_dataframe(self):
        ignore_filters = _create_filter_columns_not_null(self.foreign_keys)
        ignore_filters.append(self.table.table_filter)
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        if len(self.foreign_keys) == 1:
            negative_condition = "(" + self.foreign_keys[0]
        else:
            negative_condition = [f"cast({self.foreign_keys[i]} as string)" for i in range(len(self.foreign_keys))]
            negative_condition = "(CONCAT(" + ", '-', ".join(negative_condition) + ")"

        negative_condition += " not in ('"
        columns_keys = ""
        for col in self.primary_keys:
            columns_keys += self.dimension_table.df[col].astype(str)
        columns_keys = list(columns_keys.unique())
        columns_keys = "','".join(columns_keys)
        negative_condition += columns_keys + "'))"

        query = f"""
                SELECT 
                    CASE WHEN {negative_condition} THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from  {self.table.db_name} 
                {ignore_filters}
                GROUP BY check   
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

    def _get_number_ko_sql(self) -> int:
        if self.dimension_table.flag_dataframe:
            return self._get_number_ko_sql_dimension_table_dataframe()
        else:
            return self._get_number_ko_sql_dimension_table_sql()

    def _get_rows_ko_sql_dimension_table_sql(self) -> pd.DataFrame:
        ignore_filters = _create_filter_columns_not_null(self.foreign_keys)
        ignore_filters.append(self.table.table_filter)
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"f.{self.foreign_keys[i]} = d.{self.primary_keys[i]}" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.table.max_rows)
        query = f"""
                SELECT 
                    {output_columns}
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) f
                left join {self.dimension_table.name} d
                    on {join_keys}
                WHERE d.{self.primary_keys[0]} is null 
                {sql_limit}
                """
        df = self.table.run_query(query)
        return df

    def _get_rows_ko_sql_dimension_table_dataframe(self):

        if len(self.foreign_keys) == 1:
            negative_condition = "(" + self.foreign_keys[0]
        else:
            negative_condition = [f"cast({self.foreign_keys[i]} as string)" for i in range(len(self.foreign_keys))]
            negative_condition = "(CONCAT(" + ", '-', ".join(negative_condition) + ")"

        negative_condition += " not in ('"
        columns_keys = ""
        for col in self.primary_keys:
            columns_keys += self.dimension_table.df[col].astype(str)
        columns_keys = list(columns_keys.unique())
        columns_keys = "','".join(columns_keys)
        negative_condition += columns_keys + "'))"

        ignore_filters = _create_filter_columns_not_null(self.foreign_keys)
        ignore_filters.append(self.table.table_filter)
        ignore_filters.append(negative_condition)
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.table.max_rows)
        query = f"""
                SELECT 
                    {output_columns}
                from {self.table.db_name} {ignore_filters}
                {ignore_filters}
                {sql_limit}
                """
        df = self.table.run_query(query)
        return df

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_sql_dimension_table_dataframe()
        else:
            return self._get_rows_ko_sql_dimension_table_sql()

    def _get_rows_ko_dataframe_dimension_table_sql(self) -> pd.DataFrame:
        df = self.table.df
        for col in self.primary_keys:
            df = df[df[col].notnull() & (df[col].astype(str) != "")]

    def _get_rows_ko_dataframe_dimension_table_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        for col in self.primary_keys:
            df = df[df[col].notnull() & (df[col].astype(str) != "")]

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_dataframe_dimension_table_dataframe()
        else:
            return self._get_rows_ko_dataframe_dimension_table_sql()




