from typing import Union, Optional
from datetime import date, datetime

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _create_filter_columns_not_null, _aggregate_sql_filter, _output_column_to_sql, \
    _query_limit, _clean_string_float_inf_columns_df


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

        self.highlight_columns = self.foreign_keys

        self.check_description = f"Unable to find a match with table {dimension_table.output_name}"

    def _get_number_ko_sql_dimension_table_sql(self):
        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"cast(f.{self.foreign_keys[i]} as string) = cast(d.{self.primary_keys[i]} as string)" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        query = f"""
                SELECT 
                    CASE WHEN d.{self.primary_keys[0]} is null THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) f
                left join {self.dimension_table.db_name} d
                    on {join_keys}
                GROUP BY check   
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

    def _get_number_ko_sql_dimension_table_dataframe(self):
        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        if len(self.foreign_keys) == 1:
            negative_condition = f"(cast({self.foreign_keys[0]} as string)"
        else:
            negative_condition = [f"cast({self.foreign_keys[i]} as string)" for i in range(len(self.foreign_keys))]
            negative_condition = "(CONCAT(" + ", '-', ".join(negative_condition) + ")"

        negative_condition += " not in ('"
        columns_keys = ""
        for col in self.primary_keys:
            columns_keys += _clean_string_float_inf_columns_df(self.dimension_table.df[col])
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

    def _get_number_ko_sql(self) -> int:
        if self.dimension_table.flag_dataframe:
            return self._get_number_ko_sql_dimension_table_dataframe()
        else:
            return self._get_number_ko_sql_dimension_table_sql()

    def _get_rows_ko_sql_dimension_table_sql(self) -> pd.DataFrame:
        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"cast(f.{self.foreign_keys[i]} as string) = cast(d.{self.primary_keys[i]} as string)" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        output_columns = _output_column_to_sql(self.table.output_columns, table_tag="f")
        sql_limit = _query_limit(self.n_max_rows_output)
        query = f"""
                SELECT 
                    {output_columns}
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) f
                left join {self.dimension_table.db_name} d
                    on {join_keys}
                WHERE d.{self.primary_keys[0]} is null 
                {sql_limit}
                """
        df = self.table.source.run_query(query)
        return df

    def _get_rows_ko_sql_dimension_table_dataframe(self):

        if len(self.foreign_keys) == 1:
            negative_condition = f"(cast({self.foreign_keys[0]} as string)"
        else:
            negative_condition = [f"cast({self.foreign_keys[i]} as string)" for i in range(len(self.foreign_keys))]
            negative_condition = "(CONCAT(" + ", '-', ".join(negative_condition) + ")"

        negative_condition += " not in ('"
        columns_keys = ""
        for col in self.primary_keys:
            columns_keys += _clean_string_float_inf_columns_df(self.dimension_table.df[col])
        columns_keys = list(columns_keys.unique())
        columns_keys = "','".join(columns_keys)
        negative_condition += columns_keys + "'))"

        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          self.ignore_filters,
                          negative_condition,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        output_columns = _output_column_to_sql(self.table.output_columns)
        sql_limit = _query_limit(self.n_max_rows_output)
        query = f"""
                SELECT 
                    {output_columns}
                from {self.table.db_name}
                {ignore_filters}
                {sql_limit}
                """
        df = self.table.source.run_query(query)
        return df

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_sql_dimension_table_dataframe()
        else:
            return self._get_rows_ko_sql_dimension_table_sql()

    def _get_rows_ko_dataframe_dimension_table_sql(self) -> pd.DataFrame:
        df = self.table.df
        if df.shape[0] > 0:
            for col in self.foreign_keys:
                df = df[df[col].notnull() & (df[col].astype(str) != "")]
            tag_key = "unique_concatenate_key_data_quality"
            df[tag_key] = ""
            for col in self.foreign_keys:
                df[tag_key] += _clean_string_float_inf_columns_df(df[col])

            values_list = list(df[tag_key].unique())
            values_list = [f"(SELECT '{v}' as key_columns)" for v in values_list]
            values_list = " union all ".join(values_list)

            if len(self.primary_keys) == 1:
                concat_columns = f"cast(d.{self.primary_keys[0]} as string)"
            else:
                concat_columns = [f"cast(d.{self.primary_keys[i]} as string)" for i in range(len(self.primary_keys))]
                concat_columns = "(CONCAT(" + ", ".join(concat_columns) + ")"
            query = f"""
            SELECT 
                f.key_columns
            FROM (
                {values_list} 
            ) f
            left join {self.dimension_table.db_name} d
            on {concat_columns} = f.key_columns
            WHERE d.{self.primary_keys[0]} is null
            """
            match_df = self.dimension_table.source.run_query(query)
            df = df[df[tag_key].isin(match_df["key_columns"])]
            df.drop([tag_key], axis=1, inplace=True)
        return df

    def _get_rows_ko_dataframe_dimension_table_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        for col in self.foreign_keys:
            df = df[df[col].notnull() & (df[col].astype(str) != "")]

        tag_key = "unique_concatenate_key_data_quality"
        df[tag_key] = ""

        dimension_table = self.dimension_table.df.copy()
        dimension_table[tag_key] = ""
        for i in range(len(self.foreign_keys)):
            foreign_keys_col = self.foreign_keys[i]
            primary_keys_col = self.primary_keys[i]
            df[tag_key] += _clean_string_float_inf_columns_df(df[foreign_keys_col])
            dimension_table[tag_key] += _clean_string_float_inf_columns_df(dimension_table[primary_keys_col]) #dimension_table[primary_keys_col].astype(float, errors='ignore').astype(str)

        df = df[~df[tag_key].isin(dimension_table[tag_key].unique())]
        df.drop([tag_key], axis=1, inplace=True)
        return df

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_dataframe_dimension_table_dataframe()
        else:
            return self._get_rows_ko_dataframe_dimension_table_sql()




