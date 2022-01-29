from typing import Union

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _create_filter_columns_not_null, _aggregate_sql_filter, _output_column_to_sql, \
    _query_limit, _clean_string_float_inf_columns_df


class DatesOrderDimensionTable(Check):

    def __init__(self,
                 table,
                 foreign_keys: Union[str, list],
                 dimension_table,
                 left_column: str,
                 right_column: str,
                 operator: str = "<=",
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

        self.left_column = left_column

        self.right_column = right_column

        self.operator = self._negate_operator(operator)

        self.dimension_table = dimension_table

        self.check_description = f"Dates {left_column}, {right_column} are not in the correct order"

    @staticmethod
    def _negate_operator(operator):
        if operator == ">":
            result = "<="
        elif operator == ">=":
            result = "<"
        elif operator == "<":
            result = ">="
        elif operator == "<=":
            result = ">"
        elif operator == "=":
            result = "!="
        elif operator == "!=":
            result = "="
        else:
            raise Exception("Operator not recognize. Possible values are: >,>=,<,>=,=,!=")
        return result

    def _create_negative_filter(self):
        cast_datetime_left_column = self.table.source.cast_datetime_sql("left_table." + self.left_column,
                                                                        self.table.datetime_columns[self.left_column])
        cast_datetime_right_column = self.dimension_table.source.cast_datetime_sql("right_table." + self.right_column,
                                                                        self.dimension_table.datetime_columns[self.right_column])
        sql_filter = f"coalesce({cast_datetime_left_column} {self.operator} {cast_datetime_right_column}, false)"
        return sql_filter

    def _get_number_ko_sql_dimension_table_sql(self):
        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          _create_filter_columns_not_null(self.left_column),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"cast(left_table.{self.foreign_keys[i]} as string) = cast(right_table.{self.primary_keys[i]} as string)" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        negative_filter = self._create_negative_filter()

        query = f"""
                SELECT 
                    CASE WHEN {negative_filter} THEN "KO" ELSE "OK" END as check,
                    count(*) as n_rows
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) left_table
                left join {self.dimension_table.db_name} right_table
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
        #TODO completare
        return 0

    def _get_number_ko_sql(self) -> int:
        if self.dimension_table.flag_dataframe:
            return self._get_number_ko_sql_dimension_table_dataframe()
        else:
            return self._get_number_ko_sql_dimension_table_sql()

    def _get_rows_ko_sql_dimension_table_sql(self) -> pd.DataFrame:
        ignore_filters = [_create_filter_columns_not_null(self.foreign_keys),
                          _create_filter_columns_not_null(self.columns_not_null),
                          _create_filter_columns_not_null(self.left_column),
                          self.ignore_filters,
                          self.table.table_filter]
        ignore_filters = _aggregate_sql_filter(ignore_filters)

        join_keys = [f"cast(left_table.{self.foreign_keys[i]} as string) = cast(right_table.{self.primary_keys[i]} as string)" for i in range(len(self.foreign_keys))]
        join_keys = " AND ".join(join_keys)

        output_columns = _output_column_to_sql(self.table.output_columns, table_tag="left_table")
        output_columns += f", right_table.{self.right_column}"

        negative_filter = self._create_negative_filter()

        sql_limit = _query_limit(self.n_max_rows_output)
        query = f"""
                SELECT 
                    {output_columns}
                from 
                    (SELECT * FROM {self.table.db_name} {ignore_filters}) left_table
                left join {self.dimension_table.db_name} right_table
                    on {join_keys}
                WHERE {negative_filter}
                {sql_limit}
                """
        df = self.table.source.run_query(query)
        if self.right_column in df.columns:
            right_column_output_name = f"{self.right_column}_2"
        else:
            right_column_output_name = self.right_column
        df.rename(columns={f"right_table.{self.right_column}": right_column_output_name}, inplace=True)
        # self.output_columns.append(right_column_output_name)
        return df

    def _get_rows_ko_sql_dimension_table_dataframe(self):
        # TODO Completare
        return None

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_sql_dimension_table_dataframe()
        else:
            return self._get_rows_ko_sql_dimension_table_sql()

    def _get_rows_ko_dataframe_dimension_table_sql(self) -> pd.DataFrame:
        # TODO Completare
        return None

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
            dimension_table[tag_key] += _clean_string_float_inf_columns_df(dimension_table[primary_keys_col])
        dimension_table = dimension_table[[tag_key, self.right_column]]
        dimension_table.columns = [tag_key, "right_table_" + self.right_column]
        df = df.merge(dimension_table, on=tag_key, how="inner")
        df[self.left_column] = pd.to_datetime(df[self.left_column], errors="coerce")
        df = df[df[self.left_column].notnull() & (df[self.left_column].astype(str) != "")]
        df["right_table_" + self.right_column] = pd.to_datetime(df["right_table_" + self.right_column], errors="coerce")
        df = df[df["right_table_" + self.right_column].notnull() & (df["right_table_" + self.right_column].astype(str) != "")]
        pandas_operator = self.operator if self.operator != "=" else "=="
        df = df.query(f"{self.left_column} {pandas_operator} {'right_table_' + self.right_column}")
        df.drop([tag_key], axis=1, inplace=True)
        if self.right_column in df.columns:
            right_column_output_name = f"{self.right_column}_2"
        else:
            right_column_output_name = self.right_column
        df.rename(columns={f"right_table_{self.right_column}": right_column_output_name}, inplace=True)
        return df

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        if self.dimension_table.flag_dataframe:
            return self._get_rows_ko_dataframe_dimension_table_dataframe()
        else:
            return self._get_rows_ko_dataframe_dimension_table_sql()




