from abc import ABC, abstractmethod
from typing import Union, List

import pandas as pd

from data_quality.src.utils import _create_filter_columns_not_null, _aggregate_sql_filter, _output_column_to_sql, \
    _query_limit

TAG_CHECK_DESCRIPTION = "check_description"
TAG_WARNING_DESCRIPTION = "warning_description"


class Check(ABC):

    def __init__(self, table, check_description: str):
        self.table = table
        self.check_description = check_description
        self.n_max_rows_output = None
        self.ignore_filters = []
        self.columns_not_null = None
        self.output_columns = None

        self.flag_ko = None
        self.n_ko = None
        self.flag_over_max_rows = None
        self.ko_rows = None
        self.flag_warning = False

    @abstractmethod
    def _get_number_ko_sql(self) -> int:
        pass

    @abstractmethod
    def _get_rows_ko_sql(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        pass

    def initialize_params(self,
                          check_description: Union[str, None] = None,
                          flag_warning: bool = False,
                          n_max_rows_output: Union[int, None] = None,
                          ignore_filter: Union[str, None] = None,
                          columns_not_null: Union[int, None] = None,
                          output_columns: Union[List[str], str, None] = None
                          ):
        # TODO add long description
        if check_description is not None:
            self.check_description = check_description
        self.flag_warning = flag_warning
        self.n_max_rows_output = n_max_rows_output
        self.ignore_filters = []
        self.add_ignore_filter(ignore_filter)
        self.columns_not_null = columns_not_null
        self.output_columns = output_columns

    def add_ignore_filter(self, sql_filter):
        if self.ignore_filters is None:
            self.ignore_filters = []
        elif isinstance(self.ignore_filters, str):
            self.ignore_filters = [self.ignore_filters]
        if sql_filter is None:
            pass
        elif isinstance(sql_filter, str):
            self.ignore_filters.append(sql_filter)
        elif isinstance(sql_filter, list):
            for f in sql_filter:
                self.ignore_filters.append(f)

    def standard_get_number_ko_sql(self, negative_filter: str):
        ignore_filters = self.ignore_filters
        ignore_filters.append(_create_filter_columns_not_null(self.columns_not_null))
        ignore_filters.append(self.table.table_filter)
        ignore_filters = _aggregate_sql_filter(ignore_filters)
        query = f"""
                SELECT 
                    CASE WHEN {negative_filter} THEN "KO" ELSE "OK" END as check,
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

    def standard_rows_ko_sql(self, negative_filter: str) -> pd.DataFrame:
        sql_filter = self.ignore_filters
        sql_filter.append(_create_filter_columns_not_null(self.columns_not_null))
        sql_filter.append(self.table.table_filter)
        sql_filter.append(negative_filter)
        sql_filter = _aggregate_sql_filter(sql_filter)
        output_columns = _output_column_to_sql(self.output_columns)
        sql_limit = _query_limit(self.n_max_rows_output)
        query = f"""
        SELECT 
            {output_columns}
        from {self.table.db_name}
        {sql_filter}
        {sql_limit}
        """
        df = self.table.source.run_query(query)
        return df

    def check(self, get_rows_flag: bool = False):
        flag_over_max_rows = None
        if self.table.flag_dataframe:
            df_ko = self._get_rows_ko_dataframe()
            n_ko = df_ko.shape[0]
            if self.output_columns is not None:
                if n_ko > 0:
                    df_ko = df_ko[self.output_columns]
                else:
                    df_ko = pd.DataFrame(columns=self.output_columns)
            df_ko[TAG_CHECK_DESCRIPTION] = self.check_description
            flag_over_max_rows = False
        else:
            n_ko = self._get_number_ko_sql()
            if get_rows_flag:
                if n_ko == 0:
                    df_ko = pd.DataFrame(columns=self.output_columns)
                    flag_over_max_rows = False
                else:
                    df_ko = self._get_rows_ko_sql()
                    n_rows = df_ko.shape[0]
                    if n_rows == self.n_max_rows_output:
                        flag_over_max_rows = True
                    else:
                        flag_over_max_rows = False
                df_ko[TAG_CHECK_DESCRIPTION] = self.check_description
            else:
                df_ko = None

        self.n_ko = n_ko
        self.flag_ko = n_ko != 0
        self.ko_rows = df_ko
        self.flag_over_max_rows = flag_over_max_rows
        self.table.check_list.append(self)
        return n_ko
