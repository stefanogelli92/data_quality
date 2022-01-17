from typing import Union, Dict, List, Callable, Optional
from valdec.decorators import validate
from datetime import date, datetime

import pandas as pd

from data_quality.src.utils import _clean_sql_filter, _aggregate_sql_filter, _output_column_to_sql, _query_limit
from data_quality.src.checks.index_null import IndexNull
from data_quality.src.checks.index_duplicate import IndexDuplicate
from data_quality.src.checks.not_empthy_column import NotEmpthyColumn
from data_quality.src.checks.datetime_format import DatetimeFormat
from data_quality.src.checks.column_between_values import ColumnBetweenValues
from data_quality.src.checks.column_between_dates import ColumnBetweenDates
from data_quality.src.checks.dates_order import DatesOrder
from data_quality.src.checks.values_order import ValuesOrder
from data_quality.src.checks.values_in_list import ValuesInList
from data_quality.src.checks.match_regex import MatchRegex
from data_quality.src.checks.custom import Custom


class Table:
    def __init__(self,
                 db_name: str = None,
                 run_query: Callable[[str], pd.DataFrame] = None,
                 df: pd.DataFrame = None,
                 index_column: str = None,
                 output_name: str = None,
                 not_empthy_columns: Union[str, list] = None,
                 datetime_columns: Union[str, list] = None,
                 table_filter: str = None,
                 output_columns: Union[List[str], str] = None,
                 n_max_rows_output: int = None
                 ):
        # Input parameters
        if df is not None:
            self.flag_dataframe = True
            self.df = df
            self.get_number_of_rows()
        if db_name is not None:
            self.flag_dataframe = False
            self.db_name = db_name
            self.run_query = run_query
        self.output_name = output_name
        self.index_column = index_column
        self.not_empthy_columns = None
        self.set_not_empthy_columns(not_empthy_columns)
        self.datetime_columns = None
        self.set_datetime_columns(datetime_columns)
        self.table_filter = None
        self.set_table_filer(table_filter)
        self.output_columns = None
        self.set_output_columns(output_columns)
        self.max_rows = n_max_rows_output
        # Result parameters
        self.n_rows = None
        self.index_problem = False
        self.passed_all_checks = True
        self.check_list = []

    @validate
    def set_table_filer(self, sql_filter: Optional[str]):
        self.table_filter = _clean_sql_filter(sql_filter)
        if self.flag_dataframe and (self.table_filter is not None):
            self.df = self.df.query(self.table_filter)

    @validate
    def set_output_name(self, name: Optional[str]):
        self.output_name = name

    @validate
    def set_index_column(self, column_name: Optional[str]):
        self.index_column = column_name
        self.set_output_columns(self.output_columns)

    @validate
    def set_not_empthy_columns(self, columns_list: Union[List[str], str, None]):
        if isinstance(columns_list, str):
            columns_list = [columns_list]
        self.not_empthy_columns = columns_list

    @validate
    def set_datetime_columns(self, columns_list: Union[List[str], str, None]):
        if isinstance(columns_list, str):
            columns_list = [columns_list]
        self.datetime_columns = columns_list

    @validate
    def set_output_columns(self, columns_list: Union[List[str], str, None]):
        if isinstance(columns_list, str):
            columns_list = [columns_list]
        if (self.index_column is not None) and (columns_list is not None):
            columns_list.insert(0, self.index_column)
            columns_list = list(dict.fromkeys(columns_list))
        self.output_columns = columns_list

    def download_table(self,
                       columns_list: Union[List[str], str, None],
                       n_max_rows_output: Union[int, None] = None):
        if self.db_name is not None:
            table_filter = _aggregate_sql_filter(self.table_filter)
            output_columns_sql = _output_column_to_sql(columns_list)
            sql_limit = _query_limit(n_max_rows_output)
            query = f"""
            SELECT 
                {output_columns_sql}
            from {self.db_name}
            {table_filter}
            {sql_limit}
            """
            df = self.run_query(query)
            self.flag_dataframe = True
            self.df = df

    @validate
    def get_number_of_rows(self) -> int:
        if self.flag_dataframe:
            result = self.df.shape[0]
        else:
            filter_sql = _aggregate_sql_filter(self.table_filter)

            query = f"""
            SELECT 
                count(*) as n_rows
            from {self.db_name}
            {filter_sql}
            """
            result = self.run_query(query)["n_rows"].values[0]
        self.n_rows = result
        return result

    # Check methods

    @validate
    def check_index_not_null(self, get_rows_flag: bool = False) -> Optional[int]:

        if self.index_column is not None:
            check = IndexNull(self)
            n_ko = check.check(get_rows_flag=get_rows_flag)
        else:
            # TODO Log unable to check index insert index before
            n_ko = None

        return n_ko

    @validate
    def check_duplicates_index(self, get_rows_flag: bool = False) -> Optional[int]:

        if self.index_column is not None:
            check = IndexDuplicate(self)
            n_ko = check.check(get_rows_flag=get_rows_flag)
        else:
            # TODO Log unable to check index insert index before
            n_ko = None

        return n_ko

    @validate
    def check_not_empthy_column(self,
                                columns: Union[str, list] = None,
                                get_rows_flag: bool = False) -> Union[int, dict, None]:

        if columns is None:
            if self.not_empthy_columns is None:
                # TODO Log unable to check empthy columns set them before
                result = None
            else:
                result = {}
                for col in self.not_empthy_columns:
                    check = NotEmpthyColumn(self, col)
                    result[col] = check.check(get_rows_flag=get_rows_flag)
        else:
            if isinstance(columns, str):
                check = NotEmpthyColumn(self, columns)
                result = check.check(get_rows_flag=get_rows_flag)
            else:
                result = {}
                for col in columns:
                    check = NotEmpthyColumn(self, col)
                    result[col] = check.check(get_rows_flag=get_rows_flag)

        return result

    @validate
    def check_datetime_format(self,
                              columns: Union[str, list] = None,
                              get_rows_flag: bool = False) -> Union[int, dict, None]:

        if columns is None:
            if self.datetime_columns is None:
                # TODO Log unable to check datetime columns set them before
                result = None
            else:
                result = {}
                for col in self.datetime_columns:
                    check = DatetimeFormat(self, col)
                    result[col] = check.check(get_rows_flag=get_rows_flag)
        else:
            if isinstance(columns, str):
                check = DatetimeFormat(self, columns)
                result = check.check(get_rows_flag=get_rows_flag)
            else:
                result = {}
                for col in columns:
                    check = DatetimeFormat(self, col)
                    result[col] = check.check(get_rows_flag=get_rows_flag)

        return result

    @validate
    def run_basic_check(self,
                        get_rows_flag: bool = False):
        self.check_index_not_null(get_rows_flag=get_rows_flag)
        self.check_duplicates_index(get_rows_flag=get_rows_flag)
        self.check_not_empthy_column(get_rows_flag=get_rows_flag)
        self.check_datetime_format(get_rows_flag=get_rows_flag)

    @validate
    def check_columns_between_values(self,
                                     columns: Union[str, list],
                                     min_value: float = None,
                                     max_value: float = None,
                                     min_included: bool = True,
                                     max_included: bool = True,
                                     get_rows_flag: bool = False) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = ColumnBetweenValues(self,
                                        columns,
                                        min_value=min_value,
                                        max_value=max_value,
                                        min_included=min_included,
                                        max_included=max_included)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = ColumnBetweenValues(self,
                                            col,
                                            min_value=min_value,
                                            max_value=max_value,
                                            min_included=min_included,
                                            max_included=max_included)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_columns_between_dates(self,
                                    columns: Union[str, list],
                                    min_date: Union[str, date, datetime] = None,
                                    max_date: Union[str, date, datetime] = None,
                                    min_included: bool = True,
                                    max_included: bool = True,
                                    get_rows_flag: bool = False) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = ColumnBetweenDates(self,
                                       columns,
                                       min_date=min_date,
                                       max_date=max_date,
                                       min_included=min_included,
                                       max_included=max_included)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = ColumnBetweenDates(self,
                                           col,
                                           min_date=min_date,
                                           max_date=max_date,
                                           min_included=min_included,
                                           max_included=max_included)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_date_column_not_in_future(self,
                                        column: Union[str, list],
                                        include: bool = True,
                                        get_rows_flag: bool = False) -> Union[int, dict, None]:
        now = datetime.now()
        return self.check_columns_between_dates(column,
                                                max_date=now,
                                                max_included=include,
                                                get_rows_flag=get_rows_flag)

    @validate
    def check_dates_order(self,
                          ascending_columns: list,
                          strictly_ascending: bool = False,
                          get_rows_flag: bool = False) -> int:
        check = DatesOrder(
            self,
            ascending_columns=ascending_columns,
            strictly_ascending=strictly_ascending
        )
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_values_order(self,
                           ascending_columns: list,
                           strictly_ascending: bool = False,
                           get_rows_flag: bool = False) -> int:
        check = ValuesOrder(
            self,
            ascending_columns=ascending_columns,
            strictly_ascending=strictly_ascending
        )
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_values_in_list(self,
                             columns: Union[str, list],
                             values_list: list,
                             case_sensitive: bool = True,
                             get_rows_flag: bool = False) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = ValuesInList(self,
                                 columns,
                                 values_list=values_list,
                                 case_sensitive=case_sensitive)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = ValuesInList(self,
                                     col,
                                     values_list=values_list,
                                     case_sensitive=case_sensitive)
                result = check.check(get_rows_flag=get_rows_flag)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_column_match_regex(self,
                                 columns: Union[str, list],
                                 regex: str,
                                 case_sensitive: bool = True,
                                 get_rows_flag: bool = False) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = MatchRegex(self,
                               columns,
                               regex=regex,
                               case_sensitive=case_sensitive)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = MatchRegex(self,
                                   col,
                                   regex=regex,
                                   case_sensitive=case_sensitive)
                result = check.check(get_rows_flag=get_rows_flag)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_custom_condition(self,
                               negative_condition: str,
                               ignore_condition: str = None,
                               check_description: str = None,
                               columns_not_null: Union[str, list] = None,
                               get_rows_flag: bool = False) -> int:

        negative_condition = _clean_sql_filter(negative_condition)
        if check_description is None:
            check_description = "Custom condition failed"
        check = Custom(
            self,
            negative_filter=negative_condition,
            ignore_filters=ignore_condition,
            columns_not_null=columns_not_null,
            check_description=check_description
        )
        return check.check(get_rows_flag=get_rows_flag)




