from typing import Union, List, Optional, Dict
from valdec.decorators import validate
from datetime import date, datetime

import pandas as pd

from data_quality.src.checks.dates_order_dimension_table import DatesOrderDimensionTable
from data_quality.src.plot import plot_table_results
from data_quality.src.utils import _clean_sql_filter, _aggregate_sql_filter, _output_column_to_sql, _query_limit
from data_quality.src.check import TAG_CHECK_DESCRIPTION, TAG_WARNING_DESCRIPTION
from data_quality.src.checks.index_null import IndexNull
from data_quality.src.checks.values_duplicate import ValuesDuplicate
from data_quality.src.checks.not_empthy_column import NotEmpthyColumn
from data_quality.src.checks.datetime_format import DatetimeFormat
from data_quality.src.checks.column_between_values import ColumnBetweenValues
from data_quality.src.checks.column_between_dates import ColumnBetweenDates
from data_quality.src.checks.dates_order import DatesOrder
from data_quality.src.checks.values_order import ValuesOrder
from data_quality.src.checks.values_in_list import ValuesInList
from data_quality.src.checks.match_regex import MatchRegex
from data_quality.src.checks.custom import Custom
from data_quality.src.checks.match_dimension_table import MatchDImensionTable


class Table:
    def __init__(self,
                 db_name: str = None,
                 source=None,
                 df: pd.DataFrame = None,
                 index_column: str = None,
                 output_name: str = None,
                 not_empthy_columns: Union[str, list] = None,
                 datetime_columns: Union[str, list, None] = None,
                 datetime_formats: Union[str, list, None] = None,
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
            self.source = source
        self.output_name = output_name if output_name is not None else db_name
        self.index_column = index_column
        self.not_empthy_columns = None
        self.set_not_empthy_columns(not_empthy_columns)
        self.datetime_columns = {}
        self.set_datetime_columns(datetime_columns, datetime_formats)
        self.table_filter = None
        self.set_table_filer(table_filter)
        self.output_columns = None
        self.set_output_columns(output_columns)
        self.n_max_rows_output = n_max_rows_output

        # Result parameters
        self.n_rows = None
        self.index_problem = False
        self.passed_all_checks = True
        self.check_list = []
        self.ko_rows = None
        self.n_checks = None
        self.n_warning_checks = None
        self.total_number_ko = None
        self.total_number_warnings = None
        self.number_unique_rows_ko = None
        self.number_unique_rows_warning = None
        self.max_number_ko = None
        self.max_number_warnings = None

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

    def set_datetime_columns(self,
                             columns_list: Union[List[str], str, None],
                             datetime_formats: Union[List[str], str, None] = None,
                             replace_formats: bool = True):
        if columns_list is not None:
            if isinstance(columns_list, str):
                columns_list = [columns_list]
            if isinstance(datetime_formats, str):
                datetime_formats = [datetime_formats] * len(columns_list)
            elif datetime_formats is None:
                datetime_formats = [None] * len(columns_list)
            elif isinstance(datetime_formats, list):
                if len(datetime_formats) != len(columns_list):
                    raise Exception("Len of datetime formats != len datetime columns")
            for i in range(len(columns_list)):
                if replace_formats or (columns_list[i] not in self.datetime_columns):
                    if (datetime_formats[i] is not None) or self.flag_dataframe:
                        self.datetime_columns[columns_list[i]] = datetime_formats[i]
                    else:
                        self.datetime_columns[columns_list[i]] = self._find_datetime_format(columns_list[i])

    def _find_datetime_format(self, column_name):
        result = None
        query = f"""select {column_name} from {self.db_name} where {column_name} is not null limit 100"""
        df = self.source.run_query(query)
        is_datetime = df[df.columns[0]].apply(lambda x: isinstance(x, (datetime, date))).mean() > 0.5
        if not is_datetime:
            formats = df[df.columns[0]].apply(lambda x: pd._libs.tslibs.parsing.guess_datetime_format(x)).mode().values
            if len(formats) > 0:
                result = formats[0]
                format_replace = self.source.datetime_format_replace_dictionary
                for k, v in format_replace.items():
                    result = result.replace(k, v)
        return result

    @validate
    def set_output_columns(self, columns_list: Union[List[str], str, None]):
        if isinstance(columns_list, str):
            columns_list = [columns_list]
        if (self.index_column is not None) and (columns_list is not None):
            columns_list.insert(0, self.index_column)
            columns_list = list(dict.fromkeys(columns_list))
        self.output_columns = columns_list

    def calculate_result_info(self):
        self.get_number_of_rows()
        self.n_checks = len([a for a in self.check_list if not a.flag_warning])
        self.n_warning_checks = len([a for a in self.check_list if a.flag_warning])
        self._create_ko_rows()
        if not any([a.flag_over_max_rows for a in self.check_list if not a.flag_warning]):
            df = self.get_ko_rows(consider_warnings=False)
            self.number_unique_rows_ko = df.shape[0]
        if not any([a.flag_over_max_rows for a in self.check_list if a.flag_warning]):
            df = self.get_ko_rows(consider_warnings=True)
            self.number_unique_rows_warning = df[df["flag_only_warning"]].shape[0]
        self.max_number_ko = max([a.n_ko for a in self.check_list if not a.flag_warning], default=0)
        self.max_number_warnings = max([a.n_ko for a in self.check_list if a.flag_warning], default=0)
        self.total_number_ko = sum([a.n_ko for a in self.check_list if not a.flag_warning])
        self.total_number_warnings = sum([a.n_ko for a in self.check_list if a.flag_warning])

    def get_number_of_rows(self, refresh: bool = False):
        if self.flag_dataframe:
            self.n_rows = self.df.shape[0]
        elif (self.n_rows is None) or refresh:
            self.query_number_of_rows()

    def passed_all_checks(self, consider_warnings: bool = False) -> bool:
        failed_checks = [a for a in self.check_list if a.flag_ko]
        if not consider_warnings:
            failed_checks = [a for a in self.check_list if not a.flag_warning]
        return len(failed_checks) > 0

    def over_n_max_rows_output(self, consider_warnings: bool = False) -> bool:
        if consider_warnings:
            return any([a.flag_over_max_rows for a in self.check_list])
        else:
            return any([a.flag_over_max_rows for a in self.check_list if not a.flag_warning])

    def any_warning(self, flag_only_fail: True):
        if flag_only_fail:
            return len([a for a in self.check_list if (a.flag_warning) and (a.n_ko > 0)]) > 0
        else:
            return len([a for a in self.check_list if a.flag_warning]) > 0

    def _create_ko_rows(self):
        list_ko_rows = []
        for check in self.check_list:
            if check.flag_ko:
                _df = check.ko_rows
                if check.flag_warning:
                    _df[TAG_WARNING_DESCRIPTION] = _df[TAG_CHECK_DESCRIPTION]
                    _df[TAG_CHECK_DESCRIPTION] = None
                    _df["flag_warning"] = True
                else:
                    _df[TAG_WARNING_DESCRIPTION] = None
                    _df["flag_warning"] = False
                list_ko_rows.append(_df)

        drop_columns_list = [TAG_CHECK_DESCRIPTION, TAG_WARNING_DESCRIPTION, "flag_warning"]

        df = pd.concat(list_ko_rows, ignore_index=True)
        column_list = list(df.columns)
        column_list = [a for a in column_list if a not in drop_columns_list]
        df = df.groupby(column_list, dropna=False).agg({TAG_CHECK_DESCRIPTION: set,
                                                        TAG_WARNING_DESCRIPTION: set,
                                                        "flag_warning": min}).reset_index()
        df[TAG_CHECK_DESCRIPTION] = df[TAG_CHECK_DESCRIPTION].apply(lambda x: " - ".join([a for a in x if a is not None]))
        df[TAG_WARNING_DESCRIPTION] = df[TAG_WARNING_DESCRIPTION].apply(lambda x: " - ".join([a for a in x if a is not None]))
        if (df.shape[0] > 0) and (self.index_column is not None) and (not self.index_problem):
            df0 = df.drop(drop_columns_list, axis=1)
            df1 = df[[self.index_column] + drop_columns_list]
            df0.groupby(self.index_column, dropna=False).fillna(method="ffill", inplace=True)
            df0.groupby(self.index_column, dropna=False).fillna(method="bfill", inplace=True)
            df0 = df0.drop_duplicates()
            df1 = df1.groupby(self.index_column, dropna=False).agg({TAG_CHECK_DESCRIPTION: set,
                                                                    TAG_WARNING_DESCRIPTION: set,
                                                                    "flag_warning": min}).reset_index()
            df1[TAG_CHECK_DESCRIPTION] = df1[TAG_CHECK_DESCRIPTION].apply(
                lambda x: " - ".join([a for a in x if a is not None]))
            df1[TAG_WARNING_DESCRIPTION] = df1[TAG_WARNING_DESCRIPTION].apply(
                lambda x: " - ".join([a for a in x if a is not None]))
            df = df0.merge(df1, how="left", on=self.index_column)
        df["flag_only_warning"] = df[TAG_CHECK_DESCRIPTION].str.len() == 0
        self.ko_rows = df
        return

    def get_ko_rows(self,
                    reset: bool = False,
                    consider_warnings: bool = True,
                    output_columns: Union[List[str], None] = None) -> pd.DataFrame:
        if reset or self.ko_rows is None:
            self._create_ko_rows()
        df = self.ko_rows.copy()
        if not consider_warnings:
            df.drop([TAG_WARNING_DESCRIPTION], axis=1, inplace=True)
            df = df[~df["flag_only_warning"]]
        if output_columns is not None:
            df.drop([col for col in df.columns if col in output_columns], axis=1, inplace=True)
        return df

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
            df = self.source.run_query(query)
            self.flag_dataframe = True
            self.df = df

    @validate
    def query_number_of_rows(self):
        filter_sql = _aggregate_sql_filter(self.table_filter)
        query = f"""
        SELECT 
            count(*) as n_rows
        from {self.db_name}
        {filter_sql}
        """
        result = self.source.run_query(query)["n_rows"].values[0]
        self.n_rows = result

    # Check methods

    @validate
    def check_index_not_null(self,
                             ignore_filter: Optional[str] = None,
                             columns_not_null: Optional[Union[str, List[str]]] = None,
                             get_rows_flag: bool = False,
                             output_columns: Optional[Union[List[str], str]] = None,
                             flag_warning: bool = False,
                             check_description: Optional[str] = None,
                             n_max_rows_output: Optional[int] = None) -> Optional[int]:

        if self.index_column is not None:
            check = IndexNull(self)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
            n_ko = check.check(get_rows_flag=get_rows_flag)
        else:
            n_ko = None

        return n_ko

    @validate
    def check_duplicate_index(self,
                              ignore_filter: Union[str, None] = None,
                              columns_not_null: Union[str, List[str], None] = None,
                              get_rows_flag: bool = False,
                              output_columns: Union[List[str], str, None] = None,
                              flag_warning: bool = False,
                              check_description: Union[List[str], str, None] = None,
                              n_max_rows_output: Union[int, None] = None) -> Optional[int]:

        if self.index_column is not None:
            check = ValuesDuplicate(self, self.index_column)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
            n_ko = check.check(get_rows_flag=get_rows_flag)
        else:
            n_ko = None

        return n_ko

    @validate
    def check_not_empthy_column(self,
                                columns: Union[str, list] = None,
                                ignore_filter: Union[str, None] = None,
                                columns_not_null: Union[str, List[str], None] = None,
                                get_rows_flag: bool = False,
                                output_columns: Union[List[str], str, None] = None,
                                flag_warning: bool = False,
                                check_description: str = None,
                                n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:

        if columns is None:
            if self.not_empthy_columns is None:
                result = None
            else:
                result = {}
                for col in self.not_empthy_columns:
                    check = NotEmpthyColumn(self, col)
                    check.initialize_params(check_description=check_description,
                                            flag_warning=flag_warning,
                                            n_max_rows_output=n_max_rows_output,
                                            ignore_filter=ignore_filter,
                                            columns_not_null=columns_not_null,
                                            output_columns=output_columns)
                    result[col] = check.check(get_rows_flag=get_rows_flag)
        else:
            if isinstance(columns, str):
                check = NotEmpthyColumn(self, columns)
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result = check.check(get_rows_flag=get_rows_flag)
            else:
                result = {}
                for col in columns:
                    check = NotEmpthyColumn(self, col)
                    check.initialize_params(check_description=check_description,
                                            flag_warning=flag_warning,
                                            n_max_rows_output=n_max_rows_output,
                                            ignore_filter=ignore_filter,
                                            columns_not_null=columns_not_null,
                                            output_columns=output_columns)
                    result[col] = check.check(get_rows_flag=get_rows_flag)

        return result

    @validate
    def check_duplicate_values(self,
                               columns: Union[str, list],
                               ignore_filter: Union[str, None] = None,
                               columns_not_null: Union[str, List[str], None] = None,
                               get_rows_flag: bool = False,
                               output_columns: Union[List[str], str, None] = None,
                               flag_warning: bool = False,
                               check_description: Union[List[str], str, None] = None,
                               n_max_rows_output: Union[int, None] = None) -> Dict:
        if isinstance(columns, str):
            columns = [columns]
        result = {}
        for col in columns:
            check = ValuesDuplicate(self, col)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
            result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_datetime_format(self,
                              columns: Union[str, list, None] = None,
                              datetime_formats: Union[str, list, None] = None,
                              ignore_filter: Union[str, None] = None,
                              columns_not_null: Union[str, List[str], None] = None,
                              get_rows_flag: bool = False,
                              output_columns: Union[List[str], str, None] = None,
                              flag_warning: bool = False,
                              check_description: Union[List[str], str] = None,
                              n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:

        if columns is not None:
            self.set_datetime_columns(columns, datetime_formats)

        result = {}
        for col, f in self.datetime_columns.items():
            if (columns is None) or (col in columns):
                check = DatetimeFormat(self, col)
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result[col] = check.check(get_rows_flag=get_rows_flag)

        return result

    @validate
    def run_basic_check(self,
                        **kwargs):
        self.check_index_not_null(**kwargs)
        self.check_duplicate_index(**kwargs)
        self.check_not_empthy_column(**kwargs)
        self.check_datetime_format(**kwargs)

    @validate
    def check_columns_between_values(self,
                                     columns: Union[str, list],
                                     min_value: float = None,
                                     max_value: float = None,
                                     min_included: bool = True,
                                     max_included: bool = True,
                                     ignore_filter: Union[str, None] = None,
                                     columns_not_null: Union[str, List[str], None] = None,
                                     get_rows_flag: bool = False,
                                     output_columns: Union[List[str], str, None] = None,
                                     flag_warning: bool = False,
                                     check_description: Union[str, None] = None,
                                     n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = ColumnBetweenValues(self,
                                        columns,
                                        min_value=min_value,
                                        max_value=max_value,
                                        min_included=min_included,
                                        max_included=max_included)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
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
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_columns_between_dates(self,
                                    columns: Union[List[str], str, None],
                                    min_date: Union[str, date, datetime] = None,
                                    max_date: Union[str, date, datetime] = None,
                                    min_included: bool = True,
                                    max_included: bool = True,
                                    ignore_filter: Union[str, None] = None,
                                    columns_not_null: Union[str, List[str], None] = None,
                                    get_rows_flag: bool = False,
                                    output_columns: Union[List[str], str, None] = None,
                                    flag_warning: bool = False,
                                    check_description: Union[str, None] = None,
                                    n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:
        self.set_datetime_columns(columns, replace_formats=False)
        if isinstance(columns, str):
            check = ColumnBetweenDates(self,
                                       columns,
                                       min_date=min_date,
                                       max_date=max_date,
                                       min_included=min_included,
                                       max_included=max_included)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
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
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_date_column_not_in_future(self,
                                        column: Union[str, list],
                                        include: bool = True,
                                        ignore_filter: Union[str, None] = None,
                                        columns_not_null: Union[str, List[str], None] = None,
                                        get_rows_flag: bool = False,
                                        output_columns: Union[List[str], str, None] = None,
                                        flag_warning: bool = False,
                                        check_description: Union[str, None] = None,
                                        n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:
        now = datetime.now()
        return self.check_columns_between_dates(column,
                                                max_date=now,
                                                max_included=include,
                                                get_rows_flag=get_rows_flag,
                                                flag_warning=flag_warning,
                                                check_description=check_description,
                                                n_max_rows_output=n_max_rows_output,
                                                ignore_filter=ignore_filter,
                                                columns_not_null=columns_not_null,
                                                output_columns=output_columns)

    @validate
    def check_dates_order(self,
                          ascending_columns: list,
                          strictly_ascending: bool = False,
                          ignore_filter: Union[str, None] = None,
                          columns_not_null: Union[str, List[str], None] = None,
                          get_rows_flag: bool = False,
                          output_columns: Union[List[str], str, None] = None,
                          flag_warning: bool = False,
                          check_description: Union[str, None] = None,
                          n_max_rows_output: Union[int, None] = None) -> int:
        self.set_datetime_columns(ascending_columns, replace_formats=False)
        check = DatesOrder(
            self,
            ascending_columns=ascending_columns,
            strictly_ascending=strictly_ascending
        )
        check.initialize_params(check_description=check_description,
                                flag_warning=flag_warning,
                                n_max_rows_output=n_max_rows_output,
                                ignore_filter=ignore_filter,
                                columns_not_null=columns_not_null,
                                output_columns=output_columns)
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_values_order(self,
                           ascending_columns: list,
                           strictly_ascending: bool = False,
                           ignore_filter: Union[str, None] = None,
                           columns_not_null: Union[str, List[str], None] = None,
                           get_rows_flag: bool = False,
                           output_columns: Union[List[str], str, None] = None,
                           flag_warning: bool = False,
                           check_description: Union[str, None] = None,
                           n_max_rows_output: Union[int, None] = None) -> int:
        check = ValuesOrder(
            self,
            ascending_columns=ascending_columns,
            strictly_ascending=strictly_ascending
        )
        check.initialize_params(check_description=check_description,
                                flag_warning=flag_warning,
                                n_max_rows_output=n_max_rows_output,
                                ignore_filter=ignore_filter,
                                columns_not_null=columns_not_null,
                                output_columns=output_columns)
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_values_in_list(self,
                             columns: Union[str, list],
                             values_list: list,
                             case_sensitive: bool = True,
                             ignore_filter: Union[str, None] = None,
                             columns_not_null: Union[str, List[str], None] = None,
                             get_rows_flag: bool = False,
                             output_columns: Union[List[str], str, None] = None,
                             flag_warning: bool = False,
                             check_description: Union[str, None] = None,
                             n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = ValuesInList(self,
                                 columns,
                                 values_list=values_list,
                                 case_sensitive=case_sensitive)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = ValuesInList(self,
                                     col,
                                     values_list=values_list,
                                     case_sensitive=case_sensitive)
                result = check.check(get_rows_flag=get_rows_flag)
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_column_match_regex(self,
                                 columns: Union[str, list],
                                 regex: str,
                                 ignore_filter: Union[str, None] = None,
                                 columns_not_null: Union[str, List[str], None] = None,
                                 case_sensitive: bool = True,
                                 get_rows_flag: bool = False,
                                 output_columns: Union[List[str], str, None] = None,
                                 flag_warning: bool = False,
                                 check_description: Union[str, None] = None,
                                 n_max_rows_output: Union[int, None] = None) -> Union[int, dict, None]:
        if isinstance(columns, str):
            check = MatchRegex(self,
                               columns,
                               regex=regex,
                               case_sensitive=case_sensitive)
            check.initialize_params(check_description=check_description,
                                    flag_warning=flag_warning,
                                    n_max_rows_output=n_max_rows_output,
                                    ignore_filter=ignore_filter,
                                    columns_not_null=columns_not_null,
                                    output_columns=output_columns)
            result = check.check(get_rows_flag=get_rows_flag)
        else:
            result = {}
            for col in columns:
                check = MatchRegex(self,
                                   col,
                                   regex=regex,
                                   case_sensitive=case_sensitive)
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result[col] = check.check(get_rows_flag=get_rows_flag)
        return result

    @validate
    def check_custom_condition(self,
                               negative_condition: str,
                               ignore_condition: Union[str, None] = None,
                               check_description: Union[str, None] = None,
                               columns_not_null: Union[str, list] = None,
                               get_rows_flag: bool = False,
                               output_columns: Union[List[str], str, None] = None,
                               flag_warning: bool = False,
                               n_max_rows_output: Union[int, None] = None) -> int:

        negative_condition = _clean_sql_filter(negative_condition)
        if check_description is None:
            check_description = "Custom condition failed"
        check = Custom(
            self,
            negative_filter=negative_condition,
            check_description=check_description
        )
        check.initialize_params(check_description=check_description,
                                flag_warning=flag_warning,
                                n_max_rows_output=n_max_rows_output,
                                ignore_filter=ignore_condition,
                                columns_not_null=columns_not_null,
                                output_columns=output_columns)
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_match_dimension_table(self,
                                    foreign_keys: Union[str, list],
                                    dimension_table,
                                    primary_keys: Union[str, list] = None,
                                    ignore_filter: Union[str, None] = None,
                                    columns_not_null: Union[str, List[str], None] = None,
                                    get_rows_flag: bool = False,
                                    output_columns: Union[List[str], str, None] = None,
                                    check_description: Union[str, None] = None,
                                    flag_warning: bool = False,
                                    n_max_rows_output: Union[int, None] = None) -> int:
        check = MatchDImensionTable(
            self,
            foreign_keys=foreign_keys,
            dimension_table=dimension_table,
            primary_keys=primary_keys
        )
        check.initialize_params(check_description=check_description,
                                flag_warning=flag_warning,
                                n_max_rows_output=n_max_rows_output,
                                ignore_filter=ignore_filter,
                                columns_not_null=columns_not_null,
                                output_columns=output_columns)
        return check.check(get_rows_flag=get_rows_flag)

    @validate
    def check_dates_order_dimension_table(self,
                                          foreign_keys: Union[str, list],
                                          dimension_table,
                                          left_columns: Union[str, list],
                                          right_columns: Union[str, list],
                                          operator: str = "<=",
                                          primary_keys: Union[str, list] = None,
                                          ignore_filter: Union[str, None] = None,
                                          columns_not_null: Union[str, List[str], None] = None,
                                          get_rows_flag: bool = False,
                                          output_columns: Union[List[str], str, None] = None,
                                          check_description: Union[str, None] = None,
                                          flag_warning: bool = False,
                                          n_max_rows_output: Union[int, None] = None) -> int:
        self.set_datetime_columns(left_columns, replace_formats=False)
        dimension_table.set_datetime_columns(right_columns, replace_formats=False)
        if isinstance(left_columns, str):
            left_columns = [left_columns]
        if isinstance(right_columns, str):
            right_columns = [right_columns]
        result = 0
        for lc in left_columns:
            for rc in right_columns:
                check = DatesOrderDimensionTable(
                    self,
                    foreign_keys=foreign_keys,
                    dimension_table=dimension_table,
                    left_column=lc,
                    right_column=rc,
                    operator=operator,
                    primary_keys=primary_keys
                )
                check.initialize_params(check_description=check_description,
                                        flag_warning=flag_warning,
                                        n_max_rows_output=n_max_rows_output,
                                        ignore_filter=ignore_filter,
                                        columns_not_null=columns_not_null,
                                        output_columns=output_columns)
                result += check.check(get_rows_flag=get_rows_flag)
        return result

    def create_html_output(self,
                           title: str = None,
                           sort_by_n_ko: bool = True,
                           consider_warnings: bool = True,
                           filter_only_ko: bool = True,
                           save_in_path: str = None,
                           show_flag: bool = False):

        return plot_table_results(self,
                               title=title,
                               sort_by_n_ko=sort_by_n_ko,
                               consider_warnings=consider_warnings,
                               filter_only_ko=filter_only_ko,
                               save_in_path=save_in_path,
                               show_flag=show_flag)
