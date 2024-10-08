from typing import Union, Optional
from datetime import date, datetime

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.utils import _create_filter_columns_not_null, COLUMN_CURRENT_CHECK


class ColumnBetweenDates(Check):

    def __init__(self,
                 table,
                 column_name: str,
                 min_date: Union[str, datetime, date] = None,
                 max_date: Union[str, datetime, date] = None,
                 min_included: bool = True,
                 max_included: bool = True
                 ):
        self.column_name = column_name
        self.min_date = pd.to_datetime(min_date)
        self.max_date = pd.to_datetime(max_date)
        self.min_included = min_included
        self.max_included = max_included

        super().__init__(table,
                         self._create_check_description(),
                         [column_name])

    def _create_check_description(self):
        min_date = self.min_date.strftime("%Y-%m-%d") if self.min_date is not None else None
        max_date = self.max_date.strftime("%Y-%m-%d") if self.max_date is not None else None
        if (min_date is not None) and (max_date is not None):
            return f"Value in column {self.column_name} not between {min_date} and {max_date}"
        elif (min_date is not None) and (max_date is None):
            operator = "<" if self.min_included else "<="
            return f"Value in column {self.column_name} {operator} {min_date}"
        elif (min_date is None) and (max_date is not None):
            operator = ">" if self.max_included else ">="
            return f"Value in column {self.column_name} {operator} {self.max_date}"
        else:
            return ""

    def _create_filter(self):
        cast_sql_datetime = self.table.source.cast_datetime_sql(self.column_name, self.table.datetime_columns[self.column_name])
        min_date = self.min_date.strftime("%Y-%m-%d %H:%M:%S") if self.min_date is not None else None
        max_date = self.max_date.strftime("%Y-%m-%d %H:%M:%S") if self.max_date is not None else None
        if (min_date is not None) and (max_date is not None):
            min_operator = "<" if self.min_included else "<="
            max_operator = ">" if self.max_included else ">="
            return f"(({cast_sql_datetime} {min_operator} '{min_date}') OR ({cast_sql_datetime} {max_operator} '{max_date}'))"
        elif (min_date is not None) and (max_date is None):
            operator = "<" if self.min_included else "<="
            return f"({cast_sql_datetime} {operator} '{min_date}')"
        elif (min_date is None) and (max_date is not None):
            operator = ">" if self.max_included else ">="
            return f"({cast_sql_datetime} {operator} '{max_date}')"
        else:
            return ""

    def _get_number_ko_sql(self) -> int:
        self.add_ignore_filter(_create_filter_columns_not_null(self.column_name))
        self.add_ignore_filter(f"{self.table.source.cast_datetime_sql(self.column_name, self.table.datetime_columns[self.column_name])} is not null")
        negative_filter = self._create_filter()
        return self.standard_get_number_ko_sql(negative_filter)

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        self.add_ignore_filter(_create_filter_columns_not_null(self.column_name))
        self.add_ignore_filter(
            f"{self.table.source.cast_datetime_sql(self.column_name, self.table.datetime_columns[self.column_name])} is not null")
        negative_filter = self._create_filter()
        return self.standard_rows_ko_sql(negative_filter)

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].notnull() & (df[self.column_name].astype(str) != "")]
        a = pd.to_datetime(df[self.column_name], errors="coerce")
        df = df[a.notnull()]
        tag_check = COLUMN_CURRENT_CHECK
        df[tag_check] = False
        if self.min_date is not None:
            if self.min_included:
                df[tag_check] = a < self.min_date
            else:
                df[tag_check] = a <= self.min_date
        if self.max_date is not None:
            if self.max_included:
                df[tag_check] = df[tag_check] | (a > self.max_date)
            else:
                df[tag_check] = df[tag_check] | (a >= self.max_date)
        df = df[df[tag_check]]
        df.drop([tag_check], axis=1, inplace=True)
        return df




