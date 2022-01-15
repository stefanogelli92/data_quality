from typing import Union, Optional

import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
from data_quality.src.utils import _human_format


class ColumnBetweenValues(Check):

    def __init__(self,
                 table,
                 column_name: str,
                 min_value: float = None,
                 max_value: float = None,
                 min_included: bool = True,
                 max_included: bool = True
                 ):
        self.table = table
        self.column_name = column_name
        self.min_value = min_value
        self.max_value = max_value
        self.min_included = min_included
        self.max_included = max_included

        self.check_description = self._create_check_description()

        ignore_filter = f"({column_name} is not null) and (cast({column_name} as string) != '')"

        negative_filter = self._create_filter()

        self.custom_check = Custom(table,
                                   negative_filter,
                                   self.check_description,
                                   ignore_filters=ignore_filter)

    def _create_check_description(self):
        if (self.min_value is not None) and (self.max_value is not None):
            return f"Value in column {self.column_name} not between {_human_format(self.min_value)} and {_human_format(self.max_value)}"
        elif (self.min_value is not None) and (self.max_value is None):
            operator = "<" if self.min_included else "<="
            return f"Value in column {self.column_name} {operator} {_human_format(self.min_value)}"
        elif (self.min_value is None) and (self.max_value is not None):
            operator = ">" if self.max_included else ">="
            return f"Value in column {self.column_name} {operator} {_human_format(self.max_value)}"
        else:
            return ""

    def _create_filter(self):
        if (self.min_value is not None) and (self.max_value is not None):
            min_operator = "<" if self.min_included else "<="
            max_operator = ">" if self.max_included else ">="
            return f"((cast({self.column_name} as float) {min_operator} {self.min_value}) AND (cast({self.column_name} as float) {max_operator} {self.max_value}))"
        elif (self.min_value is not None) and (self.max_value is None):
            operator = "<" if self.min_included else "<="
            return f"(cast({self.column_name} as float) {operator} {self.min_value})"
        elif (self.min_value is None) and (self.max_value is not None):
            operator = ">" if self.max_included else ">="
            return f"(cast({self.column_name} as float) {operator} {self.max_value})"
        else:
            return ""

    def _get_number_ko_sql(self) -> int:
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].notnull() & (df[self.column_name].astype(str) != "")]
        tag_check = "current_check_data_quality"
        df[tag_check] = False
        if self.min_value is not None:
            if self.min_included:
                df[tag_check] = df[self.column_name] < self.min_value
            else:
                df[tag_check] = df[self.column_name] <= self.min_value
        if self.max_value is not None:
            if self.max_included:
                df[tag_check] = df[tag_check] | (df[self.column_name] > self.max_value)
            else:
                df[tag_check] = df[tag_check] | (df[self.column_name] >= self.max_value)
        df = df[df[tag_check]]
        df.drop(tag_check, axis=1, inplace=True)
        return df




