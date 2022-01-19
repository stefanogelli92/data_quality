from typing import Union, Optional

import pandas as pd
import re

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
from data_quality.src.utils import _create_filter_columns_not_null


class MatchRegex(Check):

    def __init__(self,
                 table,
                 column_name: str,
                 regex: str,
                 case_sensitive: bool = True
                 ):
        self.table = table
        self.column_name = column_name
        self.regex = regex
        self.case_sensitive = case_sensitive

        self.check_description = f"Wrong format in column {column_name}"

        self.custom_check = None

    def _create_filter(self):
        regex = re.sub(r"(?<!\\)\\(?!\\)", r"\\\\", self.regex)
        return f" NOT {self.table.source.match_regex(self.column_name, regex, self.case_sensitive)} "

    def _get_number_ko_sql(self) -> int:
        ignore_filter = _create_filter_columns_not_null(self.column_name)

        negative_filter = self._create_filter()

        self.custom_check = Custom(self.table,
                                   negative_filter,
                                   self.check_description,
                                   ignore_filters=ignore_filter)
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].notnull() & (df[self.column_name].astype(str) != "")]
        df = df[~df[self.column_name].astype(str).str.contains(self.regex, case=self.case_sensitive)]
        return df




