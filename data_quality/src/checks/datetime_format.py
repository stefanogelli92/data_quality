import pandas as pd

from data_quality.src.check import Check
from data_quality.src.checks.custom import Custom
# TODO formatting datetime custom
from data_quality.src.utils import _create_filter_columns_not_null


class DatetimeFormat(Check):

    def __init__(self,
                 table,
                 column_name: str):
        self.table = table
        self.check_description = f"Wrong Format in column {column_name}"
        self.column_name = column_name
        negative_filter = f"cast({column_name} as TIMESTAMP) is null"
        ignore_filter = _create_filter_columns_not_null(column_name)
        self.custom_check = Custom(table,
                                   negative_filter,
                                   self.check_description,
                                   ignore_filters=ignore_filter)

    def _get_number_ko_sql(self) -> int:
        return self.custom_check._get_number_ko_sql()

    def _get_rows_ko_sql(self) -> pd.DataFrame:
        return self.custom_check._get_rows_ko_sql()

    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        df = self.table.df
        df = df[df[self.column_name].notnull() & (df[self.column_name].astype(str) != "")]
        check_column = pd.to_datetime(df[self.column_name], errors="coerce")
        df = df[check_column.isna()]
        return df




