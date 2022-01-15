from abc import ABC, abstractmethod

import pandas as pd

TAG_CHECK_DESCRIPTION = "check_description"


class Check(ABC):

    def __init__(self, table, check_description: str):
        self.table = table
        self.check_description = check_description

        self.flag_ko = None
        self.n_ko = None
        self.flag_over_max_rows = None
        self.ko_rows = None

    @abstractmethod
    def _get_number_ko_sql(self) -> int:
        pass

    @abstractmethod
    def _get_rows_ko_sql(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def _get_rows_ko_dataframe(self) -> pd.DataFrame:
        pass

    def check(self, get_rows_flag: bool = False):
        flag_over_max_rows = None
        if self.table.flag_dataframe:
            df_ko = self._get_rows_ko_dataframe()
            n_ko = df_ko.shape[0]
            if self.table.output_columns is not None:
                if n_ko > 0:
                    df_ko = df_ko[self.table.output_columns]
                else:
                    df_ko = pd.DataFrame(columns=self.table.output_columns)
            df_ko[TAG_CHECK_DESCRIPTION] = self.check_description
            flag_over_max_rows = False
        else:
            n_ko = self._get_number_ko_sql()
            if get_rows_flag:
                if n_ko == 0:
                    df_ko = pd.DataFrame(columns=self.table.output_columns)
                    flag_over_max_rows = False
                else:
                    df_ko = self._get_rows_ko_sql()
                    n_rows = df_ko.shape[0]
                    if n_rows == self.table.max_rows:
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
