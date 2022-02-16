from __future__ import annotations
from valdec.decorators import validate
from typing import Callable, Union, List
from copy import deepcopy

import pandas as pd

from data_quality.src.plot import plot_session_results
from data_quality.src.table import Table
from data_quality.src.sources import Sources
from data_quality.src.utils import TAG_FLAG_ONLY_WARNING, TAG_FLAG_WARNING, TAG_WARNING_DESCRIPTION

DEFAULT_MAX_ROWS_OUTPUT = 1000


class DataQualitySession(object):

    def __init__(self):
        self.tables = []

    def create_sources(self,
                       run_query_function: Callable[[str], pd.DataFrame],
                       type_sources: str = None,
                       get_rows_flag: bool = False,
                       n_max_rows_output: int = DEFAULT_MAX_ROWS_OUTPUT
                       ) -> Sources:
        return Sources(run_query_function,
                       self,
                       type_sources=type_sources,
                       get_rows_flag=get_rows_flag,
                       n_max_rows_output=n_max_rows_output)

    def create_table_from_dataframe(self,
                                    df: pd.DataFrame,
                                    output_name: str,
                                    index_column: str = None,
                                    not_empthy_columns: Union[List[str], str, None] = None,
                                    datetime_columns: Union[List[str], str, None] = None,
                                    datetime_formats: Union[List[str], str, None] = None,
                                    table_filter: str = None,
                                    output_columns: Union[List[str], str] = None
                                    ) -> Table:
        table = Table(df=df,
                      index_column=index_column,
                      output_name=output_name,
                      table_filter=table_filter,
                      not_empthy_columns=not_empthy_columns,
                      datetime_columns=datetime_columns,
                      datetime_formats=datetime_formats,
                      output_columns=output_columns)
        self.tables.append(table)
        return table

    @validate
    def del_table(self,
                  table: Table):
        self.tables.remove(table)

    @validate
    def create_new_table_by_filter(self,
                                   table: Table,
                                   table_filter: str = None,
                                   output_name: str = None) -> Table:
        new_table = deepcopy(table)
        new_table.set_table_filer(table_filter)
        if output_name is not None:
            new_table.output_name = output_name
        self.tables.append(new_table)
        return new_table

    @validate
    def create_html_output(self, **kargs):
        plot_session_results(self, **kargs)

    @validate
    def create_export_details_excel(self,
                                    path: str,
                                    consider_warnings: bool = True):
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        for table in self.tables:
            _df = table.get_ko_rows(consider_warnings=consider_warnings)
            if not table.any_warning(flag_only_fail=False):
                _df.drop([TAG_WARNING_DESCRIPTION, TAG_FLAG_WARNING, TAG_FLAG_ONLY_WARNING], axis=1, inplace=True)
            _df.to_excel(writer, sheet_name=table.get_output_name(), index=None)
        writer.save()

