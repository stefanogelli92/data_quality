from __future__ import annotations
from valdec.decorators import validate
from typing import Callable, Union, List
from copy import deepcopy

import pandas as pd

from data_quality.src.table import Table
from data_quality.src.sources import Sources


class DataQualitySession(object):

    def __init__(self):
        self.tables = []

    def create_sources(self,
                       run_query_function: Callable[[str], pd.DataFrame],
                       type_sources: str = None,
                       ) -> Sources:
        return Sources(run_query_function, self, type_sources=type_sources)

    def create_table_from_dataframe(self,
                                    df: pd.DataFrame,
                                    index_column: str = None,
                                    output_name: str = None,
                                    not_empthy_columns: Union[List[str], str, None] = None,
                                    datetime_columns: Union[List[str], str, None] = None,
                                    datetime_formats: Union[List[str], str, None] = None,
                                    table_filter: str = None
                                    ) -> Table:
        table = Table(df=df,
                      index_column=index_column,
                      output_name=output_name,
                      table_filter=table_filter,
                      not_empthy_columns=not_empthy_columns,
                      datetime_columns=datetime_columns,
                      datetime_formats=datetime_formats)
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
