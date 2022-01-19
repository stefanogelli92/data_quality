from abc import ABC, abstractmethod
from typing import Callable, Union, List

import pandas as pd

from data_quality.src.sources_types.bigquery import BigQuery
from data_quality.src.sources_types.impala import Impala
from data_quality.src.table import Table


class Sources(object):
    def __init__(self,
                 run_query_function: Callable[[str], pd.DataFrame],
                 session,
                 type_sources: str):
        self.run_query_function = run_query_function
        self.session = session
        self.list_source_type = [
            Impala(run_query_function),
            BigQuery(run_query_function)
        ]
        self.cast_datetime_sql = None
        self.cast_float_sql = None
        self.set_source_type(type_sources)

    def set_source_type(self, type_sources: str):
        list_source_names = [t.name for t in self.list_source_type]
        if type_sources is None:
            self.check_source_type()
        elif type_sources.lower() in list_source_names:
            self.set_sql_functons(type_sources)
        else:
            raise Exception(f"Source type unknown. Values admitted are: {','.join(list_source_names)}")

    def check_source_type(self):
        self.check_query_function()

        # Cast Datetime
        cast_datetime_sql = None
        for source_type in self.list_source_type:
            if cast_datetime_sql is None:
                check = source_type.check_cast_datetime()
                if check:
                    cast_datetime_sql = source_type.cast_datetime_sql
        if cast_datetime_sql is not None:
            self.cast_datetime_sql = cast_datetime_sql
        else:
            raise Exception("Unable to query db for cast as datetime.")

        # Cast float
        cast_float_sql = None
        for source_type in self.list_source_type:
            if cast_float_sql is None:
                check = source_type.check_cast_float()
                if check:
                    cast_float_sql = source_type.cast_float_sql
        if cast_float_sql is not None:
            self.cast_float_sql = cast_float_sql
        else:
            raise Exception("Unable to query db for cast as float.")

    def check_query_function(self):
            query = """
            SELECT 
                1 as A
            """
            df = self.run_query(query)
            if (df["A"] == 1).sum() != 1:
                raise Exception("Unable to query on database. Check your run_query_function.")

    def set_sql_functons(self, type_sources):
        # Cast Datetime
        for source_type in self.list_source_type:
            if type_sources.lower() == source_type.name:
                self.cast_datetime_sql = source_type.cast_datetime_sql
        # Cast Float
        for source_type in self.list_source_type:
            if type_sources.lower() == source_type.name:
                self.cast_float_sql = source_type.cast_float_sql

    def create_table(self,
                     name: str,
                     index_column: str = None,
                     output_name: str = None,
                     not_empthy_columns: Union[List[str], str] = None,
                     datetime_columns: Union[List[str], str] = None,
                     datetime_formats: Union[List[str], str] = None,
                     table_filter: str = None
                     ) -> Table:
        table = Table(db_name=name,
                      source=self,
                      index_column=index_column,
                      output_name=output_name,
                      table_filter=table_filter,
                      not_empthy_columns=not_empthy_columns,
                      datetime_columns=datetime_columns,
                      datetime_formats=datetime_formats)
        self.session.tables.append(table)
        return table

    def run_query(self, query: str) -> pd.DataFrame:
        return self.run_query_function(query)

