from abc import ABC, abstractmethod
from typing import Callable, Union, List

import pandas as pd

from data_quality.src.sources_types.bigquery import BigQuery
from data_quality.src.sources_types.impala import Impala
from data_quality.src.table import Table

DEFAULT_MAX_ROWS_OUTPUT = 1000


class Sources(object):
    def __init__(self,
                 run_query_function: Callable[[str], pd.DataFrame],
                 session,
                 type_sources: str,
                 n_max_rows_output: Union[int, None] = DEFAULT_MAX_ROWS_OUTPUT):
        self.run_query_function = run_query_function
        self.session = session
        self.list_source_type = [
            Impala(run_query_function),
            BigQuery(run_query_function)
        ]
        self.cast_datetime_sql = None
        self.cast_float_sql = None
        self.match_regex = None
        self.datetime_format_replace_dictionary = None
        self.set_source_type(type_sources)
        self.n_max_rows_output = n_max_rows_output

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

        # Match Regex
        match_regex = None
        for source_type in self.list_source_type:
            if match_regex is None:
                check = source_type.check_regex()
                if check:
                    match_regex = source_type.match_regex
        if match_regex is not None:
            self.match_regex = match_regex

        # Replace format datetime
        datetime_format_replace_dictionary = None
        for source_type in self.list_source_type:
            if datetime_format_replace_dictionary is None:
                check = source_type.check_datetime_format_replace()
                if check:
                    datetime_format_replace_dictionary = source_type.datetime_format_replace_dictionary
        if datetime_format_replace_dictionary is not None:
            self.datetime_format_replace_dictionary = datetime_format_replace_dictionary
        else:
            raise Exception("Unable to query db for datetime formats.")

    def check_query_function(self):
            query = """
            SELECT 
                1 as a
            """
            df = self.run_query(query)
            if (df["a"] == 1).sum() != 1:
                
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
        # Match regex
        for source_type in self.list_source_type:
            if type_sources.lower() == source_type.name:
                self.match_regex = source_type.match_regex

        # Replace format datetime
        for source_type in self.list_source_type:
            if type_sources.lower() == source_type.name:
                self.datetime_format_replace_dictionary = source_type.datetime_format_replace_dictionary

    def create_table(self,
                     name: str,
                     index_column: str = None,
                     output_name: str = None,
                     not_empthy_columns: Union[List[str], str] = None,
                     datetime_columns: Union[List[str], str] = None,
                     datetime_formats: Union[List[str], str] = None,
                     table_filter: str = None,
                     n_max_rows_output: Union[int, None] = None,
                     output_columns: Union[List[str], str] = None
                     ) -> Table:
        if n_max_rows_output is None:
            n_max_rows_output = self.n_max_rows_output
        table = Table(db_name=name,
                      source=self,
                      index_column=index_column,
                      output_name=output_name,
                      table_filter=table_filter,
                      not_empthy_columns=not_empthy_columns,
                      datetime_columns=datetime_columns,
                      datetime_formats=datetime_formats,
                      output_columns=output_columns,
                      n_max_rows_output=n_max_rows_output)
        self.session.tables.append(table)
        return table

    def run_query(self, query: str) -> pd.DataFrame:
        return self.run_query_function(query)

