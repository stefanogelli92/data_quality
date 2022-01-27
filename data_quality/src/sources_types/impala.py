from datetime import datetime

from data_quality.src.sources_types.sources_type import SourceType


class Impala(SourceType):

    def __init__(self, run_query_function):
        self.run_query_function = run_query_function
        self.name = "impala"
        self.datetime_format_replace_dictionary = {
            "%Y": "yyyy",
            "%y": "yy",
            "%m": "MM",
            "%d": "dd",
            "%H": "HH",
            "%M": "mm",
            "%S": "SS"
        }

    def check_cast_datetime(self):
        result = True
        try:
            query = """
            SELECT 
                to_timestamp("01-02-2021", "yyyy-MM-dd") as a, 
                to_timestamp("02-02-2021", "dd-MM-yyyy") as b
            """
            df = self.run_query_function(query)
            result = result & (df["a"].isna().sum() == 1)
            result = result & (df["b"].isna().sum() == 0)
        except:
            result = False
        return result

    def cast_datetime_sql(self, column_name, format_date):
        if format_date is None:
            return column_name
        else:
            return f"to_timestamp({column_name}, '{format_date}')"

    def check_cast_float(self):
        result = True
        try:
            query = """
            SELECT 
                cast(3 as float) as a,
                cast('x' as float) as b
            """
            df = self.run_query_function(query)
            result = result & (df["a"].isna().sum() == 0)
            result = result & (df["b"].isna().sum() == 1)
        except:
            result = False
        return result

    def cast_float_sql(self, column_name):
        return f"cast({column_name} as float)"

    def check_regex(self):
        result = True
        try:
            query = """
            SELECT  
                regexp_like("2022-01-18", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$") as a,
                regexp_like("2022-01-182", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$", 'i') as b
            """
            df = self.run_query_function(query)
            result = result & (df["a"].sum() == 1)
            result = result & (df["b"].sum() == 0)
        except:
            result = False
        return result

    def match_regex(self, column_name: str, regex: str, case_sensitive: bool = True) -> str:
        if case_sensitive:
            return f"regexp_like({column_name}, '{regex}')"
        else:
            return f"regexp_like({column_name}, '{regex}', 'i')"

    def check_datetime_format_replace(self):
        try:
            query = """
            select to_timestamp("2021-01-01 11:00:00", "yyyy-MM-dd HH:mm:ss") as a
            """
            df = self.run_query_function(query)
            result = (df["a"] == "2021-01-01 11:00:00").sum() == 1
        except:
            result = False
        return result


