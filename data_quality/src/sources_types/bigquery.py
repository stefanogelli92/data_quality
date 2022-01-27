from datetime import datetime

from data_quality.src.sources_types.sources_type import SourceType


class BigQuery(SourceType):

    def __init__(self, run_query_function):
        self.run_query_function = run_query_function
        self.name = "bigquery"
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
                safe_cast("01-02-2021" as timestamp FORMAT "yyyy-MM-dd") as a, 
                safe_cast("02-02-2021" as timestamp FORMAT "dd-MM-yyyy") as b
            """
            df = self.run_query_function(query)
            result = result & (df["a"].isna().sum() == 1)
            result = result & (df["b"].isna().sum() == 0)
        except:
            result = False
        return result

    def cast_datetime_sql(self, column_name, format_date):
        if format_date is None:
            return f"safe_cast({column_name} as timestamp)"
        else:
            return f"safe_cast({column_name} as timestamp FORMAT '{format_date}')"

    def check_cast_float(self):
        result = True
        try:
            query = """
            SELECT
                safe_cast(3 as float64) as a,
                safe_cast('-' as float64) as b
            """
            df = self.run_query_function(query)
            result = result & (df["a"].isna().sum() == 0)
            result = result & (df["b"].isna().sum() == 1)
        except:
            result = False
        return result

    def cast_float_sql(self, column_name):
        return f"safe_cast({column_name} as float64)"

    def check_regex(self):
        result = True
        try:
            query = """
               SELECT  
                REGEXP_CONTAINS("2022-01-18", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$") as a,
                REGEXP_CONTAINS("2022-01-182", "(?i)^[0-9]{4}-[0-9]{2}-[0-9]{2}$") as b
               """
            df = self.run_query_function(query)
            result = result & (df["a"].sum() == 1)
            result = result & (df["b"].sum() == 0)
        except:
            result = False
        return result

    def match_regex(self, column_name: str, regex: str, case_sensitive: bool = True) -> str:
        if case_sensitive:
            return f"REGEXP_CONTAINS({column_name}, '{regex}')"
        else:
            return f"REGEXP_CONTAINS({column_name}, '(?i){regex}')"

    def check_datetime_format_replace(self):
        try:
            query = """
            select safe_cast("2021-01-01 11:00:00" as timestamp FORMAT "yyyy-MM-dd HH24:MI:ss") as a
            """
            df = self.run_query_function(query)
            result = (df["a"] == "2021-01-01 11:00:00").sum() == 1
        except:
            result = False
        return result

