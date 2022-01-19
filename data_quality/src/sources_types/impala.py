
from data_quality.src.sources_types.sources_type import SourceType


class Impala(SourceType):

    def __init__(self, run_query_function):
        self.run_query_function = run_query_function
        self.name = "impala"

    def check_cast_datetime(self):
        result = True
        try:
            query = """
            SELECT 
                to_timestamp("01-02-2021", "yyyy-MM-dd") as A, 
                to_timestamp("02-02-2021", "dd-MM-yyyy") as B
            """
            df = self.run_query_function(query)
            result = result & (df["A"].isna().sum() == 1)
            result = result & (df["B"].isna().sum() == 0)
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
                cast(3 as float) as A,
                cast('x' as float) as B
            """
            df = self.run_query_function(query)
            result = result & (df["A"].isna().sum() == 0)
            result = result & (df["B"].isna().sum() == 1)
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
                regexp_like("2022-01-18", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$") as A,
                regexp_like("2022-01-182", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$", 'i') as B
            """
            df = self.run_query_function(query)
            result = result & (df["A"].sum() == 1)
            result = result & (df["B"].sum() == 0)
        except:
            result = False
        return result

    def match_regex(self, column_name: str, regex: str, case_sensitive: bool = True) -> str:
        if case_sensitive:
            return f"regexp_like({column_name}, '{regex}')"
        else:
            return f"regexp_like({column_name}, '{regex}', 'i')"



