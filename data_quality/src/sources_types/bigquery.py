
from data_quality.src.sources_types.sources_type import SourceType


class BigQuery(SourceType):

    def __init__(self, run_query_function):
        self.run_query_function = run_query_function
        self.name = "bigquery"

    def check_cast_datetime(self):
        result = True
        try:
            query = """
            SELECT 
                safe_cast("01-02-2021" as timestamp FORMAT "yyyy-MM-dd") as A, 
                safe_cast("02-02-2021" as timestamp FORMAT "dd-MM-yyyy") as B
            """
            df = self.run_query_function(query)
            result = result & (df["A"].isna().sum() == 1)
            result = result & (df["B"].isna().sum() == 0)
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
                safe_cast(3 as float64) as A,
                safe_cast('-' as float64) as B
            """
            df = self.run_query_function(query)
            result = result & (df["A"].isna().sum() == 0)
            result = result & (df["B"].isna().sum() == 1)
        except:
            result = False
        return result

    def cast_float_sql(self, column_name):
        return f"safe_cast({column_name} as float64)"



