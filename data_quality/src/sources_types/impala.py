
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
        return f"to_timestamp({column_name}, '{format_date}')"



