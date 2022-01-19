from abc import ABC, abstractmethod


class SourceType(ABC):

    def __init__(self, run_query_function):
        self.run_query_function = run_query_function
        self.name = None

    @abstractmethod
    def check_cast_datetime(self) -> bool:
        pass

    @abstractmethod
    def cast_datetime_sql(self, column_name: str, format_date: str) -> str:
        pass

    @abstractmethod
    def check_cast_float(self) -> bool:
        pass

    @abstractmethod
    def cast_float_sql(self, column_name: str) -> str:
        pass

    @abstractmethod
    def check_regex(self) -> bool:
        pass

    @abstractmethod
    def match_regex(self, column_name: str, regex: str, case_sensitive: bool = True) -> str:
        pass
