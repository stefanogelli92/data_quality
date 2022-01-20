import pandas as pd


FISCALCODE_REGEX = r"(?:[A-Z][AEIOU][AEIOUX]|[B-DF-HJ-NP-TV-Z]{2}[A-Z]){2}(?:[\dLMNP-V]{2}(?:[A-EHLMPR-T](?:[04LQ][1-9MNP-V]|[15MR][\dLMNP-V]|[26NS][0-8LMNP-U])|[DHPS][37PT][0L]|[ACELMRT][37PT][01LM]|[AC-EHLMPR-T][26NS][9V])|(?:[02468LNQSU][048LQU]|[13579MPRTV][26NS])B[26NS][9V])(?:[A-MZ][1-9MNP-V][\dLMNP-V]{2}|[A-M][0L](?:[1-9MNP-V][\dLMNP-V]|[0L][1-9MNP-V]))[A-Z]"
EMAIL_REGEX = r"""(?:[a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`{|}~-]+)*|(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*)@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
CODICEATECO_REGEX = r"^\d{2}[.]{1}\d{2}[.]{1}[0-9A-Za-z]{1,2}$"


def _human_format(num):
    # Show float number in more readble format
    num = float('{:.2g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])


def _human_format_perc(num):
    # Show percent number in more readble format
    num = num * 100
    num = float('{:.2g}'.format(num))
    magnitude = 0
    if abs(num) <= 0.005:
        pass
    elif abs(num) <= 0.095:
        magnitude = 2
        num *= 100.0
    elif abs(num) <= 0.95:
        magnitude = 1
        num *= 10.0
    return '{}{}'.format('{:.0f}'.format(num), ['%', '‰', '‱'][magnitude])


def _clean_sql_filter(text):
    if text is not None:
        text = text.strip()
        if text.lower().startswith("where"):
            text = text[5:].strip()
        text = "(" + text + ")"
    return text


def _aggregate_sql_filter(filter_list):
    if isinstance(filter_list, list):
        filter_list = [f for f in filter_list if (f is not None) and (len(f) > 0)]
    elif isinstance(filter_list, str):
        filter_list = [filter_list]

    if (filter_list is None) or len(filter_list) == 0:
        sql_filter = ""
    else:
        sql_filter = """ WHERE """
        sql_filter += " AND ".join(filter_list)

    return sql_filter


def _output_column_to_sql(output_columns, table_tag=None):
    if output_columns is None:
        if table_tag is None:
            output_columns = "*"
        else:
            output_columns = f"{table_tag}.*"
    elif isinstance(output_columns, list):
        if table_tag is not None:
            output_columns = [f"{table_tag}.{col}" for col in output_columns]
        output_columns = ",".join(output_columns)
    return output_columns


def _query_limit(value):
    if isinstance(value, int):
        max_rows_sql = "LIMIT {}".format(value)
    else:
        max_rows_sql = ""
    return max_rows_sql


def _create_filter_columns_not_null(columns):
    if columns is not None:
        if isinstance(columns, str):
            columns = [columns]
        filter_sql = [f"({col} is not null) AND (cast({col} as string) != '')" for col in columns]
        filter_sql = " AND ".join(filter_sql)
    else:
        filter_sql = ""
    return filter_sql


def _create_filter_columns_null(columns):
    if columns is not None:
        if isinstance(columns, str):
            columns = [columns]
        filter_sql = [f"({col} is null) OR (cast({col} as string) = '')" for col in columns]
        filter_sql = " AND ".join(filter_sql)
    else:
        filter_sql = ""
    return filter_sql


def _clean_string_float_inf_columns_df(series):
    result = series.astype(str)
    result.replace({r'\.([0-9]*[1-9])(0+)$': r'\.\1',
                             r'(\.0+)$': ''}, regex=True, inplace=True)
    return result


