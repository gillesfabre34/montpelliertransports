import pyspark.sql.functions as F
from pyspark.sql.column import Column
from rich import print


def logg(title: str, value=None) -> None:
    if value is None:
        print(f"\n----------\n{title}\n----------\n")
    else:
        print(f"\n----------\n{title}\n----------\n", value)


def is_number(col: Column) -> Column:
    return col.rlike("^[0-9]+$")


def sort_by_natural_order(col: Column) -> list:
    return [
        F.when(is_number(col), 0).otherwise(1),
        F.when(is_number(col), col.cast('int')),
        col
    ]
