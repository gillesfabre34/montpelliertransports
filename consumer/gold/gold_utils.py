from pyspark.sql import DataFrame

from consumer.silver.silver_utils import get_df_bronze, build_silver_dataframe
from consumer.utils.logg import logg


def get_df_silver() -> DataFrame:
    df_bronze = get_df_bronze()
    df_silver = build_silver_dataframe(df_bronze)
    logg("df_silver schema", df_silver.schema)
    return df_silver


if __name__ == "__main__":
    get_df_silver()
