from pyspark.sql import DataFrame

from consumer.silver.silver_utils import get_df_bronze, build_silver_dataframe
from utils.tools import logg


def get_df_silver() -> DataFrame:
    df_bronze = get_df_bronze()
    logg("Getting df_silver...")
    return build_silver_dataframe(df_bronze)


if __name__ == "__main__":
    get_df_silver()
