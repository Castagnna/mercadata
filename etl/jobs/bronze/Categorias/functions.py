from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def formata_dados(categorias: DataFrame) -> DataFrame:
    return categorias.select(
        F.col("COD_ID_CATEGORIA_PRODUTO").cast(StringType()),
        F.col("DES_CATEGORIA").cast(StringType()),
        F.col("COD_ID_CATEGORIA_PAI").cast(IntegerType()).cast(StringType()),
    )

def dumb_func(x, y):
    return x + y
