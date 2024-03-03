from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


@F.udf(ArrayType(StringType()))
def string_to_list(col: str) -> Column:
    return col[2:-2].split("','")


def formata_dados(produtos: DataFrame) -> DataFrame:
    return produtos.select(
        F.col("COD_ID_PRODUTO").cast(StringType()),
        F.col("COD_ID_CATEGORIA_PRODUTO").cast(StringType()),
        string_to_list("ARR_CATEGORIAS_PRODUTO").alias("ARR_CATEGORIAS_PRODUTO"),
        F.col("DES_PRODUTO").cast(StringType()),
        F.col("DES_UNIDADE").cast(StringType()),
        F.col("COD_CODIGO_BARRAS").cast(StringType()),
    )
