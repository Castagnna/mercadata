from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F


def deduplica_vendas(vendas: DataFrame) -> DataFrame:
    distincts_window = W.partitionBy(
        "COD_ID_LOJA", "COD_ID_VENDA_UNICO", "COD_ID_PRODUTO"
    ).orderBy(F.desc("DATA_PROCESSAMENTO"), F.desc("DATA_DA_COMPRA"))
    return (
        vendas.withColumn("row_number", F.row_number().over(distincts_window))
        .where("row_number = 1")
        .drop("row_number")
    )
