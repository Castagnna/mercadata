from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agrega_produtos_por_cliente(vendas: DataFrame) -> DataFrame:
    return (
        vendas
        .where("COD_ID_CLIENTE IS NOT NULL")
        .groupBy(
            "COD_ID_LOJA",
            "COD_ID_CLIENTE",
            "COD_ID_PRODUTO",
        )
        .agg(
            F.count("*").alias("qtd_de_transacoes"),
            F.sum("VAL_QUANTIDADE_KG").alias("qtd_total_kg"),
        )
    )
