from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agrega_vendas_por_produto(vendas: DataFrame) -> DataFrame:
    return vendas.groupBy(
        "COD_ID_LOJA",
        "COD_ID_PRODUTO",
    ).agg(
        F.count("*").alias("qtd_de_transacoes"),
        F.countDistinct("COD_ID_CLIENTE").alias(
            "qtd_de_transacoes_por_cliente_distintos"
        ),
        F.sum("VAL_QUANTIDADE_KG").alias("qtd_total_kg"),
        F.sum(F.col("VAL_VALOR_COM_DESC") * F.col("VAL_QUANTIDADE_KG")).alias(
            "valor_total"
        ),
    )
