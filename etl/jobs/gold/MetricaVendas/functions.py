from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calcula_metricas_de_vendas_por_produto(
    produtos: DataFrame, vendas_por_produto: DataFrame
) -> DataFrame:
    produtos = produtos.select("COD_ID_PRODUTO", "DES_PRODUTO")
    return (
        vendas_por_produto.groupBy(
            "COD_ID_PRODUTO",
        )
        .agg(
            F.sum("qtd_de_transacoes").alias("qtd_de_transacoes"),
            F.sum("qtd_total_kg").alias("qtd_total_kg"),
            F.sum("valor_total").alias("valor_total"),
        )
        .join(produtos, "COD_ID_PRODUTO", "left")
    )
