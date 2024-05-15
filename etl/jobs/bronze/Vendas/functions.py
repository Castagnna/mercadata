from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def formata_dados(vendas: DataFrame) -> DataFrame:
    return vendas.select(
        "COD_ID_LOJA",
        "COD_ID_CLIENTE",
        "DES_TIPO_CLIENTE",
        F.col("DES_SEXO_CLIENTE").cast("string"),
        "COD_ID_VENDA_UNICO",
        "COD_ID_PRODUTO",
        "VAL_VALOR_SEM_DESC",
        "VAL_VALOR_DESCONTO",
        "VAL_VALOR_COM_DESC",
        "VAL_QUANTIDADE_KG",
        F.to_date("DATA_DA_COMPRA", "yyyy-MM-dd").alias("DATA_DA_COMPRA"),
        F.to_date("DATA_PROCESSAMENTO", "yyyy-MM-dd").alias("DATA_PROCESSAMENTO"),
    )
