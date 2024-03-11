from pyspark.sql.types import *


vendas = StructType(
    [
        StructField("COD_ID_LOJA", StringType(), True),
        StructField("NUM_ANOMESDIA", StringType(), True),
        StructField("COD_ID_CLIENTE", StringType(), True),
        StructField("DES_TIPO_CLIENTE", StringType(), True),
        StructField("DES_SEXO_CLIENTE", StringType(), True),
        StructField("COD_ID_VENDA_UNICO", StringType(), True),
        StructField("COD_ID_PRODUTO", StringType(), True),
        StructField("VAL_VALOR_SEM_DESC", DoubleType(), True),
        StructField("VAL_VALOR_DESCONTO", DoubleType(), True),
        StructField("VAL_VALOR_COM_DESC", DoubleType(), True),
        StructField("VAL_QUANTIDADE_KG", DoubleType(), True),
    ]
)
