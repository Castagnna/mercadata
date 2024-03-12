from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from etl.jobs.silver.VendasPorProduto.functions import agrega_vendas_por_produto


def test_formata_dados(spark_fixture):
    original = spark_fixture.createDataFrame(
        [
            ("L1", "C1", "P1", 1.0, 10.55),
            ("L1", "C1", "P1", 2.0, 10.00),
            ("L1", "C2", "P1", 3.0, 10.00),
            ("L1", "C2", "P2", 1.0, 10.00),
            ("L1", "C3", "P2", 1.0, 10.00),
            ("L1", "C4", "P2", 1.0, 10.00),
            ("L2", "C4", "P2", 1.0, 10.00),
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("COD_ID_CLIENTE", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("VAL_QUANTIDADE_KG", DoubleType(), True),
                StructField("VAL_VALOR_COM_DESC", DoubleType(), True),
            ]
        ),
    )
    transformed = agrega_vendas_por_produto(original)

    expected = spark_fixture.createDataFrame(
        [
            ("L1", "P1", 3, 2, 6.0, 60.55),
            ("L1", "P2", 3, 3, 3.0, 30.00),
            ("L2", "P2", 1, 1, 1.0, 10.00),
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("qtd_de_transacoes", LongType(), True),
                StructField("qtd_de_transacoes_por_cliente_distintos", LongType(), True),
                StructField("qtd_total_kg", DoubleType(), True),
                StructField("valor_total", DoubleType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
