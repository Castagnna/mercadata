from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from etl.jobs.gold.MetricaVendas.functions import calcula_metricas_de_vendas_por_produto


def test_calcula_metricas_de_vendas_por_produto(spark_fixture):
    original_product = spark_fixture.createDataFrame(
        [
            ("P1", "PICANHA"),
            ("P2", "ALFACE"),
        ],
        StructType(
            [
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("DES_PRODUTO", StringType(), True),
            ]
        ),
    )
    original_sales_by_product = spark_fixture.createDataFrame(
        [
            ("L1", "P1", 2, 20.00, 200.00),
            ("L2", "P1", 3, 30.00, 300.00),
            ("L1", "P2", 2, 20.00, 200.00),
            ("L2", "P2", 3, 30.00, 300.00),
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("qtd_de_transacoes", LongType(), True),
                StructField("qtd_total_kg", DoubleType(), True),
                StructField("valor_total", DoubleType(), True),
            ]
        ),
    )

    transformed = calcula_metricas_de_vendas_por_produto(
        original_product, original_sales_by_product
    )

    expected = spark_fixture.createDataFrame(
        [
            ("P1", 5, 50.00, 500.00, "PICANHA"),
            ("P2", 5, 50.00, 500.00, "ALFACE"),
        ],
        StructType(
            [
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("qtd_de_transacoes", LongType(), True),
                StructField("qtd_total_kg", DoubleType(), True),
                StructField("valor_total", DoubleType(), True),
                StructField("DES_PRODUTO", StringType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
