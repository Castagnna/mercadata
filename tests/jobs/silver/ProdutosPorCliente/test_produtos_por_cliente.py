from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from etl.jobs.silver.ProdutosPorCliente.functions import agrega_produtos_por_cliente


def test_formata_dados(spark_fixture):
    original = spark_fixture.createDataFrame(
        [
            ("L1", "C1", "P1", 1.0),
            ("L1", None, "P1", 2.5),
            ("L1", "C1", "P1", 2.6),
            ("L1", "C2", "P2", 1.0),
            ("L1", "C2", "P22", 1.0),
            ("L2", "C1", "P1", 1.0),
            ("L2", None, "P1", 2.5),
            ("L2", "C1", "P1", 2.6),
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("COD_ID_CLIENTE", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("VAL_QUANTIDADE_KG", DoubleType(), True),
            ]
        ),
    )
    transformed = agrega_produtos_por_cliente(original)

    expected = spark_fixture.createDataFrame(
        [
            ("L1", "C1", "P1", 2, 3.6),
            ("L1", "C2", "P2", 1, 1.0),
            ("L1", "C2", "P22", 1, 1.0),
            ("L2", "C1", "P1", 2, 3.6),
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("COD_ID_CLIENTE", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("qtd_de_transacoes", LongType(), True),
                StructField("qtd_total_kg", DoubleType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
