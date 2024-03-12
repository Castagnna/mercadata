from datetime import datetime
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, DateType
from etl.jobs.silver.DeduplicaVendas.functions import deduplica_vendas


def test_formata_dados(spark_fixture):
    original = spark_fixture.createDataFrame(
        [
            ("V1", "P1", datetime(2024, 1, 1), datetime(2024, 2, 1)),
            ("V1", "P1", datetime(2024, 1, 2), datetime(2024, 2, 1)),
            ("V1", "P1", datetime(2024, 1, 2), datetime(2024, 2, 2)),
        ],
        StructType(
            [
                StructField("COD_ID_VENDA_UNICO", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("DATA_PROCESSAMENTO", DateType(), True),
                StructField("DATA_DA_COMPRA", DateType(), True),
            ]
        ),
    )
    transformed = deduplica_vendas(original)

    expected = spark_fixture.createDataFrame(
        [
            ("V1", "P1", datetime(2024, 1, 2), datetime(2024, 2, 2)),
        ],
        StructType(
            [
                StructField("COD_ID_VENDA_UNICO", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("DATA_PROCESSAMENTO", DateType(), True),
                StructField("DATA_DA_COMPRA", DateType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
