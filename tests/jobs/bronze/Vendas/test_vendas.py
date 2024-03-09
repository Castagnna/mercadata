from datetime import datetime
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
from etl.jobs.bronze.Vendas.functions import formata_dados


def test_formata_dados(spark_fixture):
    sample_data = [
        {
            "COD_ID_LOJA": "LOJA_1",
            "NUM_ANOMESDIA": "20240101",
            "COD_ID_CLIENTE": "1001",
            "DES_TIPO_CLIENTE": "PJ",
            "DES_SEXO_CLIENTE": "",
            "COD_ID_VENDA_UNICO": "V1234",
            "COD_ID_PRODUTO": "0001",
            "VAL_VALOR_SEM_DESC": 10.55,
            "VAL_VALOR_DESCONTO": 2.00,
            "VAL_VALOR_COM_DESC": 8.55,
            "VAL_QUANTIDADE_KG": 1.653,
        },
    ]
    original = spark_fixture.createDataFrame(sample_data)
    transformed = formata_dados(original, "202402")

    expected = spark_fixture.createDataFrame(
        [
            (
                "LOJA_1",
                datetime(2024, 1, 1),
                "1001",
                "PJ",
                "",
                "V1234",
                "0001",
                10.55,
                2.00,
                8.55,
                1.653,
                datetime(2024, 2, 1),
            )
        ],
        StructType(
            [
                StructField("COD_ID_LOJA", StringType(), True),
                StructField("DATA_DA_COMPRA", DateType(), True),
                StructField("COD_ID_CLIENTE", StringType(), True),
                StructField("DES_TIPO_CLIENTE", StringType(), True),
                StructField("DES_SEXO_CLIENTE", StringType(), True),
                StructField("COD_ID_VENDA_UNICO", StringType(), True),
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("VAL_VALOR_SEM_DESC", DoubleType(), True),
                StructField("VAL_VALOR_DESCONTO", DoubleType(), True),
                StructField("VAL_VALOR_COM_DESC", DoubleType(), True),
                StructField("VAL_QUANTIDADE_KG", DoubleType(), True),
                StructField("DATA_PROCESSAMENTO", DateType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
    assertSchemaEqual(transformed.schema, expected.schema)
