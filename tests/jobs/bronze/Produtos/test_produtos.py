from pyspark.sql import Column
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from etl.jobs.bronze.Produtos.functions import formata_dados


# TODO:
# def test_string_to_list(spark_fixture):


def test_formata_dados(spark_fixture):
    original = spark_fixture.createDataFrame(
        [
            (1, 2, "['1','2','3']", "CARNE", "KG", 1001001),
        ],
        StructType(
            [
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("COD_ID_CATEGORIA_PRODUTO", StringType(), True),
                StructField("ARR_CATEGORIAS_PRODUTO", StringType(), True),
                StructField("DES_PRODUTO", StringType(), True),
                StructField("DES_UNIDADE", StringType(), True),
                StructField("COD_CODIGO_BARRAS", StringType(), True),
            ]
        ),
    )
    transformed = formata_dados(original)

    expected = spark_fixture.createDataFrame(
        [
            ("1", "2", ["1", "2", "3"], "CARNE", "KG", "1001001"),
        ],
        StructType(
            [
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("COD_ID_CATEGORIA_PRODUTO", StringType(), True),
                StructField("ARR_CATEGORIAS_PRODUTO", ArrayType(StringType()), True),
                StructField("DES_PRODUTO", StringType(), True),
                StructField("DES_UNIDADE", StringType(), True),
                StructField("COD_CODIGO_BARRAS", StringType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
    assertSchemaEqual(transformed.schema, expected.schema)
