from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from etl.jobs.bronze.Categorias.functions import formata_dados


def test_formata_dados(spark_fixture):
    sample_data = [
        {
            "COD_ID_CATEGORIA_PRODUTO": 1000,
            "DES_CATEGORIA": "CARNES",
            "COD_ID_CATEGORIA_PAI": 1001,
        },
    ]
    original = spark_fixture.createDataFrame(sample_data)
    transformed = formata_dados(original)

    expected = spark_fixture.createDataFrame(
        [("1000", "CARNES", "1001")],
        ["COD_ID_CATEGORIA_PRODUTO", "DES_CATEGORIA", "COD_ID_CATEGORIA_PAI"]
    )

    assertDataFrameEqual(transformed, expected)
    assertSchemaEqual(transformed.schema, expected.schema)
