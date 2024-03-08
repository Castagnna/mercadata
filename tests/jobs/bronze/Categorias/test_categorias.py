import os
import sys
import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual


# if not any("/mercadata/etl/.." in p for p in sys.path) and "__file__" in vars():
#     path = os.path.join(os.path.dirname(__file__).split("/tests")[0], "etl", os.pardir)
#     sys.path.append(path)

# sys.path.insert(1, os.getcwd())


from tools.spark import start_spark
from etl.jobs.bronze.Categorias.functions import formata_dados


@pytest.fixture
def spark_fixture():
    yield start_spark("Testing PySpark")


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
