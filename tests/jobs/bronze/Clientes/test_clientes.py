from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from etl.jobs.bronze.Clientes.functions import formata_dados


def test_formata_dados(spark_fixture):
    sample_data = [
        {
            "COD_ID_CLIENTE": 1000,
            "DES_TIPO_CLIENTE": "PF",
            "NOM_NOME": "JOAO MANOEL",
            "DES_SEXO_CLIENTE": "M",
            "DAT_DATA_NASCIMENTO": datetime(2000, 1, 1),
        },
    ]
    original = spark_fixture.createDataFrame(sample_data)
    transformed = formata_dados(original)

    expected = spark_fixture.createDataFrame(
        [
            ("1000", "PF", "JOAO MANOEL", "M", datetime(2000, 1, 1)),
        ],
        StructType(
            [
                StructField("COD_ID_CLIENTE", StringType(), True),
                StructField("DES_TIPO_CLIENTE", StringType(), True),
                StructField("NOM_NOME", StringType(), True),
                StructField("DES_SEXO_CLIENTE", StringType(), True),
                StructField("DAT_DATA_NASCIMENTO", DateType(), True),
            ]
        ),
    )

    assertDataFrameEqual(transformed, expected)
    assertSchemaEqual(transformed.schema, expected.schema)
