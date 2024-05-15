from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType


def formata_dados(clientes: DataFrame) -> DataFrame:
    return clientes.select(
        F.col("COD_ID_CLIENTE").cast(IntegerType()).cast(StringType()),
        F.col("DES_TIPO_CLIENTE").cast(StringType()),
        F.col("NOM_NOME").cast(StringType()),
        F.col("DES_SEXO_CLIENTE").cast(StringType()),
        F.col("DAT_DATA_NASCIMENTO").cast(DateType()),
    )
