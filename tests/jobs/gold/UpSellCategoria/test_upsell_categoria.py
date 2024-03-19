from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from etl.jobs.gold.UpSellCategoria.functions import (
    prepara_produtos,
    # prepara_produtos_por_cliente,
    # pega_produtos_que_o_cliente_ja_comprou,
    # calcula_qtd_de_transacoes_por_categoria_por_cliente,
    # calcula_top_5_categorias_por_cliente,
    # prepara_vendas_por_produto,
    # calcula_top_10_produtos_por_categoria,
    # agrega_e_ordena_produtos,
    # junta_categorias_produtos_e_produtos_comprados,
    # pega_top_n_produtos_nunca_comprados,
    # calcula_relevancia_dos_produtos,
    # agrega_e_ordena_recomendacoes,
)


def test_prepara_produtos(spark_fixture):
    original = spark_fixture.createDataFrame(
        [
            ("P1", "C1", "value1"),
        ],
        StructType(
            [
                StructField("COD_ID_PRODUTO", StringType(), True),
                StructField("COD_ID_CATEGORIA_PRODUTO", StringType(), True),
                StructField("col1", StringType(), True),
            ]
        ),
    )

    transformed = prepara_produtos(original)

    expected_schema = StructType(
        [
            StructField("COD_ID_PRODUTO", StringType(), True),
            StructField("COD_ID_CATEGORIA_PRODUTO", StringType(), True),
        ]
    )

    assertSchemaEqual(transformed.schema, expected_schema)
