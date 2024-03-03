from os import path as P
from datetime import datetime
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import calcula_metricas_de_vendas_por_produto


def transform(produtos: DataFrame, vendas_por_produto: DataFrame) -> DataFrame:
    return calcula_metricas_de_vendas_por_produto(produtos, vendas_por_produto)


def setup(
    env="prd",
    date_ref="today",
    deploy_mode="standalone",
    dry_run=False,
):
    job_start_dttm = datetime.now()

    spark = start_spark(deploy_mode)

    # inputs
    produtos = (
        spark.read.parquet(P.join(ROOT, env, "bronze", "produtos"))
    )

    vendas_por_produto = spark.read.parquet(
        P.join(ROOT, env, "silver", "vendas_por_produto")
    )

    # output
    output = None
    if not dry_run:
        output = transform(produtos, vendas_por_produto)

        generation = job_start_dttm.strftime("%Y%m%d-%H%M%S")

        (
            output
            .write
            .mode("overwrite")
            .parquet(P.join(ROOT, env, "gold", "metricas_de_vendas_por_produto", generation))
        )
