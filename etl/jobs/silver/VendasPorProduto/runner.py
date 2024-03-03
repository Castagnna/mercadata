from os import path as P
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import agrega_vendas_por_produto


def transform(vendas: DataFrame) -> DataFrame:
    return agrega_vendas_por_produto(vendas)


def setup(
    env="prd",
    date_ref="today",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(deploy_mode, executor_memory_gb=26)

    # inputs
    vendas = spark.read.parquet(P.join(ROOT, env, "silver", "vendas_deduplicadas"))

    # output
    output = None
    if not dry_run:
        output = transform(vendas)

        (
            output.coalesce(1)
            .write.partitionBy("COD_ID_LOJA")
            .mode("overwrite")
            .parquet(P.join(ROOT, env, "silver", "vendas_por_produto"))
        )

        print(P.join(ROOT, env, "silver", "vendas_por_produto"))
