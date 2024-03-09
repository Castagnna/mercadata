from os import path as P
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import agrega_produtos_por_cliente


def transform(vendas: DataFrame) -> DataFrame:
    return agrega_produtos_por_cliente(vendas)


def setup(
    env="prd",
    date_ref="today",
    app_name="Spark Job",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(app_name, deploy_mode, executor_memory_gb=26)

    # inputs
    vendas = spark.read.parquet(P.join(ROOT, env, "silver", "vendas_deduplicadas"))

    # output
    output = None
    if not dry_run:
        output = transform(vendas)

        (
            output
            .write
            .partitionBy("COD_ID_LOJA")
            .mode("overwrite")
            .parquet(P.join(ROOT, env, "silver", "produtos_por_cliente"))
        )

        print(P.join(ROOT, env, "silver", "produtos_por_cliente"))
