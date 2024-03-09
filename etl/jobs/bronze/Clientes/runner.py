from os import path as P
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import formata_dados


def transform(clientes: DataFrame) -> DataFrame:
    return formata_dados(clientes)


def setup(
    env="prd",
    date_ref="today",
    app_name="Spark Job",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(app_name, deploy_mode)

    # inputs
    clientes = spark.read.parquet(P.join(ROOT, env, "raw", "clientes"))

    # output
    output = None
    if not dry_run:
        output = transform(clientes)

        (
            output
            .write
            .mode("overwrite")
            .parquet(P.join(ROOT, env, "bronze", "clientes"))
        )

        print(P.join(ROOT, env, "bronze", "clientes"))
