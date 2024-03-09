from os import path as P
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import formata_dados


def transform(categorias: DataFrame) -> DataFrame:
    return formata_dados(categorias)


def setup(
    env="prd",
    date_ref="today",
    app_name="Spark Job",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(app_name, deploy_mode)

    # inputs
    categorias = spark.read.parquet(P.join(ROOT, env, "raw", "categorias"))

    # output
    output = None
    if not dry_run:
        output = transform(categorias)

        (
            output
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(P.join(ROOT, env, "bronze", "categorias"))
        )

        print(P.join(ROOT, env, "bronze", "categorias"))
