from os import path as P
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT
from .functions import formata_dados


def transform(produtos: DataFrame) -> DataFrame:
    return formata_dados(produtos)


def setup(
    env="prd",
    date_ref="today",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(deploy_mode)

    # inputs
    produtos = spark.read.parquet(P.join(ROOT, env, "raw", "produtos"))

    # output
    output = None
    if not dry_run:
        output = transform(produtos)

        (
            output.write.mode("overwrite").parquet(
                P.join(ROOT, env, "bronze", "produtos")
            )
        )

        print(P.join(ROOT, env, "bronze", "produtos"))
