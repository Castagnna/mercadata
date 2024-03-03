from os import path as P
from datetime import datetime
from dateutil import parser as dateparser
from pyspark.sql import DataFrame
from tools.spark import start_spark
from tools.schemas import schema_vendas
from etl.configs import ROOT
from .functions import formata_dados


def transform(vendas: DataFrame, file_date: str) -> DataFrame:
    return formata_dados(vendas, file_date)


def setup(
    env="prd",
    date_ref="today",
    deploy_mode="standalone",
    dry_run=False,
):
    spark = start_spark(deploy_mode)

    job_start_dttm = datetime.now()

    if date_ref == "today":
        date_ref = job_start_dttm
    else:
        date_ref = dateparser.parse(date_ref)

    ano_mes = f"{date_ref.year}{date_ref.month:02d}"

    file = f"vendas_{ano_mes}.csv.gz"

    # inputs
    vendas = spark.read.csv(
        P.join(ROOT, env, "raw", "vendas", file),
        schema=schema_vendas,
        sep=";",
        header=True,
    )

    # output
    output = None
    if not dry_run:
        output = transform(vendas, ano_mes)

        (
            output.write.partitionBy("DATA_DA_COMPRA", "DATA_PROCESSAMENTO")
            .mode("append")
            .parquet(P.join(ROOT, env, "bronze", "vendas"))
        )

        print(P.join(ROOT, env, "bronze", "vendas"))
