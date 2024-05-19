from os import path as P
from pyspark.sql import DataFrame
from tools.logging import logging
from tools.schemas.handler import get_schema
from tools.os import get_project_root

ROOTS = {
    "gcp": "gs://mercafacil/data",
    "s3": "s3a://mercafacil/data",
    "os": P.join(get_project_root(), "data"),
}


def read_csv(
    spark,
    env,
    layer,
    event,
    date_ref,
    provider="os",
    select_fields=None,
    drop_fields=None,
    custom_schema=None,
    dry_run=False,
) -> DataFrame:
    # TODO: def resolve_paths()
    paths = P.join(ROOTS[provider], env, layer, event)
    logging.info(f"{paths = }")

    schema = custom_schema or get_schema("events", event, select_fields, drop_fields)

    if dry_run:
        return

    return spark.read.csv(
        paths,
        schema=schema,
        sep=";",
        header=True,
    )


def read_parquet(
    spark,
    env,
    layer,
    event,
    date_ref=None,
    provider="os",
    select_fields=["*"],
    dry_run=False,
) -> DataFrame:
    # TODO: def resolve_paths()
    paths = P.join(ROOTS[provider], env, layer, event)
    logging.info(f"{paths = }")

    if dry_run:
        return

    return spark.read.parquet(paths).select(*select_fields)
