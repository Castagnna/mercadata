from os import path as P
from pyspark.sql import Window as W
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from tools.schemas.handler import get_schema
from tools.os import get_project_root

ROOTS = {
    "gcp": "gs://mercafacil/data",
    "s3": "s3a://mercafacil/data",
    "os": P.join(get_project_root(), "data"),
}


def split_skewed_data(
    skewed_data, partition_by_columns, desired_rows_per_output_file=1e3, provider="os"
) -> None:
    """split skewed data with same number of rows per partition

    Created based on stackoverflow.com/questions/53037124/partitioning-a-large-skewed-dataset-in-s3-with-sparks-partitionby-method/65433689#65433689
    and powered by @github.com/Castagnna
    """
    w = W.partitionBy(partition_by_columns)
    total_rows_per_partition = F.count("*").over(w)

    partition_balanced_data = skewed_data.select(
        "*",
        (F.rand() * total_rows_per_partition / desired_rows_per_output_file)
        .cast("int")
        .alias("repartition_seed"),
    )

    (
        partition_balanced_data.write.partitionBy(
            *partition_by_columns, "repartition_seed"
        )
        .mode("append")
        .parquet(ROOTS[provider])
    )


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
    print(f"{paths = }")

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
    print(f"{paths = }")

    if dry_run:
        return

    return spark.read.parquet(paths).select(*select_fields)
