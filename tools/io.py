from os import path as P
from tools.schemas.handler import get_schema

ROOTS = {
    "gcp": "gs://mercafacil/data",
    "os": "/home/castagna/voltz/notebooks/data/mercafacil",
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
):
    # TODO: def resolve_paths()
    ano_mes = f"{date_ref.year}{date_ref.month:02d}"
    file = f"{event}_{ano_mes}.csv.gz"
    paths = P.join(ROOTS[provider], env, layer, event, file)
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
