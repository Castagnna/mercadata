from os import path as P
from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from .functions import deduplica_vendas


class Setup(BaseSetup):
    def __init__(self, env, date_ref, app_name, deploy_mode, dry_run, noop):
        super(Setup, self).__init__(
            env=env,
            app_name=app_name,
            deploy_mode=deploy_mode,
            dry_run=dry_run,
            noop=noop,
        )

    def load(self) -> dict:
        return {
            "vendas": self.spark.read.parquet(P.join(self.root, self.env, "bronze", "vendas"))
        }

    @staticmethod
    def transform(vendas: DataFrame) -> DataFrame:
        return deduplica_vendas(vendas)

    def write(self, output):
        # (
        #     output
        #     .write
        #     .partitionBy("COD_ID_LOJA", "DATA_DA_COMPRA")
        #     .mode("overwrite")
        #     .parquet(P.join(ROOT, env, "silver", "vendas_deduplicadas"))
        # )
        return super().write(output)
