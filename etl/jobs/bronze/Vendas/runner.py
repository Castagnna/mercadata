from os import path as P
from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from tools.schemas import schema_vendas
from .functions import formata_dados

class Setup(BaseSetup):
    def __init__(self, env, date_ref, app_name, deploy_mode, dry_run, noop):
        super(Setup, self).__init__(
            env=env,
            date_ref=date_ref,
            app_name=app_name,
            deploy_mode=deploy_mode,
            dry_run=dry_run,
            noop=noop,
        )

    def load(self) -> dict:
        ano_mes = f"{self.date_ref.year}{self.date_ref.month:02d}"
        file = f"vendas_{ano_mes}.csv.gz"
        return {
            "vendas": self.spark.read.csv(
                P.join(self.root, self.env, "raw", "vendas", file),
                schema=schema_vendas,
                sep=";",
                header=True,
            ),
            "file_date": ano_mes,
        }

    @staticmethod
    def transform(vendas: DataFrame, file_date: str) -> DataFrame:
        return formata_dados(vendas, file_date)

    def write(self, output):
        # (
        #     output.write.partitionBy("DATA_DA_COMPRA", "DATA_PROCESSAMENTO")
        #     .mode("append")
        #     .parquet(P.join(ROOT, env, "bronze", "vendas"))
        # )
        return super().write(output)
