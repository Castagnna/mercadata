from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from tools.io import read_csv
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
        return {
            "vendas": read_csv(
                self.spark,
                self.env,
                "raw",
                "vendas",
                self.date_ref,
                dry_run=self.dry_run,
            ),
        }

    @staticmethod
    def transform(vendas: DataFrame) -> DataFrame:
        return formata_dados(vendas)

    def write(self, output):
        # (
        #     output.write.partitionBy("DATA_DA_COMPRA", "DATA_PROCESSAMENTO")
        #     .mode("append")
        #     .parquet(P.join(ROOT, env, "bronze", "vendas"))
        # )
        return super().write(output)
