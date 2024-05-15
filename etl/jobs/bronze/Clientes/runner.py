from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from tools.io import read_parquet
from .functions import formata_dados


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
            "clientes": read_parquet(
                self.spark, self.env, "raw", "clientes", dry_run=self.dry_run
            )
        }

    @staticmethod
    def transform(clientes: DataFrame) -> DataFrame:
        return formata_dados(clientes)

    def write(self, output):
        # (
        #     output
        #     .write
        #     .mode("overwrite")
        #     .parquet(P.join(ROOT, env, "bronze", "clientes"))
        # )
        return super().write(output)
