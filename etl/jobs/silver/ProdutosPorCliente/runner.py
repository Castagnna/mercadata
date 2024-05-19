from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from tools.readers import read_parquet
from .functions import agrega_produtos_por_cliente


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
            "vendas": read_parquet(
                self.spark,
                self.env,
                "silver",
                "vendas_deduplicadas",
                dry_run=self.dry_run,
            )
        }

    @staticmethod
    def transform(vendas: DataFrame) -> DataFrame:
        return agrega_produtos_por_cliente(vendas)

    def write(self, output):
        # (
        #     output
        #     .write
        #     .partitionBy("COD_ID_LOJA")
        #     .mode("overwrite")
        #     .parquet(P.join(ROOT, env, "silver", "produtos_por_cliente"))
        # )
        return super().write(output)
