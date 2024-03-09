from os import path as P
from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from .functions import calcula_metricas_de_vendas_por_produto


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
            "produtos": self.spark.read.parquet(
                P.join(self.root, self.env, "bronze", "produtos")
            ),
            "vendas_por_produto": self.spark.read.parquet(
                P.join(self.root, self.env, "silver", "vendas_por_produto")
            ),
        }

    @staticmethod
    def transform(produtos: DataFrame, vendas_por_produto: DataFrame) -> DataFrame:
        return calcula_metricas_de_vendas_por_produto(produtos, vendas_por_produto)

    def write(self, output):
        # generation = job_start_dttm.strftime("%Y%m%d-%H%M%S")
        # (
        #     output
        #     .write
        #     .mode("overwrite")
        #     .parquet(P.join(ROOT, env, "gold", "metricas_de_vendas_por_produto", generation))
        # )
        return super().write(output)
