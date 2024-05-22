from pyspark.sql import DataFrame
from jobs.setup import BaseSetup
from tools.readers import read_parquet
from tools.pipes import pipe, upartial
from .functions import (
    prepara_produtos,
    prepara_produtos_por_cliente,
    pega_produtos_que_o_cliente_ja_comprou,
    calcula_qtd_de_transacoes_por_categoria_por_cliente,
    calcula_top_5_categorias_por_cliente,
    prepara_vendas_por_produto,
    calcula_top_10_produtos_por_categoria,
    agrega_e_ordena_produtos,
    junta_categorias_produtos_e_produtos_comprados,
    pega_top_n_produtos_nunca_comprados,
    calcula_relevancia_dos_produtos,
    agrega_e_ordena_recomendacoes,
)


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
            "produtos": read_parquet(
                self.spark, self.env, "bronze", "produtos", dry_run=self.dry_run
            ),
            "produtos_por_cliente": read_parquet(
                self.spark,
                self.env,
                "silver",
                "produtos_por_cliente",
                dry_run=self.dry_run,
            ),
            "vendas_por_produto": read_parquet(
                self.spark,
                self.env,
                "silver",
                "vendas_por_produto",
                dry_run=self.dry_run,
            ),
            "top_n": 3,
        }

    @staticmethod
    def transform(
        produtos: DataFrame,
        produtos_por_cliente: DataFrame,
        vendas_por_produto: DataFrame,
        top_n: int,
    ) -> DataFrame:
        """Recomenda 5 produtos nunca comprados para o cliente.
        Lógica: em determinada loja, pega-se as 5 categorias mais compradas
        por cada cliente. Depois pega-se os produtos mais comprados por categoria.
        Então recomenda-se o primeiro produto mais comprado das 5 categorias que
        o cliente mais compra, excluindo os produtos que o cliente já comprou no passado.
        Caso não atinja 5 produtos por cliente, recomenda-se o próximo produto mais comprado
        da categoria até que chegue em 5 recomendacoes.
        """
        produtos = prepara_produtos(produtos).cache()

        produtos_por_cliente = prepara_produtos_por_cliente(
            produtos_por_cliente
        ).cache()

        produtos_que_o_cliente_ja_comprou = pega_produtos_que_o_cliente_ja_comprou(
            produtos_por_cliente
        )

        top_5_categorias_por_cliente = pipe(
            produtos_por_cliente,
            upartial(calcula_qtd_de_transacoes_por_categoria_por_cliente, produtos),
            calcula_top_5_categorias_por_cliente,
        )

        vendas_por_produto_com_categorias = prepara_vendas_por_produto(
            vendas_por_produto, produtos
        )

        top_10_produtos_por_categoria = pipe(
            vendas_por_produto_com_categorias,
            calcula_top_10_produtos_por_categoria,
            agrega_e_ordena_produtos,
        )

        top_produtos_das_top_categorias_do_cliente = (
            junta_categorias_produtos_e_produtos_comprados(
                top_5_categorias_por_cliente,
                top_10_produtos_por_categoria,
                produtos_que_o_cliente_ja_comprou,
            )
        )
        return pipe(
            top_produtos_das_top_categorias_do_cliente,
            upartial(pega_top_n_produtos_nunca_comprados, top_n),
            calcula_relevancia_dos_produtos,
            agrega_e_ordena_recomendacoes,
        )

    def write(self, output):
        # generation = job_start_dttm.strftime("%Y%m%d-%H%M%S")
        # (
        #     output.write.partitionBy("COD_ID_LOJA")
        #     .mode("overwrite")
        #     .parquet(
        #         P.join(
        #             ROOT, env, "gold", "top_5_produtos_para_o_cliente", generation
        #         )
        #     )
        # )
        return super().write(output)
