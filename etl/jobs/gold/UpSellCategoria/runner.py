from os import path as P
from datetime import datetime
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.jobs.common import pipe, partial
from etl.configs import ROOT
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

    produtos_por_cliente = prepara_produtos_por_cliente(produtos_por_cliente).cache()

    produtos_que_o_cliente_ja_comprou = pega_produtos_que_o_cliente_ja_comprou(
        produtos_por_cliente
    )

    top_5_categorias_por_cliente = pipe(
        produtos_por_cliente,
        partial(calcula_qtd_de_transacoes_por_categoria_por_cliente, produtos=produtos),
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
        partial(pega_top_n_produtos_nunca_comprados, n=top_n),
        calcula_relevancia_dos_produtos,
        agrega_e_ordena_recomendacoes,
    )


def setup(
    env="prd",
    date_ref="today",
    app_name="Spark Job",
    deploy_mode="standalone",
    dry_run=False,
    noop=False,
):
    job_start_dttm = datetime.now()

    spark = start_spark(app_name, deploy_mode)

    # inputs
    produtos = spark.read.parquet(P.join(ROOT, env, "bronze", "produtos"))

    produtos_por_cliente = spark.read.parquet(
        P.join(ROOT, env, "silver", "produtos_por_cliente")
    )

    vendas_por_produto = spark.read.parquet(
        P.join(ROOT, env, "silver", "vendas_por_produto")
    )

    # output
    output = None
    if not dry_run:
        output = transform(
            produtos,
            produtos_por_cliente,
            vendas_por_produto,
            top_n=3,
        )

        generation = job_start_dttm.strftime("%Y%m%d-%H%M%S")

        if not noop:
            (
                output.write.partitionBy("COD_ID_LOJA")
                .mode("overwrite")
                .parquet(
                    P.join(
                        ROOT, env, "gold", "top_5_produtos_para_o_cliente", generation
                    )
                )
            )
        else:
            output.write.format("noop").mode("overwrite").save()
