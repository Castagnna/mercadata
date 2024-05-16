from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F


def prepara_produtos(
    produtos: DataFrame,
) -> DataFrame:
    return produtos.select("COD_ID_PRODUTO", "COD_ID_CATEGORIA_PRODUTO")


def prepara_produtos_por_cliente(
    produtos_por_cliente: DataFrame,
) -> DataFrame:
    return produtos_por_cliente.select(
        "COD_ID_LOJA",
        "COD_ID_CLIENTE",
        "COD_ID_PRODUTO",
        "qtd_de_transacoes",
        "qtd_total_kg",
    )


def pega_produtos_que_o_cliente_ja_comprou(
    produtos_por_cliente: DataFrame,
) -> DataFrame:
    return produtos_por_cliente.groupBy("COD_ID_LOJA", "COD_ID_CLIENTE").agg(
        F.collect_set("COD_ID_PRODUTO").alias("produtos_ja_comprados")
    )


def calcula_qtd_de_transacoes_por_categoria_por_cliente(
    produtos_por_cliente: DataFrame,
    produtos: DataFrame,
) -> DataFrame:
    return (
        produtos_por_cliente.join(produtos, "COD_ID_PRODUTO", "inner")
        .groupBy("COD_ID_LOJA", "COD_ID_CLIENTE", "COD_ID_CATEGORIA_PRODUTO")
        .agg(
            F.sum("qtd_de_transacoes").alias("qtd_de_transacoes"),
            F.sum("qtd_total_kg").alias("qtd_total_kg"),
        )
    )


def calcula_top_5_categorias_por_cliente(
    transacoes_por_categoria_por_cliente: DataFrame,
) -> DataFrame:
    rank_window = W.partitionBy("COD_ID_LOJA", "COD_ID_CLIENTE").orderBy(
        F.desc("qtd_de_transacoes"), F.desc("qtd_total_kg")
    )
    return (
        transacoes_por_categoria_por_cliente.withColumn(
            "rk_categoria", F.row_number().over(rank_window)
        )
        .where("rk_categoria <= 5")
        .select(
            "COD_ID_LOJA", "COD_ID_CLIENTE", "COD_ID_CATEGORIA_PRODUTO", "rk_categoria"
        )
    )


def prepara_vendas_por_produto(
    vendas_por_produto: DataFrame,
    produtos: DataFrame,
) -> DataFrame:
    return vendas_por_produto.select(
        "COD_ID_LOJA",
        "COD_ID_PRODUTO",
        "qtd_de_transacoes_por_cliente_distintos",
        "qtd_de_transacoes",
        "qtd_total_kg",
    ).join(produtos, "COD_ID_PRODUTO", "left")


def calcula_top_10_produtos_por_categoria(
    vendas_por_produto_com_categorias: DataFrame,
) -> DataFrame:
    rank_window = W.partitionBy("COD_ID_LOJA", "COD_ID_CATEGORIA_PRODUTO").orderBy(
        F.desc("qtd_de_transacoes_por_cliente_distintos"),
        F.desc("qtd_de_transacoes"),
        F.desc("qtd_total_kg"),
    )
    return (
        vendas_por_produto_com_categorias.withColumn(
            "rk_produto", F.row_number().over(rank_window)
        )
        .where("rk_produto <= 10")
        .select(
            "COD_ID_LOJA", "COD_ID_CATEGORIA_PRODUTO", "COD_ID_PRODUTO", "rk_produto"
        )
    )


def agrega_e_ordena_produtos(
    top_10_produtos_por_categoria: DataFrame,
) -> DataFrame:
    return (
        top_10_produtos_por_categoria.groupBy("COD_ID_LOJA", "COD_ID_CATEGORIA_PRODUTO")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("rk_produto", "COD_ID_PRODUTO")), asc=True
            ).alias("top_10_produtos")
        )
        .withColumn("top_10_produtos", F.col("top_10_produtos.COD_ID_PRODUTO"))
    )


def junta_categorias_produtos_e_produtos_comprados(
    top_5_categorias_por_cliente: DataFrame,
    top_10_produtos_por_categoria: DataFrame,
    produtos_que_o_cliente_ja_comprou: DataFrame,
) -> DataFrame:
    return top_5_categorias_por_cliente.join(
        top_10_produtos_por_categoria,
        ["COD_ID_LOJA", "COD_ID_CATEGORIA_PRODUTO"],
        "left",
    ).join(produtos_que_o_cliente_ja_comprou, ["COD_ID_LOJA", "COD_ID_CLIENTE"], "left")


def pega_top_n_produtos_nunca_comprados(
    top_produtos_das_top_categorias_do_cliente: DataFrame,
    n: int,
) -> DataFrame:
    nunca_comprados = F.array_except("top_10_produtos", "produtos_ja_comprados")
    return top_produtos_das_top_categorias_do_cliente.select(
        "COD_ID_LOJA",
        "COD_ID_CLIENTE",
        "rk_categoria",
        F.slice(nunca_comprados, 1, n).alias("top_n_produtos_da_categoria"),
    )


def calcula_relevancia_dos_produtos(
    top_n_produtos_nunca_comprados: DataFrame,
) -> DataFrame:
    """Criterios de relevancia
    primeiro: categoria mais relevante;
    segundo: produto mais relevante dentro da categoria;
    """
    return top_n_produtos_nunca_comprados.select(
        "COD_ID_LOJA",
        "COD_ID_CLIENTE",
        "rk_categoria",
        F.posexplode("top_n_produtos_da_categoria").alias("pos", "produto"),
    ).withColumn("relevancia", F.array("pos", "rk_categoria"))


def agrega_e_ordena_recomendacoes(
    produtos_relevantes_para_o_cliente: DataFrame,
) -> DataFrame:
    return (
        produtos_relevantes_para_o_cliente.groupBy("COD_ID_LOJA", "COD_ID_CLIENTE")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("relevancia", "produto")), asc=True
            ).alias("top_5_produtos")
        )
        .withColumn("top_5_produtos", F.slice("top_5_produtos.produto", 1, 5))
    )
