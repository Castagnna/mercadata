from etl.jobs.bronze.Vendas.functions import dumb_func, formata_dados


def test_dumb_func():
    assert dumb_func(1, 5) == 6


def test_dumb_func2():
    assert dumb_func(-1, 5) == 4
