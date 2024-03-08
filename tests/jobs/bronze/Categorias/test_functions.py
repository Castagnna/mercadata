import pytest
# from etl.jobs.bronze.Categorias.functions import dumb_func


def dumb_func(x, y):
    return x + y


def test_dumb_func():
    assert dumb_func(1, 5) == 6


def test_dumb_func2():
    assert dumb_func(-1, 5) == 4
