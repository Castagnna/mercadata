import os
import sys
import pytest


if not any("/mercadata/etl/.." in p for p in sys.path) and "__file__" in vars():
    path = os.path.join(
        os.path.dirname(__file__).split("/tests")[0], "etl", os.pardir
    )
    sys.path.append(path)

from etl.jobs.bronze.Categorias.functions import dumb_func, formata_dados


def test_dumb_func():
    assert dumb_func(1, 5) == 6


def test_dumb_func2():
    assert dumb_func(-1, 5) == 4
