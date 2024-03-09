import os
import sys
import pytest

sys.path.insert(1, os.getcwd())
from tools.spark import start_spark


@pytest.fixture(scope="session")
def spark_fixture():
    yield start_spark("Testing PySpark")
