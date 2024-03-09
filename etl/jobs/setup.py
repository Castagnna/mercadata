from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from tools.spark import start_spark
from etl.configs import ROOT


class BaseSetup(ABC):
    def __init__(
        self,
        env="prd",
        app_name="Spark Job",
        deploy_mode="standalone",
        date_ref="today",
        dry_run=False,
        noop=False,
        kwargs={},
    ) -> object:
        self.__env = env
        self.__spark = start_spark(app_name, deploy_mode)
        self.__app_name = app_name
        self.__deploy_mode = deploy_mode
        self.__date_ref = date_ref
        self.__dry_run = dry_run
        self.__noop = noop
        self.__kwargs = kwargs
        self.__root = ROOT

    @property
    def env(self) -> str:
        return self.__env

    @property
    def spark(self):
        return self.__spark

    @property
    def root(self) -> str:
        return self.__root

    @abstractmethod
    def load(self) -> dict:
        return {
            "dataframe": None,
        }

    @abstractmethod
    def transform(self, dataframe: DataFrame) -> DataFrame:
        return dataframe

    @abstractmethod
    def write(self, output):
        output.write.format("noop").mode("overwrite").save()

    def run(self):
        loads = self.load()
        output = self.transform(**loads)
        self.write(output)
