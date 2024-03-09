from abc import ABC, abstractmethod
from datetime import datetime
from dateutil.parser import parse as dateparse
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
        self.__job_start_dttm = datetime.now()
        self.__date_ref = (
            self.job_start_dttm if date_ref == "today" else dateparse(date_ref)
        )
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
    
    @property
    def dry_run(self) -> str:
        return self.__dry_run

    @property
    def noop(self) -> str:
        return self.__noop
    
    @property
    def job_start_dttm(self) -> datetime:
        return self.__job_start_dttm
    
    @property
    def date_ref(self) -> datetime:
        return self.__date_ref

    @abstractmethod
    def load(self) -> dict:
        return {
            "dataframe": None,
        }

    @abstractmethod
    def transform(self, dataframe: DataFrame) -> DataFrame:
        return dataframe

    @staticmethod
    def write_noop(output):
        output.write.format("noop").mode("overwrite").save()

    @abstractmethod
    def write(self, output):
        if self.noop:
            self.write_noop(output)
        else:
            pass

    def run(self):
        loads = self.load()
        if not self.dry_run:
            output = self.transform(**loads)
            self.write(output)
