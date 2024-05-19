import logging
from typing import List, Callable
from functools import wraps
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="\t%(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def logend(start: datetime) -> None:
    logging.info(f"Spark session stopped")
    elapsed_time = (datetime.now() - start).total_seconds() / 60
    logging.info(f"Elapsed time: {elapsed_time:.1f} minutes")


def logmetrics(sum_cols: List[str] = []) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> DataFrame:
            df: DataFrame = func(*args, **kwargs).cache()
            metrics = df.agg(
                F.count("*").alias("count"),
                *[F.sum(col).alias(col) for col in sum_cols],
            )
            logging.info(f"\t{metrics.show(truncate=False)}")
            return df

        return wrapper

    return decorator
