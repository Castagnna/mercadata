from pyspark.sql import Window as W
from pyspark.sql import functions as F
from tools.readers import ROOTS


def split_skewed_data(
    skewed_data, partition_by_columns, desired_rows_per_output_file=1e3, provider="os"
) -> None:
    """split skewed data with same number of rows per partition

    Created based on stackoverflow.com/questions/53037124/partitioning-a-large-skewed-dataset-in-s3-with-sparks-partitionby-method/65433689#65433689
    and powered by @github.com/Castagnna
    """
    w = W.partitionBy(partition_by_columns)
    total_rows_per_partition = F.count("*").over(w)

    partition_balanced_data = skewed_data.select(
        "*",
        (F.rand() * total_rows_per_partition / desired_rows_per_output_file)
        .cast("int")
        .alias("repartition_seed"),
    )

    (
        partition_balanced_data.write.partitionBy(
            *partition_by_columns, "repartition_seed"
        )
        .mode("append")
        .parquet(ROOTS[provider])
    )
