__all__ = ["start_spark"]

from pyspark.sql import SparkSession
from os.path import join
from tools.os import get_total_memory, get_num_cores, choose_storage_device

OVERHEAD_FRACTION = 0.1
CORES_PER_EXECUTOR = 5
PARALLELISM_PER_CORE = 2

def start_spark(
    app_name="Spark Job",
    deploy_mode="standalone",
    local_dir=None,
    executor_instances=None,
    executor_memory_gb=20,
    overhead_memory=OVERHEAD_FRACTION,
    executor_cores=CORES_PER_EXECUTOR,
    parallelism=PARALLELISM_PER_CORE,
    set_checkpoint_dir=False,
    extra_conf={},
    print_conf=False,
):
    print(f"{app_name = }")
    print(f"{deploy_mode = }")
    if deploy_mode == "cluster":
        # trust the cluster configuration except unless explicit values are given

        # TODO accept overrides
        conf = {
            # "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
            **extra_conf,
        }

    elif deploy_mode == "standalone":
        # Mostly reverse-engieneered from http://spark-configuration.luminousmen.com/

        if not local_dir:
            mount_points = choose_storage_device(return_all_possible=True)
            local_dir = ",".join(
                join(mount_point, "spark") for mount_point in mount_points
            )

        ncores = get_num_cores()
        memgibs = get_total_memory()

        executor_cores = min(executor_cores, ncores - 1)  # cannot exceed total cores -1

        if not executor_instances:
            executor_instances = ncores // executor_cores

        if not executor_memory_gb:
            executor_memory_gb = int(
                ((memgibs / executor_instances) - 1) * (1 - OVERHEAD_FRACTION)
            )

        if overhead_memory < 1:
            overhead_memory = int(executor_memory_gb * OVERHEAD_FRACTION * 1024)

        executor_instances = max(
            executor_instances - 1, 1
        )  # Leaving 1 for application manager

        default_parallelism = parallelism * executor_cores * executor_instances

        conf = {
            "spark.master": "local[*]",
            "spark.local.dir": local_dir,
            "spark.default.parallelism": default_parallelism,
            "spark.memory.fraction": "0.8",
            "spark.dynamicAllocation.enabled": "false",
            "spark.speculation": "false",
            "spark.rdd.compress": "true",
            "spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures": "5",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.ui.port": "4040",
            "spark.executor.cores": executor_cores,
            "spark.executor.memory": f"{executor_memory_gb}g",
            "spark.executor.memoryOverhead": f"{overhead_memory}m",
            "spark.executor.instances": executor_instances,
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark",
            "spark.driver.cores": executor_cores,
            "spark.driver.memory": f"{executor_memory_gb}g",
            "spark.driver.maxResultSize": f"{executor_memory_gb}g",
            "spark.driver.memoryOverhead": f"{overhead_memory}m",
            "spark.driver.port": "11000",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.parquet.mergeSchema": "false",
            "spark.sql.shuffle.partitions": "32",
            "spark.sql.caseSensitive": "true",
            "spark.shuffle.compress": "true",
            "spark.shuffle.spill.compress": "true",
            "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.multipart.threshold": "2097152000",
            "spark.hadoop.fs.s3a.multipart.size": "1048576000",
            "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
            "spark.hadoop.fs.s3a.committer.name": "magic",
            "spark.hadoop.fs.s3a.connection.maximum": "200",
            "spark.hadoop.fs.s3a.connection.timeout": "3600000",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
            "spark.io.compression.codec": "zstd",
            **extra_conf,
        }

    else:
        conf = {**extra_conf}

    builder = SparkSession.builder

    for key, value in conf.items():
        if value:
            builder = builder.config(key, value)

    spark = builder.enableHiveSupport().appName(app_name).getOrCreate()

    if set_checkpoint_dir:
        # Set a checkpoint dir to use DataFrame method .checkpoint()
        spark.sparkContext.setCheckpointDir("hdfs:///checkpoint")

    sparkConf = spark.sparkContext.getConf().getAll()
    if print_conf:
        print("\n".join(f"{k}:\t{v}" for k, v in sparkConf if len(v) < 500))

    return spark
