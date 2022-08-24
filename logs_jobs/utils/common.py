import os
from pyspark.sql import SparkSession
from logging import Logger
from delta import configure_spark_with_delta_pip


def get_spark_session() -> SparkSession:
    if is_running_in_databricks():
        print("Running on Databricks")
        spark = SparkSession.builder.getOrCreate()
        return spark
    else:
        print("Running locally")
        _builder = (
            SparkSession.builder.master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(_builder).getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        if is_running_with_databricks_connect(spark):
            print("Running on Databricks Connect")
        return spark

def get_data_root_path(spark) -> str:
    data_root_path = ""
    if is_running_in_databricks() or is_running_with_databricks_connect(spark):
        data_root_path = "/mnt/lake"
    else:
        user_home_path = os.path.expanduser('~')
        data_root_path = f"file:///{user_home_path}/data"
    return data_root_path

def get_logger(spark: SparkSession, job_name: str) -> Logger:
    log4j_logger = spark._jvm.org.apache.log4j
    return log4j_logger.LogManager.getLogger(job_name)

def get_dbutils(spark):
    if is_running_in_databricks() or is_running_with_databricks_connect(spark):
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    else:
        raise Exception("DBUtils not available in this environment, use databricks-connect")

def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def is_running_with_databricks_connect(spark: SparkSession) -> bool:
    try:
        return spark.conf.get("spark.databricks.service.client.enabled")
    except:
        return False

