from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp, input_file_name, current_timestamp, when
)

# ugly workaround so submodule import also works when running on a package
import os
import sys
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from utils.common import get_spark_session, get_data_root_path, get_logger


class BronzeLogsJob:

    def __init__(self) -> None:
        self.job_name = "bronze_logs"
        self.spark = get_spark_session()
        self.logger = get_logger(self.spark, self.job_name)

    def run(self):
        self.logger.info(f"Running {self.job_name} job")
        spark = self.spark
        data_root_path = get_data_root_path(spark)
        database_name = "solution_package_db"
        output_table_name = "logs"
        raw_logs_path = f"{data_root_path}/raw/sample_logs/"
        bronze_logs_path = f"{data_root_path}/bronze/{database_name}/{output_table_name}/"

        self.logger.info(f"Transformation logic")
        self.logger.info(f"Reading data from {raw_logs_path}")
        raw_logs_df = spark.read.text(raw_logs_path)
        transformed_logs_df = self.transform_logs(raw_logs_df)

        self.logger.info(f"Writing data into {bronze_logs_path}")
        transformed_logs_df.write.format("delta").mode("overwrite").save(bronze_logs_path)

        self.logger.info(f"Creating database in mestastore if not exists {database_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        self.logger.info(f"Creating table in mestastore if not exists {database_name}.{output_table_name}")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{output_table_name}
            USING DELTA
            LOCATION '{bronze_logs_path}'
        """)

    def transform_logs(self, df: DataFrame) -> DataFrame:
        row_regex = r'(\d*\.\d*\.\d*\.\d*) (.*) (.*) \[(.*)\] \"(.*) (.*) (.*)\" (\d*) (\d*)'
        clean_df = (
            df.select(
                regexp_extract(col("value"), row_regex, 1).alias("ip"),
                regexp_extract(col("value"), row_regex, 3).alias("user"),
                to_timestamp(regexp_extract(col("value"), row_regex, 4), "dd/MMM/yyyy:HH:mm:ss Z").alias("datetime"),
                regexp_extract(col("value"), row_regex, 5).alias("request_type"),
                regexp_extract(col("value"), row_regex, 6).alias("request_endpoint"),
                regexp_extract(col("value"), row_regex, 7).alias("request_protocol"),
                regexp_extract(col("value"), row_regex, 8).cast("integer").alias("response_code"),
                regexp_extract(col("value"), row_regex, 9).cast("integer").alias("response_time"),
                input_file_name().alias("_source_file_path"),
                current_timestamp().alias("_load_timestamp")
            )
            .withColumn("user", when(col("user") == '-', None).otherwise(col("user")))
            .drop_duplicates()
        )
        return clean_df


def entrypoint():
    job = BronzeLogsJob()
    job.run()


if __name__ == "__main__":
    entrypoint()
