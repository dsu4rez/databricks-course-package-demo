from pyspark.sql import DataFrame
from pyspark.sql.functions import col, add_months, max, current_timestamp

# ugly workaround so submodule import also works when running on a package
import os
import sys
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from utils.common import get_spark_session, get_data_root_path, get_logger


class GoldCityRequestsJob:

    def __init__(self) -> None:
        self.job_name = "gold_city_requests"
        self.spark = get_spark_session()
        self.logger = get_logger(self.spark, self.job_name)

    def run(self):
        self.logger.info(f"Running {self.job_name} job")
        spark = self.spark
        data_root_path = get_data_root_path(spark)
        database_name = "solution_package_db"
        input_table_name = "logs_enriched"
        output_table_name = "city_requests"
        silver_logs_path = f"{data_root_path}/silver/{database_name}/{input_table_name}/"
        gold_city_requests_path = f"{data_root_path}/gold/{database_name}/{output_table_name}/"

        self.logger.info(f"Transformation logic")
        self.logger.info(f"Reading data from {silver_logs_path}")
        silver_logs_df = spark.read.format("delta").load(silver_logs_path)
        transformed_logs_df = self.transform_city_requests(silver_logs_df)

        self.logger.info(f"Writing data into {gold_city_requests_path}")
        transformed_logs_df.write.format("delta").mode("overwrite").save(gold_city_requests_path)

        self.logger.info(f"Creating database in mestastore if not exists {database_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        self.logger.info(f"Creating table in mestastore if not exists {database_name}.{output_table_name}")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{output_table_name}
            USING DELTA
            LOCATION '{gold_city_requests_path}'
        """)

    def transform_city_requests(self, df: DataFrame, n_months: int = 2) -> DataFrame:
        date = (
            df.select(add_months(max("datetime").cast("date"), -n_months))
        ).collect()[0][0]

        city_requests_df = (
            df
            .select("ip_country_name", "ip_state_prov", "ip_city", "request_type")
            .where(col("datetime") > date)
            .groupBy("ip_country_name", "ip_state_prov", "ip_city", "request_type").count()
            .withColumnRenamed("count", "n_requests")
            .withColumn("_load_timestamp", current_timestamp())
        )
        return city_requests_df


def entrypoint():
    job = GoldCityRequestsJob()
    job.run()


if __name__ == "__main__":
    entrypoint()
