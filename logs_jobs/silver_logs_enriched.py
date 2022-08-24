import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, udf

# ugly workaround so submodule import also works when running on a package
import os
import sys
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from utils.common import get_spark_session, get_data_root_path, get_logger 


class SilverLogsEnrichedJob:

    def __init__(self) -> None:
        self.job_name = "silver_logs_enriched"
        self.spark = get_spark_session()
        self.logger = get_logger(self.spark, self.job_name)

    def run(self):
        self.logger.info(f"Running {self.job_name} job")
        spark = self.spark
        data_root_path = get_data_root_path(spark)
        database_name = "solution_package_db"
        input_table_name = "logs"
        output_table_name = "logs_enriched"
        bronze_logs_path = f"{data_root_path}/bronze/{database_name}/{input_table_name}/"
        silver_logs_path = f"{data_root_path}/silver/{database_name}/{output_table_name}/"

        self.logger.info(f"Transformation logic")
        self.logger.info(f"Reading data from {bronze_logs_path}")
        bronze_logs_df = spark.read.format("delta").load(bronze_logs_path)
        transformed_logs_df = self.transform_logs_enriched(bronze_logs_df)

        self.logger.info(f"Writing data into {silver_logs_path}")
        transformed_logs_df.write.format("delta").mode("overwrite").save(silver_logs_path)

        self.logger.info(f"Creating database in mestastore if not exists {database_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        self.logger.info(f"Creating table in mestastore if not exists {database_name}.{output_table_name}")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{output_table_name}
            USING DELTA
            LOCATION '{silver_logs_path}'
        """)

    def transform_logs_enriched(self, df: DataFrame) -> DataFrame:
        distinct_ip_df = df.select("ip").distinct().where(col("ip").isNotNull())

        def get_ip_info(ip) -> dict:
            url = f"http://api.db-ip.com/v2/free/{ip}"
            ip_info = requests.get(url).json()
            return ip_info

        ip_info_return_type = """
            struct<
                ipAddress: STRING, 
                continentCode: STRING, 
                continentName: STRING, 
                countryCode: STRING,
                countryName: STRING,
                stateProvCode: STRING,
                stateProv: STRING,
                city: STRING
            >
        """
        fetch_ip_info_udf = udf(lambda ip: get_ip_info(ip), returnType=ip_info_return_type)

        enriched_distinct_ip_df = (
            distinct_ip_df
            .withColumn("ip_info", fetch_ip_info_udf(col("ip")))
            .select(
                "ip",
                col("ip_info.continentCode").alias("ip_continent_code"),
                col("ip_info.continentName").alias("ip_continent_name"),
                col("ip_info.countryCode").alias("ip_country_code"),
                col("ip_info.countryName").alias("ip_country_name"),
                col("ip_info.stateProvCode").alias("ip_state_prov_code"),
                col("ip_info.stateProv").alias("ip_state_prov"),
                col("ip_info.city").alias("ip_city")
            )
        )
        enriched_df = (
            df
            .join(enriched_distinct_ip_df, on="ip")
            .select(
                "datetime", 
                "request_type", 
                "request_endpoint", 
                "request_protocol", 
                "response_code", 
                "response_time", 
                "user", 
                "ip", 
                "ip_continent_code", 
                "ip_continent_name", 
                "ip_country_code", 
                "ip_country_name", 
                "ip_state_prov_code", 
                "ip_state_prov", 
                "ip_city",
                current_timestamp().alias("_load_timestamp")
            )
        )
        return enriched_df


def entrypoint():
    job = SilverLogsEnrichedJob()
    job.run()


if __name__ == "__main__":
    entrypoint()
