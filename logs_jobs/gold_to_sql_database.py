from typing import Tuple
import pyodbc
import os
import json
import sqlalchemy
from sqlalchemy.engine import URL 

# ugly workaround so submodule import also works when running on a package
import os
import sys
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from utils.common import (
    get_spark_session, get_data_root_path, get_logger, get_dbutils, 
    is_running_in_databricks, is_running_with_databricks_connect
)


class SqlUserRequestsJob:

    def __init__(self) -> None:
        self.job_name = "gold_user_requests"
        self.spark = get_spark_session()
        self.logger = get_logger(self.spark, self.job_name)

    def run(self):
        self.logger.info(f"Running {self.job_name} job")
        spark = self.spark
        data_root_path = get_data_root_path(spark)
        database_name = "solution_package_db"
        gold_root_path = f"{data_root_path}/gold/{database_name}"
        sql_server = "dbxlab-mssqlserver.database.windows.net"
        sql_database_name = "dbxlab-db"

        sql_user, sql_password = self._get_sql_database_credentials()
        sql_connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={sql_server};"
            f"Database={sql_database_name};"
            f"Uid={sql_user};"
            f"Pwd={sql_password};"
            f"Connection Timeout=30;"
        )
        connection_url = URL.create(
            "mssql+pyodbc", 
            query={"odbc_connect": sql_connection_string}
        )
        engine = sqlalchemy.create_engine(connection_url)

        self.write_gold_table_to_sql_database(gold_root_path, "user_requests", engine)
        self.write_gold_table_to_sql_database(gold_root_path, "city_requests", engine)

    def write_gold_table_to_sql_database(self, gold_root_path: str, table_name: str, engine: sqlalchemy.engine):
        self.logger.info(f"Reading data from {gold_root_path}/{table_name}")
        pandas_df = self.spark.read.format("delta").load(f"{gold_root_path}/{table_name}").drop("_load_timestamp").toPandas()
        output_sql_table_name = f"solution_package_db_{table_name}"
        self.logger.info(f"Writing data into SQL database --> dbo.{output_sql_table_name}")
        pandas_df.to_sql(
            con=engine, 
            name=output_sql_table_name, 
            schema="dbo", 
            if_exists="replace", 
            index=False
        )
    
    def _get_sql_database_credentials(self) -> Tuple[str, str]:
        if is_running_in_databricks() or is_running_with_databricks_connect(self.spark):
            dbutils = get_dbutils(self.spark)
            sql_user = dbutils.secrets.get("dbxlab-keyvault", "mssqlserver-user")
            sql_password = dbutils.secrets.get("dbxlab-keyvault", "mssqlserver-password")
        else:
            current_path = "/".join(__file__.split("/")[:-1])
            f = open(f"{current_path}/.secrets.json")
            secrets = json.load(f)
            sql_user = secrets["sql_user"]
            sql_password = secrets["sql_password"]
        return sql_user, sql_password


def entrypoint():
    job = SqlUserRequestsJob()
    job.run()


if __name__ == "__main__":
    entrypoint()
