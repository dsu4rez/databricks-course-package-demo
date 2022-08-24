# Databricks notebook source
import sys
sys.path.append("/Workspace/Repos/master/databricks-course-package")

# COMMAND ----------

from logs_jobs.utils.common import get_data_root_path
from logs_jobs.bronze_logs import BronzeLogsJob

# COMMAND ----------

df = spark.read.text(f"{get_data_root_path(spark)}/raw/sample_logs/")
display(df)

# COMMAND ----------

job = BronzeLogsJob()

# COMMAND ----------

output_df = job.transform_logs(df)
display(output_df)
