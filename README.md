# Logs Pipelines Spark Jobs package

```
.
│   README.md
│   requirements-dbx.txt
│   requirements-local.txt
│   deploy_package.sh
│   setup.py
│
└───notebooks
│   │   use_bronze_logs.py
│
└───logs_jobs
    │   __init__.py
    │   bronze_logs.py
    │   gold_city_requests.py
    │   gold_to_sql_database.py
    │   silver_logs_enriched.py
    │   gold_user_requests.py
    │
    └───utils
        │   common.py
```

### Setup
- Create a Virtual Environment `python3 -m venv .venv`
- Activate Virtual Envrionment: `source .venv/bin/activate`
- Install dependencies: enable only the local dev packages first and then run `pip install -r requirements-local.txt`

### Build Python Wheel from package
`python3 setup.py bdist_wheel --universal`

### Deploy Python Wheel to Databricks
Run the `deploy_package.sh`. It will upload the package to DBFS in `dbfs:/jobs/logs_jobs/logs_jobs-0.1-py2.py3-none-any.whl`.
(It requires Databricks CLI and Azure CLI installed in your Terminal)


## Local development and testing
### Setup
- Create a Virtual Environment `python3 -m venv .venv`
- Activate Virtual Envrionment: `source .venv/bin/activate`
- Install dependencies: enable only the local dev packages first and then run `pip install -r requirements-local.txt`

### Run the code locally
(Requires Logs Raw data into the following DBFS location `dbfs:/mnt/lake/raw/sample_logs/`, if you don't have it yet run the Notebook: `sample_notebooks/utils/nb_copy_data_to_raw`)
1. Download a sample of data from `dbfs:/mnt/lake/raw/`, a couple of files is enough, for that you can use the following command as example: 
   `databricks fs cp dbfs:/mnt/lake/raw/sample_logs/part-00000 ~/data/raw/sample_logs/part-00000`
2. Run `bronze_logs.py` as a normal Python file: `python3 ./logs_jobs/bronze_logs.py`. You can edit this file and run as many times as you need. The output of this job will be stored in `~/data/bronze/solution_package_db/logs/`.
3. Run `silver_logs_enriched.py` as a normal Python file: `python3 ./logs_jobs/silver_logs_enriched.py`. You can edit this file and run as many times as you need. The output of this job will be stored in `~/data/silver/solution_package_db/logs_enriched/`.
4. Run `gold_city_requests.py` as a normal Python file: `python3 ./logs_jobs/gold_city_requests.py`. You can edit this file and run as many times as you need. The output of this job will be stored in `~/data/gold/solution_package_db/city_requests/`.
5. Run `gold_user_requests.py` as a normal Python file: `python3 ./logs_jobs/gold_user_requests.py`. You can edit this file and run as many times as you need. The output of this job will be stored in `~/data/gold/solution_package_db/user_requests/`.
6. Create `./logs_jobs/.secrets.json` based on the template with the right username and password for the SQL database. Run `gold_to_sql_database.py` as a normal Python file: `python3 ./logs_jobs/gold_to_sql_database.py`. You can edit this file and run as many times as you need. The output of this job will be stored in the SQL database on `dbo` schema tables.


## Databricks Connect
Databricks Connect it's a tool to connect our local environment to a Databricks Cluster. In this way, we can develop code locally while using the Cluster.

This can be very useful in some cases like:
- We need to use DBUtils locally to use some of its functionalities, like listing folders or files in DBFS on Databrikcs.
- We can't download any samples of data to our machine because of company policy.
- We require many datasets that are present in a mounted Data Lake, and even in the Hive Metastore, and download sample of all of them would require lots of effort.

### Setup
- We use a cluster with Databricks Runtime 10.4 LTS, which uses Python 3.8, so we need to install it first. On MacOS: `brew install python@3.8`
- Create a Virtual Environment with this Python version:  `/usr/local/opt/python@3.8/bin/python3.8  -m venv .venv-dbx` 
- Activate Virtual Envrionment: `source .venv-dbx/bin/activate`
- Install dependencies: enable only the databricks-connect packages first and then run `pip install -r requirements-dbx.txt`
- Install databricks-connect for Databrics Runtime 10.4: `pip install -U "databricks-connect==10.4.*"`. (This packages conflicts with pyspark, so we need to uninstall it first in case we have it installed)
- Configure Databricks Connect to connect to your Cluster:
  - `databricks-connect configure`

### Run the code with Databricks Connect
Follow exactly the same steps as for running the code locally. Be aware that step 6 will fail since there is a limitation when trying to retrieve secrets from Secret Scopes on Databricks using Databricks Connect.

In this case, the data will be written in DBFS locations instead of on your local file system.


## Git Repos
You can also use the code of this Repo directly from Databricks. Go to `Git Repos` on Databricks and add it to the Workspace.
You can run the code on `./notebooks/use_bronze_logs.py` notebook to see an example on how to import and use the Python modules from notebooks.

Note: the same notebook `./notebooks/use_bronze_logs.py` can be used in case you rather use the Package installed on the cluster. If that's the case, just skip the first import cell.


