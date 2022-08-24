from setuptools import find_packages, setup

setup(
    name="logs_jobs",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    entry_points = {
        "console_scripts": [
            "bronze_logs = logs_jobs.bronze_logs:entrypoint",
            "silver_logs_enriched = logs_jobs.silver_logs_enriched:entrypoint",
            "gold_city_requests = logs_jobs.gold_city_requests:entrypoint",
            "gold_user_requests = logs_jobs.gold_user_requests:entrypoint",
            "gold_to_sql_database = logs_jobs.gold_to_sql_database:entrypoint"
    ]},
    version="0.1",
    description="",
    author="David Suarez Esteban",
)