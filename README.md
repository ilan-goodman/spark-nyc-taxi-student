# Spark + Airflow + Snowflake Lab (Student)

In this lab you’ll build a Spark pipeline that ingests NYC Yellow Taxi trip data, cleans and transforms it, computes aggregations, and writes results both to local Parquet and optionally to Snowflake. You’ll use the Spark UI to understand execution and improve performance, then orchestrate execution with Airflow.

What You’ll Do
1) Download 1–3 months of public taxi data and the taxi zone lookup. I have tested with the months 2023-02 and 2023-03 to ensure the data works well, but if you want an extra challenge, there is a schema mismatch with 2023-01 that can be solved in Spark.
2) Implement ETL logic in `jobs/nyc_taxi_etl_template.py` (look for TODOs).
3) Run locally with `spark-submit` and explore the Spark UI.
4) Orchestrate with Airflow via `dags/spark_nyc_taxi_etl_dag.py`.
5) Write to Snowflake (supports key‑pair auth). See `docs/SNOWFLAKE_SETUP.md`.
6) Optimize something (partitioning, caching, broadcast joins) and document its impact.

Other notes
- You may want to comment out the `spark.stop()` line while testing so you can view the Spark UI after the job is finished.
- You can run spark jobs in a script via `spark-submit`, in a spark REPL via `pyspark`, or in a Jupyter notebook. You can also trigger a Spark job as an Airflow task using the `SparkSubmitOperator`.
- Sample `spark-submit` command to run your Spark job from the command line:
```
spark-submit \
  --master 'local[4]' \
  --conf spark.sql.shuffle.partitions=8 \
  --packages net.snowflake:spark-snowflake_2.13:2.13.0-spark_3.3,net.snowflake:snowflake-jdbc:3.17.0 \
  jobs/nyc_taxi_etl_solution.py \
  --input_paths data/raw/yellow_tripdata_2023-02.parquet data/raw/yellow_tripdata_2023-03.parquet \
  --zone_csv data/raw/taxi_zone_lookup.csv \
  --curated_out data/curated/yellow \
  --aggregates_out data/aggregates \
  --write_snowflake
```

Deliverables
- Completed `jobs/nyc_taxi_etl_template.py`
- Spark UI screenshots (DAG visualization and a shuffle stage page)
- Optional Snowflake query results validating outputs
- A 1–2 page write-up describing your optimization and its effect

Setup (Local)
- Python 3.9–3.11, Spark 3.4+ (3.5 recommended), Airflow 2.7+ (2.9 recommended)
- Optional: copy `.env.example` to `.env` and adjust paths/settings.
- Install: `pip install -r requirements.txt`

Data
- NYC Yellow Taxi monthly Parquet
- Taxi zone lookup (CSV)
Use `scripts/download_tlc_data.py` to fetch files.

Snowflake (optional)
- We support key‑pair (RSA) authentication (no interactive MFA). Follow `docs/SNOWFLAKE_SETUP.md`.
- Do not commit any credentials or `.env` files to Git.
