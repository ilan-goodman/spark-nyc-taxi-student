from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {"owner": "airflow", "depends_on_past": False, "retries": 0}

MONTHS = ["2023-01", "2023-02"]  # TODO: adjust scale
DATA_RAW = os.environ.get("DATA_RAW", "data/raw")
CURATED_OUT = os.environ.get("CURATED_OUT", "data/curated/yellow")
AGG_OUT = os.environ.get("AGG_OUT", "data/aggregates")
ZONE_CSV = os.path.join(DATA_RAW, "taxi_zone_lookup.csv")
SNOWFLAKE_WRITE = os.environ.get("WRITE_SNOWFLAKE", "false").lower()

# Optional: pass connector packages via env (recommended)
SPARK_PACKAGES = os.environ.get("SPARK_PACKAGES", None)

def download_callable(**context):
    from scripts.download_tlc_data import main as dl_main
    import sys
    argv = ["--months", *MONTHS, "--outdir", DATA_RAW, "--zones"]
    sys.argv = ["download_tlc_data.py"] + argv
    dl_main()

with DAG(
    dag_id="spark_nyc_taxi_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "nyc-taxi", "etl"],
) as dag:

    download = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_callable,
    )

    SPARK_APP = os.environ.get("SPARK_APP", "jobs/nyc_taxi_etl_template.py")
    INPUT_PATHS = [os.path.join(DATA_RAW, f"yellow_tripdata_{m}.parquet") for m in MONTHS]

    # Snowflake-related env (students set these locally; do not commit credentials)
    sf_url = os.environ.get("SF_URL", "")
    sf_user = os.environ.get("SF_USER", "")
    sf_password = os.environ.get("SF_PASSWORD", "")  # optional; key-pair preferred
    sf_db = os.environ.get("SF_DATABASE", "")
    sf_schema = os.environ.get("SF_SCHEMA", "")
    sf_wh = os.environ.get("SF_WAREHOUSE", "")
    sf_auth = os.environ.get("SF_AUTHENTICATOR", "snowflake")
    sf_pk_b64 = os.environ.get("SF_PRIVATE_KEY_B64", "")
    sf_pk_pass = os.environ.get("SF_PRIVATE_KEY_PASSPHRASE", "")

    etl = SparkSubmitOperator(
        task_id="spark_etl",
        application=SPARK_APP,
        application_args=[
            "--input_paths", *INPUT_PATHS,
            "--zone_csv", ZONE_CSV,
            "--curated_out", CURATED_OUT,
            "--aggregates_out", AGG_OUT,
            "--write_snowflake", SNOWFLAKE_WRITE,
            "--sfURL", sf_url,
            "--sfUser", sf_user,
            "--sfPassword", sf_password,
            "--sfDatabase", sf_db,
            "--sfSchema", sf_schema,
            "--sfWarehouse", sf_wh,
            "--sfAuthenticator", sf_auth,
            "--sfPrivateKeyB64", sf_pk_b64,
            "--sfPrivateKeyPassphrase", sf_pk_pass,
            "--shuffle_partitions", os.environ.get("SHUFFLE_PARTITIONS", "200"),
        ],
        packages=SPARK_PACKAGES,
        conf={"spark.sql.shuffle.partitions": os.environ.get("SHUFFLE_PARTITIONS", "200")},
    )

    download >> etl
