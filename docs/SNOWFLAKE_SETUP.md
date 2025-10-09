# Configuring Snowflake (Key‑Pair Authentication)

Never commit credentials to Git. Use environment variables or Airflow Connections.

## Overview

We support Snowflake key‑pair authentication (non-interactive; works without MFA prompts). Your Snowflake user must have a PKCS#8 RSA public key associated with it. You will provide the matching private key locally via environment variables.

You’ll also need the Spark Snowflake connector and JDBC driver on Spark’s classpath.

## 1) Prepare your private key (PKCS#8) and base64-encode it

If you already have a PKCS#8 private key file (e.g., rsa_key_pkcs8.p8), base64 encode it:

- macOS/Linux:
  - `base64 -w0 rsa_key_pkcs8.p8 > rsa_key_pkcs8.b64`
- Windows PowerShell:
  - `[Convert]::ToBase64String([IO.File]::ReadAllBytes("rsa_key_pkcs8.p8")) | Out-File -NoNewline rsa_key_pkcs8.b64`

If your private key is PKCS#1, convert it to PKCS#8 first:
- `openssl pkcs8 -topk8 -inform PEM -outform PEM -in rsa_key.p8 -out rsa_key_pkcs8.p8`

Note your passphrase if your key is encrypted (recommended).

## 2) Set environment variables (local .env)

Copy `.env.example` to `.env` and set:

- WRITE_SNOWFLAKE=true
- SF_URL=<your_account>.snowflakecomputing.com
- SF_USER=<your_username>
- SF_DATABASE=<your_db>
- SF_SCHEMA=<your_schema>
- SF_WAREHOUSE=<your_warehouse>
- SF_PRIVATE_KEY_B64=<paste contents of rsa_key_pkcs8.b64>
- SF_PRIVATE_KEY_PASSPHRASE=<your_passphrase_if_any>
- SPARK_PACKAGES="net.snowflake:spark-snowflake_2.12:<version>,net.snowflake:snowflake-jdbc:<version>"

Export your .env into your shell:
- macOS/Linux: `set -a; source .env; set +a`
- Windows PowerShell: see the command in the student README or use a dotenv loader.

## 3) Run with spark-submit

Example:
```
spark-submit \
  --packages "$SPARK_PACKAGES" \
  jobs/nyc_taxi_etl_template.py \
  --input_paths data/raw/yellow_tripdata_2023-01.parquet \
  --zone_csv data/raw/taxi_zone_lookup.csv \
  --curated_out data/curated/yellow \
  --aggregates_out data/aggregates \
  --write_snowflake true \
  --sfURL "$SF_URL" --sfUser "$SF_USER" --sfDatabase "$SF_DATABASE" \
  --sfSchema "$SF_SCHEMA" --sfWarehouse "$SF_WAREHOUSE" \
  --sfPrivateKeyB64 "$SF_PRIVATE_KEY_B64" \
  --sfPrivateKeyPassphrase "$SF_PRIVATE_KEY_PASSPHRASE"
```

Inside the Spark job, the base64 string is decoded and passed to the connector as `pem_private_key` (and passphrase if set).

## 4) Airflow (optional)

Set the same env vars in the Airflow environment (or an Airflow Connection you read at runtime). The DAG reads:
- WRITE_SNOWFLAKE
- SF_URL, SF_USER, SF_DATABASE, SF_SCHEMA, SF_WAREHOUSE
- SF_PRIVATE_KEY_B64, SF_PRIVATE_KEY_PASSPHRASE
- SPARK_PACKAGES  (used by SparkSubmitOperator’s `packages`)

Then trigger the DAG.

## 5) Connector versions

Pick versions compatible with your Spark:
- net.snowflake:spark-snowflake_2.12:<connector-version>
- net.snowflake:snowflake-jdbc:<jdbc-version>

Refer to Snowflake’s compatibility matrix and set `SPARK_PACKAGES` accordingly.

## Alternative (SSO/external browser)

For interactive use, you can also set:
- SF_AUTHENTICATOR=externalbrowser
- and omit key/password

But for headless runs, prefer key-pair.
