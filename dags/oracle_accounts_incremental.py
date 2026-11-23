"""
### Oracle ACCOUNTS -> S3 (Iceberg) -> Snowflake

Daily incremental load of the Oracle `POV_ACCOUNTS` table into Snowflake.
Changed rows land in S3 as an Iceberg table first, then get merged into Snowflake.

The watermark (the latest `LAST_UPDATED_TS` we've loaded) lives in the asset state
store of the `pov_accounts` asset, under the key `watermark`. If it doesn't exist we
do a full load. To view, seed, or reset it: Airflow UI -> Assets -> `pov_accounts` ->
Asset State Store. Delete the `watermark` key to force a full reload.
"""

from datetime import datetime, timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLValueCheckOperator
from airflow.sdk import Asset, chain, dag, task

ORACLE_CONN_ID = "oracle_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"

# The Snowflake target table as an Airflow asset. Its state store holds the watermark.
POV_ACCOUNTS_ASSET = Asset(name="pov_accounts")
WATERMARK_KEY = "watermark"

ORACLE_TABLE = "POV_ACCOUNTS"
ICEBERG_TABLE = "landing.accounts"  # Glue database.table
SNOWFLAKE_STAGING_TABLE = "SANDBOX.VANSHTULI.POV_ACCOUNTS_STG"
SNOWFLAKE_TARGET_TABLE = "SANDBOX.VANSHTULI.POV_ACCOUNTS"

COLUMNS = [
    "ACCOUNT_ID",
    "CUSTOMER_NAME",
    "ACCOUNT_TYPE",
    "BALANCE",
    "CURRENCY",
    "STATUS",
    "BRANCH_CODE",
    "CREATED_TS",
    "LAST_UPDATED_TS",
]


def get_iceberg_table():
    """Open landing.accounts through the Glue catalog.

    pyiceberg doesn't know about Airflow connections, so we borrow the
    credentials from aws_default (on Astro these are short-lived credentials
    from the Deployment's IAM role) and hand them over.
    """
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from pyiceberg.catalog import load_catalog

    aws = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="glue")
    creds = aws.get_credentials()
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "client.region": aws.conn_region_name,
            "client.access-key-id": creds.access_key,
            "client.secret-access-key": creds.secret_key,
            "client.session-token": creds.token,
        },
    )
    return catalog.load_table(ICEBERG_TABLE)


@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    doc_md=__doc__,
    tags=["barclays-pov", "oracle", "iceberg", "snowflake"],
)
def oracle_accounts_incremental():

    @task(inlets=[POV_ACCOUNTS_ASSET])
    def extract_to_iceberg(run_id: str, asset_state_store=None) -> dict:
        import oracledb
        import pyarrow as pa
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from pyiceberg.expressions import EqualTo

        # Without this, oracledb hands back NUMBER(18,2) as a float and balances drift.
        oracledb.defaults.fetch_decimals = True

        watermark = asset_state_store[POV_ACCOUNTS_ASSET].get(WATERMARK_KEY, default=None)

        # LAST_UPDATED_TS is set when a row is created and every time it changes,
        # so one filter picks up both new and updated accounts.
        sql = f"SELECT {', '.join(COLUMNS)} FROM {ORACLE_TABLE}"
        params = {}
        if watermark:
            sql += " WHERE LAST_UPDATED_TS > :watermark"
            params["watermark"] = datetime.fromisoformat(watermark)
            print(f"Incremental load, changes since {watermark}")
        else:
            print("No watermark yet, doing a full load")

        rows = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_records(sql, parameters=params)
        print(f"Pulled {len(rows)} rows from Oracle")

        # Tag each row with the run_id. That's how the next task finds this batch,
        # and it's what makes a retry replace the batch instead of duplicating it.
        records = [dict(zip([c.lower() for c in COLUMNS], row), _batch_id=run_id) for row in rows]

        table = get_iceberg_table()
        batch = EqualTo("_batch_id", run_id)
        if records:
            data = pa.Table.from_pylist(records, schema=table.schema().as_arrow())
            table.overwrite(data, overwrite_filter=batch)
        else:
            table.delete(delete_filter=batch)

        # Next watermark = newest change we actually loaded. If nothing changed, keep the old one.
        last_updated = [r["last_updated_ts"] for r in records]
        new_watermark = max(last_updated).isoformat() if last_updated else watermark
        return {"rows": len(records), "new_watermark": new_watermark}

    @task
    def stage_in_snowflake(run_id: str) -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from pyiceberg.expressions import EqualTo
        from snowflake.connector.pandas_tools import write_pandas

        snowflake = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Read back just this run's batch from Iceberg.
        df = get_iceberg_table().scan(row_filter=EqualTo("_batch_id", run_id)).to_pandas()
        df.columns = df.columns.str.upper()

        if df.empty:
            snowflake.run(f"TRUNCATE TABLE {SNOWFLAKE_STAGING_TABLE}")
            print("Nothing changed since the last run")
            return

        # overwrite=True empties the staging table first, so it only ever holds this batch.
        database, schema, table = SNOWFLAKE_STAGING_TABLE.split(".")
        with snowflake.get_conn() as conn:
            write_pandas(
                conn,
                df,
                table_name=table,
                database=database,
                schema=schema,
                quote_identifiers=False,
                overwrite=True,
                use_logical_type=True,
            )
        print(f"Staged {len(df)} rows in {SNOWFLAKE_STAGING_TABLE}")

    # Upsert on ACCOUNT_ID: update accounts we already have, insert new ones.
    # Running it twice gives the same result, so reruns can't create duplicates.
    merge_into_snowflake = SQLExecuteQueryOperator(
        task_id="merge_into_snowflake",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            MERGE INTO {SNOWFLAKE_TARGET_TABLE} t
            USING {SNOWFLAKE_STAGING_TABLE} s
               ON t.ACCOUNT_ID = s.ACCOUNT_ID
            WHEN MATCHED THEN UPDATE SET
                CUSTOMER_NAME   = s.CUSTOMER_NAME,
                ACCOUNT_TYPE    = s.ACCOUNT_TYPE,
                BALANCE         = s.BALANCE,
                CURRENCY        = s.CURRENCY,
                STATUS          = s.STATUS,
                BRANCH_CODE     = s.BRANCH_CODE,
                LAST_UPDATED_TS = s.LAST_UPDATED_TS,
                _LOADED_AT      = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ACCOUNT_ID, CUSTOMER_NAME, ACCOUNT_TYPE, BALANCE, CURRENCY, STATUS,
                 BRANCH_CODE, CREATED_TS, LAST_UPDATED_TS, _LOADED_AT)
            VALUES
                (s.ACCOUNT_ID, s.CUSTOMER_NAME, s.ACCOUNT_TYPE, s.BALANCE, s.CURRENCY, s.STATUS,
                 s.BRANCH_CODE, s.CREATED_TS, s.LAST_UPDATED_TS, CURRENT_TIMESTAMP())
        """,
    )

    # Reconciliation, in two steps: count the accounts in Oracle, then check that
    # Snowflake has exactly the same number. A mismatch fails the run, so the
    # watermark doesn't move.
    count_oracle_rows = SQLExecuteQueryOperator(
        task_id="count_oracle_rows",
        conn_id=ORACLE_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {ORACLE_TABLE}",
    )

    check_snowflake_count = SQLValueCheckOperator(
        task_id="check_snowflake_count",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {SNOWFLAKE_TARGET_TABLE}",
        pass_value="{{ ti.xcom_pull(task_ids='count_oracle_rows')[0][0] }}",
    )

    # Last task on purpose: it only runs if everything above succeeded,
    # so a failed run never moves the watermark.
    @task(outlets=[POV_ACCOUNTS_ASSET])
    def update_watermark(extract: dict, asset_state_store=None) -> None:
        if extract["new_watermark"]:
            asset_state_store[POV_ACCOUNTS_ASSET].set(WATERMARK_KEY, extract["new_watermark"])
        print(f"Watermark is now {extract['new_watermark']}")

    extracted = extract_to_iceberg()
    chain(
        extracted,
        stage_in_snowflake(),
        merge_into_snowflake,
        count_oracle_rows,
        check_snowflake_count,
        update_watermark(extracted),
    )


oracle_accounts_incremental()
