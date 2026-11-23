"""
## Oracle ACCOUNTS -> S3 (Iceberg) -> Snowflake: daily incremental load

Every day at 02:00:

1. **get_watermark**: read the last watermark from the Airflow Variable `oracle_accounts_watermark`.
   If the Variable doesn't exist, this is the first run, so load everything (full load).
2. **extract_to_iceberg**: pull new or changed rows from Oracle and write them to the
   Iceberg table `landing.accounts` on S3.
3. **load_to_snowflake**: copy those rows into a staging table, then MERGE them into
   `POV_ACCOUNTS`: insert new accounts and update changed ones.
4. **reconcile_counts**: Oracle and Snowflake must have the same number of accounts, otherwise fail.
5. **update_watermark**: save the new watermark. This is the last task, so it only runs when
   everything above succeeded. A failed run leaves the watermark untouched.

Re-running is safe: every step replaces or merges its data, so it never appends duplicates.
"""

from datetime import datetime, timedelta

import pendulum
from airflow.sdk import Variable, dag, get_current_context, task

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
ORACLE_CONN_ID = "oracle_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"

WATERMARK_VARIABLE = "oracle_accounts_watermark"  # Airflow Variable holding the last loaded timestamp

ORACLE_TABLE = "POV_ACCOUNTS"
ICEBERG_TABLE = "landing.accounts"  # <glue database>.<table>
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
    """Open landing.accounts through the AWS Glue catalog, using the Airflow AWS connection."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from pyiceberg.catalog import load_catalog

    aws = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="glue")
    creds = aws.get_credentials()  # temporary credentials from the Astro workload identity
    settings = {
        "type": "glue",
        "client.region": aws.conn_region_name,
        "client.access-key-id": creds.access_key,
        "client.secret-access-key": creds.secret_key,
    }
    if creds.token:
        settings["client.session-token"] = creds.token
    return load_catalog("glue", **settings).load_table(ICEBERG_TABLE)


@dag(
    dag_id="oracle_accounts_incremental",
    doc_md=__doc__,
    schedule="0 2 * * *",  # every day at 02:00
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,  # never run two loads at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["barclays-pov", "oracle", "iceberg", "snowflake"],
)
def oracle_accounts_incremental():

    @task
    def get_watermark() -> dict:
        """Work out which rows to load in this run."""
        from airflow.providers.oracle.hooks.oracle import OracleHook

        # Last successful watermark, or None on the very first run.
        watermark = Variable.get(WATERMARK_VARIABLE, default=None)

        # Note Oracle's current time now, *before* extracting. If the run succeeds, this
        # becomes the next watermark, so nothing that changes during the run is missed.
        run_started_at = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_first(
            "SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP) FROM DUAL"
        )[0]

        print("FULL LOAD (no watermark yet)" if watermark is None else f"INCREMENTAL since {watermark}")
        return {"watermark": watermark, "run_started_at": run_started_at.isoformat()}

    @task
    def extract_to_iceberg(window: dict) -> int:
        """Copy new or changed Oracle rows into the Iceberg landing table on S3."""
        import oracledb
        import pyarrow as pa
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from pyiceberg.expressions import EqualTo

        oracledb.defaults.fetch_decimals = True  # keep BALANCE exact (Decimal, not float)

        sql = f"SELECT {', '.join(COLUMNS)} FROM {ORACLE_TABLE}"
        params = {}
        if window["watermark"]:
            # New rows are caught by CREATED_TS, changed rows by LAST_UPDATED_TS.
            sql += " WHERE CREATED_TS > :wm OR LAST_UPDATED_TS > :wm"
            params["wm"] = datetime.fromisoformat(window["watermark"])

        rows = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_records(sql, parameters=params)
        print(f"Extracted {len(rows)} rows from Oracle")

        # Tag every row with the Airflow run_id so this run's rows can be found again.
        run_id = get_current_context()["run_id"]
        records = [{**dict(zip([c.lower() for c in COLUMNS], row)), "_batch_id": run_id} for row in rows]

        table = get_iceberg_table()
        data = pa.Table.from_pylist(records, schema=table.schema().as_arrow())

        # overwrite + filter = "replace this run's rows". A retry therefore doesn't
        # add a second copy of the same batch.
        if records:
            table.overwrite(data, overwrite_filter=EqualTo("_batch_id", run_id))
        else:
            table.delete(delete_filter=EqualTo("_batch_id", run_id))  # nothing new; clear any earlier attempt
        return len(rows)

    @task
    def load_to_snowflake(row_count: int) -> None:
        """Iceberg batch -> Snowflake staging table -> MERGE into POV_ACCOUNTS."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from pyiceberg.expressions import EqualTo
        from snowflake.connector.pandas_tools import write_pandas

        run_id = get_current_context()["run_id"]
        snowflake = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # 1) Read back only this run's rows from Iceberg.
        df = get_iceberg_table().scan(row_filter=EqualTo("_batch_id", run_id)).to_pandas()
        df.columns = [c.upper() for c in df.columns]
        if df.empty:
            print("Nothing changed since the last run.")
            return

        # 2) Replace the staging table with this batch (overwrite=True truncates it first).
        database, schema, table = SNOWFLAKE_STAGING_TABLE.split(".")
        conn = snowflake.get_conn()
        try:
            write_pandas(
                conn, df, table_name=table, database=database, schema=schema,
                quote_identifiers=False, overwrite=True, use_logical_type=True,
            )
        finally:
            conn.close()

        # 3) Upsert staging into the target, keyed on ACCOUNT_ID: no duplicates, even on re-runs.
        snowflake.run(f"""
            MERGE INTO {SNOWFLAKE_TARGET_TABLE} AS tgt
            USING {SNOWFLAKE_STAGING_TABLE} AS src
               ON tgt.ACCOUNT_ID = src.ACCOUNT_ID
            WHEN MATCHED THEN UPDATE SET
                CUSTOMER_NAME   = src.CUSTOMER_NAME,
                ACCOUNT_TYPE    = src.ACCOUNT_TYPE,
                BALANCE         = src.BALANCE,
                CURRENCY        = src.CURRENCY,
                STATUS          = src.STATUS,
                BRANCH_CODE     = src.BRANCH_CODE,
                CREATED_TS      = src.CREATED_TS,
                LAST_UPDATED_TS = src.LAST_UPDATED_TS,
                _LOADED_AT      = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ACCOUNT_ID, CUSTOMER_NAME, ACCOUNT_TYPE, BALANCE, CURRENCY, STATUS,
                 BRANCH_CODE, CREATED_TS, LAST_UPDATED_TS, _LOADED_AT)
            VALUES
                (src.ACCOUNT_ID, src.CUSTOMER_NAME, src.ACCOUNT_TYPE, src.BALANCE, src.CURRENCY, src.STATUS,
                 src.BRANCH_CODE, src.CREATED_TS, src.LAST_UPDATED_TS, CURRENT_TIMESTAMP())
        """)
        print(f"Merged {len(df)} rows into {SNOWFLAKE_TARGET_TABLE}")

    @task
    def reconcile_counts(window: dict) -> None:
        """Fail the run if Oracle and Snowflake don't hold the same number of accounts."""
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # Count accounts created up to the moment this run started, on both sides,
        # so accounts created while the run was going don't cause a false alarm.
        cutoff = datetime.fromisoformat(window["run_started_at"])

        oracle_count = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_first(
            f"SELECT COUNT(*) FROM {ORACLE_TABLE} WHERE CREATED_TS <= :cutoff", parameters={"cutoff": cutoff}
        )[0]
        snowflake_count = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_first(
            f"SELECT COUNT(*) FROM {SNOWFLAKE_TARGET_TABLE} WHERE CREATED_TS <= %(cutoff)s", parameters={"cutoff": cutoff}
        )[0]

        print(f"Oracle: {oracle_count} rows | Snowflake: {snowflake_count} rows")
        if oracle_count != snowflake_count:
            raise ValueError(f"Row counts don't match: Oracle={oracle_count}, Snowflake={snowflake_count}")

    @task
    def update_watermark(window: dict) -> None:
        """Last step: save the new watermark. Only reached when every task above succeeded."""
        Variable.set(WATERMARK_VARIABLE, window["run_started_at"])
        print(f"Watermark moved from {window['watermark']} to {window['run_started_at']}")

    # ---------------------------------------------------------------------------
    # Task order
    # ---------------------------------------------------------------------------
    window = get_watermark()
    extracted = extract_to_iceberg(window)
    loaded = load_to_snowflake(extracted)
    reconciled = reconcile_counts(window)

    loaded >> reconciled >> update_watermark(window)


oracle_accounts_incremental()
