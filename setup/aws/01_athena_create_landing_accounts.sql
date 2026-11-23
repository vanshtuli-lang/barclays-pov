-- =====================================================================================
-- AWS: Iceberg landing table `landing.accounts` (Glue Data Catalog + S3)
-- Run in the Athena query editor (engine v3) as your own AWS user, not Airflow's role.
-- Replace <your-bucket> before running. Run each statement separately.
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS landing;

-- Every DAG run adds its rows tagged with `_batch_id` (= Airflow run_id).
-- A retry of the same run replaces that batch's rows, so it is never duplicated.
CREATE TABLE landing.accounts (
    `account_id`       bigint,
    `customer_name`    string,
    `account_type`     string,
    `balance`          decimal(18,2),
    `currency`         string,
    `status`           string,
    `branch_code`      string,
    `created_ts`       timestamp,
    `last_updated_ts`  timestamp,
    `_batch_id`        string
)
LOCATION 's3://<your-bucket>/iceberg/landing/accounts/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'zstd'
);

-- After the first DAG run, check what landed:
-- SELECT _batch_id, COUNT(*) FROM landing.accounts GROUP BY 1 ORDER BY 1;
