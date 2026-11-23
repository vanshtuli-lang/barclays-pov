-- =====================================================================================
-- Snowflake (sandbox): tables for the Oracle ACCOUNTS pipeline + key-pair login for Airflow
-- Run in a Snowsight worksheet, top to bottom. Uses your existing role/schema:
--   role VANSHTULI, schema SANDBOX.VANSHTULI
-- =====================================================================================

USE ROLE VANSHTULI;
USE SCHEMA SANDBOX.VANSHTULI;

-- ---------- 1. Who am I? (copy these values; you need them for the Airflow connection) ----------
SELECT CURRENT_USER()                                          AS login,
       CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account,
       CURRENT_ROLE()                                          AS role,
       CURRENT_WAREHOUSE()                                     AS warehouse;  -- if NULL: run SHOW WAREHOUSES and pick one you can use

-- ---------- 2. Tables ----------
-- Staging: holds one batch at a time (Airflow truncates + reloads it every run).
CREATE TRANSIENT TABLE IF NOT EXISTS SANDBOX.VANSHTULI.POV_ACCOUNTS_STG (
    ACCOUNT_ID       NUMBER(38,0),
    CUSTOMER_NAME    VARCHAR(100),
    ACCOUNT_TYPE     VARCHAR(20),
    BALANCE          NUMBER(18,2),
    CURRENCY         VARCHAR(3),
    STATUS           VARCHAR(10),
    BRANCH_CODE      VARCHAR(10),
    CREATED_TS       TIMESTAMP_NTZ,
    LAST_UPDATED_TS  TIMESTAMP_NTZ,
    _BATCH_ID        VARCHAR(250)             -- Airflow run_id that extracted the row
);

-- Target: one row per account (Airflow MERGEs into it on ACCOUNT_ID).
CREATE TABLE IF NOT EXISTS SANDBOX.VANSHTULI.POV_ACCOUNTS (
    ACCOUNT_ID       NUMBER(38,0)  NOT NULL,
    CUSTOMER_NAME    VARCHAR(100),
    ACCOUNT_TYPE     VARCHAR(20),
    BALANCE          NUMBER(18,2),
    CURRENCY         VARCHAR(3),
    STATUS           VARCHAR(10),
    BRANCH_CODE      VARCHAR(10),
    CREATED_TS       TIMESTAMP_NTZ,           -- UTC, as in Oracle
    LAST_UPDATED_TS  TIMESTAMP_NTZ,           -- UTC, as in Oracle
    _LOADED_AT       TIMESTAMP_LTZ,           -- when Airflow last wrote this row
    CONSTRAINT POV_ACCOUNTS_PK PRIMARY KEY (ACCOUNT_ID)
);

SHOW TABLES LIKE 'POV_ACCOUNTS%' IN SCHEMA SANDBOX.VANSHTULI;   -- expect 2 rows

-- ---------- 3. Permissions Airflow needs (role VANSHTULI owns the tables, so it has them) ----------
-- Airflow's bulk load (write_pandas) also creates a TEMPORARY stage in this schema:
SHOW GRANTS ON SCHEMA SANDBOX.VANSHTULI;   -- VANSHTULI should have OWNERSHIP, or CREATE STAGE

-- ---------- 4. Key-pair login for Airflow ----------
-- Paste the one-line public key from:  grep -v "PUBLIC KEY" ~/rsa_key.pub | tr -d '\n'
-- If this fails with "insufficient privileges", ask your Snowflake admin to run it for your user.
SET me = CURRENT_USER();

-- First check: if RSA_PUBLIC_KEY_FP already has a value, another tool uses that key.
-- Don't overwrite it: change RSA_PUBLIC_KEY to RSA_PUBLIC_KEY_2 below (Snowflake accepts either).
DESC USER IDENTIFIER($me);

ALTER USER IDENTIFIER($me) SET RSA_PUBLIC_KEY = '<RSA_PUBLIC_KEY>';

DESC USER IDENTIFIER($me);                 -- RSA_PUBLIC_KEY_FP (or _2_FP) should now have a value
