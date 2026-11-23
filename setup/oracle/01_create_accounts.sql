-- =====================================================================================
-- Oracle: source table POV_ACCOUNTS
-- Run as the schema owner (normally the same user as the Airflow `oracle_default`
-- connection). If you use a different schema, update ORACLE_SOURCE_TABLE in the DAG,
-- e.g. "POV_SRC.POV_ACCOUNTS".
-- =====================================================================================

CREATE TABLE POV_ACCOUNTS (
    ACCOUNT_ID       NUMBER(10)     NOT NULL,
    CUSTOMER_NAME    VARCHAR2(100)  NOT NULL,
    ACCOUNT_TYPE     VARCHAR2(20)   NOT NULL,                 -- CURRENT / SAVINGS / ISA / MORTGAGE
    BALANCE          NUMBER(18,2)   DEFAULT 0      NOT NULL,
    CURRENCY         VARCHAR2(3)    DEFAULT 'GBP'  NOT NULL,
    STATUS           VARCHAR2(10)   DEFAULT 'OPEN' NOT NULL,  -- OPEN / FROZEN / CLOSED (soft delete, never hard-delete)
    BRANCH_CODE      VARCHAR2(10),
    -- All timestamps are stored in UTC so they compare cleanly with the watermark.
    CREATED_TS       TIMESTAMP(6)   DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    LAST_UPDATED_TS  TIMESTAMP(6)   DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    CONSTRAINT POV_ACCOUNTS_PK PRIMARY KEY (ACCOUNT_ID)
);

-- The incremental extract filters on LAST_UPDATED_TS; reconciliation filters on CREATED_TS.
CREATE INDEX POV_ACCOUNTS_LAST_UPDATED_IX ON POV_ACCOUNTS (LAST_UPDATED_TS);
CREATE INDEX POV_ACCOUNTS_CREATED_IX      ON POV_ACCOUNTS (CREATED_TS);

-- Note: every UPDATE must also set LAST_UPDATED_TS = SYS_EXTRACT_UTC(SYSTIMESTAMP),
-- otherwise the DAG won't see the change (see 03_simulate_daily_changes.sql).
