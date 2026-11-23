-- =====================================================================================
-- Oracle: seed 1,000 synthetic accounts (the first DAG run loads all of them in FULL mode)
-- Data is fake: no real customer information.
-- =====================================================================================

INSERT INTO POV_ACCOUNTS (
    ACCOUNT_ID, CUSTOMER_NAME, ACCOUNT_TYPE, BALANCE, CURRENCY, STATUS, BRANCH_CODE,
    CREATED_TS, LAST_UPDATED_TS
)
SELECT
    n                                                              AS ACCOUNT_ID,
    'Customer ' || LPAD(n, 5, '0')                                 AS CUSTOMER_NAME,
    CASE MOD(n, 4) WHEN 0 THEN 'CURRENT' WHEN 1 THEN 'SAVINGS'
                   WHEN 2 THEN 'ISA'     ELSE 'MORTGAGE' END       AS ACCOUNT_TYPE,
    ROUND(DBMS_RANDOM.VALUE(0, 250000), 2)                         AS BALANCE,
    'GBP'                                                          AS CURRENCY,
    CASE WHEN MOD(n, 50) = 0 THEN 'CLOSED' ELSE 'OPEN' END         AS STATUS,
    'BR' || LPAD(MOD(n, 25) + 1, 3, '0')                           AS BRANCH_CODE,
    ts                                                             AS CREATED_TS,
    ts                                                             AS LAST_UPDATED_TS
FROM (
    -- Spread creation dates over the past year so the data looks realistic.
    SELECT LEVEL AS n,
           SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(MOD(LEVEL * 7, 365) + 1, 'DAY') AS ts
    FROM DUAL
    CONNECT BY LEVEL <= 1000
);

COMMIT;

-- Sanity check: expect 1000
SELECT COUNT(*) AS ACCOUNT_COUNT FROM POV_ACCOUNTS;
