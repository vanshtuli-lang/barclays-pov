-- =====================================================================================
-- Oracle: simulate one "business day" of changes. Run between DAG runs to demo the
-- incremental behaviour. The next run should pick up exactly these rows
-- (new rows via CREATED_TS, changed rows via LAST_UPDATED_TS).
-- =====================================================================================

-- 1) ~50 balance changes. Setting LAST_UPDATED_TS is what lets the DAG pick them up.
UPDATE POV_ACCOUNTS
   SET BALANCE         = BALANCE + ROUND(DBMS_RANDOM.VALUE(-500, 500), 2),
       LAST_UPDATED_TS = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE ACCOUNT_ID IN (
       SELECT ACCOUNT_ID FROM (SELECT ACCOUNT_ID FROM POV_ACCOUNTS ORDER BY DBMS_RANDOM.VALUE)
        WHERE ROWNUM <= 50);

-- 2) A couple of status changes (soft delete = CLOSED; never hard-DELETE, or reconciliation fails).
UPDATE POV_ACCOUNTS
   SET STATUS          = 'FROZEN',
       LAST_UPDATED_TS = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE ACCOUNT_ID IN (SELECT ACCOUNT_ID FROM (SELECT ACCOUNT_ID FROM POV_ACCOUNTS WHERE STATUS = 'OPEN'
                                              ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= 2);

-- 3) 25 brand-new accounts (CREATED_TS / LAST_UPDATED_TS default to now, UTC).
INSERT INTO POV_ACCOUNTS (ACCOUNT_ID, CUSTOMER_NAME, ACCOUNT_TYPE, BALANCE, BRANCH_CODE)
SELECT m.max_id + LEVEL,
       'New Customer ' || TO_CHAR(m.max_id + LEVEL),
       'CURRENT',
       ROUND(DBMS_RANDOM.VALUE(0, 5000), 2),
       'BR001'
FROM (SELECT MAX(ACCOUNT_ID) AS max_id FROM POV_ACCOUNTS) m
CONNECT BY LEVEL <= 25;

COMMIT;

-- What the next DAG run should see (approx.): ~52 updated + 25 inserted
SELECT COUNT(*) AS CHANGED_LAST_HOUR
  FROM POV_ACCOUNTS
 WHERE LAST_UPDATED_TS > SYS_EXTRACT_UTC(SYSTIMESTAMP) - INTERVAL '1' HOUR;
