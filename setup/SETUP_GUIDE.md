# Barclays POV: Oracle ACCOUNTS → Iceberg → Snowflake setup guide

Do these steps in order. Every table, schema and grant is created **by hand** with the scripts in
`setup/`; the DAG never runs DDL.

| What | Where | Value used by the DAG |
|---|---|---|
| Source table | Oracle | `POV_ACCOUNTS` (in the `oracle_default` user's schema) |
| Landing table | Glue catalog + S3 (Iceberg) | `landing.accounts` |
| Staging / target | Snowflake | `SANDBOX.VANSHTULI.POV_ACCOUNTS_STG` → `SANDBOX.VANSHTULI.POV_ACCOUNTS` |
| Watermark | Airflow **Variable** | `oracle_accounts_watermark` (UTC timestamp) |

**You don't need to create the Variable.** The first run finds no Variable and does a full load;
the last task creates or updates it. All other settings are constants at the top of
`dags/oracle_accounts_incremental.py`. The only Airflow-side setup is three **connections**:
`oracle_default`, `aws_default`, `snowflake_default`.
Create them in the **Astro UI → Environment → Connections** (Environment Manager) and **link them to
your Deployment**. Environment Manager connections take precedence over env vars and the Airflow UI.

---

## 1. Oracle (existing server)

1. Connect as the schema owner and run `setup/oracle/01_create_accounts.sql`, then `02_seed_accounts.sql` (1,000 rows).
2. Check the `oracle_default` connection:

   | Field | Value |
   |---|---|
   | Connection type | Oracle |
   | Host / Port | `<oracle-host>` / `1521` |
   | Login / Password | DB user that can `SELECT` from `ACCOUNTS` |
   | Extra | `{"service_name": "<SERVICE_NAME>"}` (or `{"sid": "<SID>"}`) |

3. **Permissions:** the Airflow user needs `CREATE SESSION` and `SELECT` on `ACCOUNTS` only. If the
   table belongs to another schema, run `GRANT SELECT ON <owner>.ACCOUNTS TO <airflow_user>;` and set
   `ORACLE_SOURCE_TABLE = "<owner>.ACCOUNTS"` in the DAG.
4. **Network:** the Oracle listener must accept connections from the Astro Deployment's egress IPs
   (Astro UI → Deployment → Details → *External IPs*). An on-prem Oracle typically needs a firewall rule
   or a private link.
   The provider uses python-oracledb **thin mode**, so no Oracle Instant Client is required.

## 2. AWS (S3 + Glue + Iceberg)

1. Pick (or create) a bucket, e.g. `s3://barclays-pov-lake`, **in the same region as Glue**.
2. In **Athena**, run `setup/aws/01_athena_create_landing_accounts.sql` (replace `<your-bucket>`).
3. **IAM role for Airflow:** create a role, e.g. `astro-barclays-pov`, with the inline policy from
   `setup/aws/iam_policy_airflow_pov.json` (replace `<region>`, `<account-id>`, `<your-bucket>`).
   It grants least privilege: read/update the Glue table metadata, and read/write/delete objects only under the
   table's S3 prefix. If the bucket uses SSE-KMS, also add `kms:Decrypt` and `kms:GenerateDataKey` on that key.
4. **Let the Astro Deployment assume the role (no access keys).** Use workload identity:
   - Astro UI → Deployment → **Details → Advanced → Edit → Workload Identity → Customer Managed Identity**.
   - Paste the role ARN, then run the AWS CLI command it shows (it adds the OIDC trust to the role) → **Save** → **Update Deployment**.
5. `aws_default` connection:

   | Field | Value |
   |---|---|
   | Connection type | Amazon Web Services |
   | Login / Password | *leave empty* (credentials come from workload identity) |
   | Extra | `{"region_name": "<region>"}` |

   > Alternative if you keep the default workload identity: add the Deployment's workload-identity ARN to
   > the role's trust policy (`sts:AssumeRole`) and set Extra to
   > `{"role_arn": "arn:aws:iam::<account-id>:role/astro-barclays-pov", "region_name": "<region>"}`.

   `region_name` is **required**: the DAG passes it to pyiceberg's Glue/S3 clients.

## 3. Snowflake (sandbox schema, key-pair auth)

Airflow connects as **your** user with role `VANSHTULI` and writes to `SANDBOX.VANSHTULI`.

1. Generate a key pair on your Mac (remember the passphrase):
   ```bash
   cd ~ && openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   grep -v "PUBLIC KEY" rsa_key.pub | tr -d '\n'; echo        # one-line public key
   base64 -i rsa_key.p8 | tr -d '\n'; echo                    # private key for Airflow
   ```
2. In Snowsight, run `setup/snowflake/01_create_tables.sql` top to bottom, with `<RSA_PUBLIC_KEY>` replaced.
   Note the login, account and warehouse from its first query.
3. Astro → Deployment → **Environment → Environment Variables**: add `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` (mark it **Secret**):
   ```json
   {"conn_type": "snowflake", "login": "<LOGIN>", "password": "<key passphrase>",
    "schema": "VANSHTULI",
    "extra": {"account": "<ORG>-<ACCOUNT>", "warehouse": "<WAREHOUSE>", "database": "SANDBOX",
              "role": "VANSHTULI", "authenticator": "SNOWFLAKE_JWT",
              "private_key_content": "<base64 private key>"}}
   ```

## 4. Deploy & run

```bash
git add -A && git commit -m "Oracle ACCOUNTS incremental pipeline" && git push
astro deploy                     # full image deploy (new packages in requirements.txt)
```
1. Unpause `oracle_accounts_incremental` and trigger it. There's no watermark yet, so it does a **full load** (1,000 rows).
   Then check **Admin → Variables**: `oracle_accounts_watermark` now holds a timestamp.
2. Run `setup/oracle/03_simulate_daily_changes.sql`, then trigger again. This time it's **incremental** (~77 rows).

### Demo scenarios
| Show | How |
|---|---|
| Failure keeps the watermark | Rename `POV_ACCOUNTS` temporarily → `merge_into_snowflake` fails after 2 retries → the Variable is unchanged. Rename it back and clear the task: it succeeds. |
| Rerun = no duplicates | Clear a successful run → the Snowflake count stays the same and reconciliation passes. |
| Reconciliation check | `DELETE` one row from `POV_ACCOUNTS` → the next run fails in `check_snowflake_count`. |
| Start over (full load) | Delete the `oracle_accounts_watermark` Variable. |

## Known limits (fine for a demo)
- Hard deletes in Oracle aren't detected by a timestamp watermark, so use `STATUS='CLOSED'`.
- Extracted rows are held in memory; fine for demo-sized data.
