# copy_schemas.py
# Minimal: read a CSV of table names and create those tables in PROD using DEV as source.
# Does: CREATE SCHEMA IF NOT EXISTS PROD_DB.PROD_SCHEMA
#       CREATE TABLE IF NOT EXISTS PROD_DB.PROD_SCHEMA.TABLE LIKE DEV_DB.DEV_SCHEMA.TABLE

import os, sys

def need(name, default=None):
    v = os.getenv(name, default)
    if v is None or v == "":
        print(f"Missing env: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def read_tables(path: str):
    if not os.path.exists(path):
        print(f"CSV not found: {path}", file=sys.stderr); sys.exit(2)
    txt = open(path, "r", encoding="utf-8").read()
    # allow commas and newlines; ignore blanks
    parts = [p.strip() for p in txt.replace("\n", ",").split(",")]
    return [p for p in parts if p]

def main():
    import snowflake.connector

    # Connection env (use GitHub Secrets)
    user      = "SAATVIKRAYALU"
    password  = "w_vrX7.CVfFNh.8"
    account   = "JFUVMRO-FB11082"        # e.g. acct-xyz123.us-west-2 (no .snowflakecomputing.com)
    warehouse = "COMPUTE_WH"
    role      = "ACCOUNTADMIN"

    # Source/target
    dev_db     = os.getenv("DEV_DB", "DEV_DB").upper()
    dev_schema = os.getenv("DEV_SCHEMA", "PUBLIC").upper()
    prod_db     = os.getenv("PROD_DB", "PROD_DB").upper()
    prod_schema = os.getenv("PROD_SCHEMA", "PUBLIC").upper()

    tables_file = os.getenv("TABLES_FILE", "tables.csv")
    tables = [t.upper() for t in read_tables(tables_file)]

    conn = snowflake.connector.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, role=role
    )
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {prod_db}.{prod_schema}")
        print(f"✅ Ensured schema {prod_db}.{prod_schema}")

        for t in tables:
            src = f"{dev_db}.{dev_schema}.{t}"
            dst = f"{prod_db}.{prod_schema}.{t}"
            ddl = f"CREATE TABLE IF NOT EXISTS {dst} LIKE {src}"
            print(f"➕ {ddl}")
            cur.execute(ddl)

        print("🎉 Done.")
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    main()

