#!/usr/bin/env python3
# Minimal: snapshot DEV DDLs to versioned folder, then create those tables in PROD (schema/metadata only).
# No extra validation; idempotent.

import os, sys, re, datetime, pathlib
import snowflake.connector

def need(name, default=None):
    v = os.getenv(name, default)
    if v is None or v == "":
        print(f"Missing env: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def read_tables(csv_path: str):
    p = pathlib.Path(csv_path)
    if not p.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr); sys.exit(2)
    txt = p.read_text(encoding="utf-8")
    # split on commas or newlines, trim, drop blanks
    return [t.strip().upper() for t in re.split(r"[,\n]+", txt) if t.strip()]

def main():
    # Connection (use account *identifier*, no .snowflakecomputing.com)
    user      = need("SNOWFLAKE_USER")
    password  = need("SNOWFLAKE_PASSWORD")
    account   = need("SNOWFLAKE_ACCOUNT")             # e.g. acct-xyz123.us-west-2
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role      = os.getenv("SNOWFLAKE_ROLE")

    # Source/Target (unquoted identifiers → uppercase)
    DEV_DB      = os.getenv("DEV_DB", "DEV_DB").upper()
    DEV_SCHEMA  = os.getenv("DEV_SCHEMA", "PUBLIC").upper()
    PROD_DB     = os.getenv("PROD_DB", "PROD_DB").upper()
    PROD_SCHEMA = os.getenv("PROD_SCHEMA", "PUBLIC").upper()

    tables_file = os.getenv("TABLES_FILE", "tables.csv")
    tables = read_tables(tables_file)

    # Prepare snapshot folder
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    root = pathlib.Path("schema_snapshots") / f"{DEV_DB}.{DEV_SCHEMA}" / ts
    root.mkdir(parents=True, exist_ok=True)
    manifest = root / "_manifest.txt"
    manifest.write_text(
        "Snapshot from DEV → version folder\n"
        f"UTC: {ts}\n"
        f"DEV: {DEV_DB}.{DEV_SCHEMA}\n"
        f"Tables: {', '.join(tables)}\n",
        encoding="utf-8",
    )

    # Connect
    conn = snowflake.connector.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, role=role
    )
    cur = conn.cursor()
    try:
        # 1) Snapshot DDLs from DEV into versioned folder
        for t in tables:
            dev_fqn = f"{DEV_DB}.{DEV_SCHEMA}.{t}"
            cur.execute("SELECT GET_DDL('TABLE', %s)", (dev_fqn,))
            ddl = cur.fetchone()[0]
            (root / f"{t}.sql").write_text(ddl.strip() + "\n", encoding="utf-8")
            print(f"📝 wrote {root / f'{t}.sql'}")

        # 2) Migrate (create schema + tables in PROD)
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PROD_DB}.{PROD_SCHEMA}")
        print(f"✅ ensured {PROD_DB}.{PROD_SCHEMA}")
        for t in tables:
            dev_fqn  = f"{DEV_DB}.{DEV_SCHEMA}.{t}"
            prod_fqn = f"{PROD_DB}.{PROD_SCHEMA}.{t}"
            ddl = f"CREATE TABLE IF NOT EXISTS {prod_fqn} LIKE {dev_fqn}"
            print(f"➕ {ddl}")
            cur.execute(ddl)

        print("🎉 Snapshot + migration complete.")
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    main()
