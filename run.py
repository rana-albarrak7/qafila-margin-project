"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import sqlite3
from contextlib import closing

import pandas as pd
import pyarrow as pa
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"
APP_TABLES = ["orders", "order_items", "products", "stores"]
PUBLISHED_TABLES = ["silver.product_cost_history", "gold.weekly_margin", "quality.price_quarantine"]
def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive."""
    # 1) App database: read each table from SQLite as is.
    with closing(sqlite3.connect(data_dir / "qafila_oltp.sqlite")) as src:
        tables = {t: pd.read_sql_query(f"SELECT * FROM {t}", src) for t in APP_TABLES}

    # 2) Supplier files: every column as text, so raw costs like "9,49" stay untouched.
    price_files = sorted((data_dir / "suppliers").glob("prices_*.csv"))
    if not price_files:
        raise SystemExit(f"no supplier files found in {data_dir / 'suppliers'}")

    frames = []
    for path in price_files:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
        df["source_file"] = path.name
        frames.append(df)
    # Files have different columns (W39 renames unit_cost -> cost_sar): concat unions them by name.
    tables["supplier_prices"] = pd.concat(frames, ignore_index=True)

    # 3) Write to DuckDB. CREATE OR REPLACE makes re-runs safe: nothing is ever appended twice.
    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        for name, df in tables.items():
            con.register("incoming", df)
            con.sql(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM incoming")
            con.unregister("incoming")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish the three dbt models to Iceberg, updating each table in place.

    A table is only rewritten when its rows changed, so a re-run on the same data adds no
    snapshot, and the snapshot from before a correction stays readable (time travel).
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db)) as con:
        for name in PUBLISHED_TABLES:
            namespace = name.split(".")[0]
            new_rows = con.sql(f"SELECT * FROM {name}").to_arrow_table()

            catalog.create_namespace_if_not_exists(namespace)
            table = catalog.create_table_if_not_exists(name, schema=new_rows.schema)

            if same_rows(table.scan().to_arrow(), new_rows):
                print(f"{name}: unchanged, no new snapshot")
                continue
            table.overwrite(new_rows)  # replaces the contents; older snapshots are kept
            print(f"{name}: {new_rows.num_rows} rows, snapshot {table.current_snapshot().snapshot_id}")


def same_rows(old: pa.Table, new: pa.Table) -> bool:
    """True when both tables hold exactly the same rows, in any order."""
    if old.num_rows != new.num_rows:
        return False
    with duckdb.connect() as tmp:  # in-memory database, only for the comparison
        tmp.register("old_rows", old)
        tmp.register("new_rows", new)
        extra = tmp.sql(
            "SELECT count(*) FROM (SELECT * FROM new_rows EXCEPT ALL SELECT * FROM old_rows)"
        ).fetchone()[0]
    return extra == 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
