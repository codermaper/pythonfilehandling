# ssiscodetest_new.py
# GitHub Copilot
#
# Modified to use MS SQL Server via pyodbc (SQLAlchemy engine + raw pyodbc cursor)
# Uses provided DATABASE_URI and targets table "table1"
#
# Requirements:
# - Python 3.7+
# - sqlalchemy
# - pyodbc
# - (ODBC Driver 17 for SQL Server)
#
from typing import Iterable, Callable, List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
import time

# ---------- Configuration ----------
DATABASE_URI = r"mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
SRC_QUERY = "SELECT id, name, value, address FROM table2"  # source query (adjust as needed)
TARGET_TABLE = "table6"  # target table name
BATCH_SIZE = 1000
WORKER_THREADS = 4
# -----------------------------------

def extract_stream(engine, query: str, params: Tuple = (), chunksize: int = 1000) -> Iterable[List[Tuple]]:
    """
    Stream rows from SQL Server using DBAPI cursor.fetchmany to avoid loading entire resultset.
    """
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        if params:
            cur.execute(query, *params)
        else:
            cur.execute(query)
        while True:
            batch = cur.fetchmany(chunksize)
            if not batch:
                break
            yield batch
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

def transform_row(row: Tuple) -> Tuple:
    _id, name, value, address = row
    name = (name or "").strip()
    # try:
    #     amount = float(amount) if amount is not None else 0.0
    # except Exception:
    #     amount = 0.0
    # norm = amount / 100.0
    return (_id, name, value, address)

def transform_batch(batch: List[Tuple], transform_fn: Callable[[Tuple], Tuple]) -> List[Tuple]:
    return [transform_fn(r) for r in batch]

def load_batch_sqlserver(engine, table: str, columns: List[str], rows: List[Tuple]):
    """
    Bulk load using pyodbc executemany with fast_executemany enabled for better performance.
    """
    if not rows:
        return
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        # enable fast_executemany on pyodbc cursor for speed (if supported)
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        cols = ",".join(columns)
        placeholders = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cur.executemany(sql, rows)
        raw_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

def run_etl(database_uri: str, src_query: str, query_params: Tuple = (),
            table: str = TARGET_TABLE, batch_size: int = 1000, workers: int = 4):
    engine = create_engine(database_uri, fast_executemany=False)  # fast_executemany set on cursor instead
    start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = []
        total_loaded = 0
        for raw_batch in extract_stream(engine, src_query, params=query_params, chunksize=batch_size):
            fut = exe.submit(transform_batch, raw_batch, transform_row)
            futures.append(fut)

            if len(futures) >= workers * 2:
                for f in as_completed(futures):
                    transformed = f.result()
                    load_batch_sqlserver(engine, table, ["id", "name", "value", "address"], transformed)
                    total_loaded += len(transformed)
                futures = []

        for f in futures:
            transformed = f.result()
            load_batch_sqlserver(engine, table, ["id", "name", "value", "address"], transformed)
            total_loaded += len(transformed)

    elapsed = time.time() - start
    print(f"ETL completed: loaded {total_loaded} rows in {elapsed:.2f}s")

if __name__ == "__main__":
    # Call the method using the provided DATABASE_URI and table1
    run_etl(DATABASE_URI, SRC_QUERY, query_params=(), table=TARGET_TABLE,
            batch_size=BATCH_SIZE, workers=WORKER_THREADS)
