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
import csv
import io
from typing import Iterable, Callable, List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
import time
import os
import uuid
import functools

# ---------- Configuration ----------
DATABASE_URI = r"mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
SRC_QUERY = "SELECT id,name,value,address,c1_int,c2_smallint,c3_bigint,c4_decimal,c5_money,c6_float,c7_bit,c8_date,c9_datetime2,c10_time,c11_guid,c12_char,c13_varchar,c14_email,c15_phone,c16_city,c17_state,c18_zip,c19_country,c20_gender,c21_age,c22_score,c23_status,c24_notes,c25_json,c27_lat,c28_lng,c29_bin,c30_uuid FROM table1"  # source query (adjust as needed)
TARGET_TABLE = "table6"  # target table name
BATCH_SIZE = 20000  # number of rows per batch
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
    id,name,value,address,c1_int,c2_smallint,c3_bigint,c4_decimal,c5_money,c6_float,c7_bit,c8_date,c9_datetime2,c10_time,c11_guid,c12_char,c13_varchar,c14_email,c15_phone,c16_city,c17_state,c18_zip,c19_country,c20_gender,c21_age,c22_score,c23_status,c24_notes,c25_json,c27_lat,c28_lng,c29_bin,c30_uuid = row
    name = (name or "").strip()
    # try:
    #     amount = float(amount) if amount is not None else 0.0
    # except Exception:
    #     amount = 0.0
    # norm = amount / 100.0
    return (id,name,value,address,c1_int,c2_smallint,c3_bigint,c4_decimal,c5_money,c6_float,c7_bit,c8_date,c9_datetime2,c10_time,c11_guid,c12_char,c13_varchar,c14_email,c15_phone,c16_city,c17_state,c18_zip,c19_country,c20_gender,c21_age,c22_score,c23_status,c24_notes,c25_json,c27_lat,c28_lng,c29_bin,c30_uuid)

def transform_batch(batch: List[Tuple], transform_fn: Callable[[Tuple], Tuple]) -> List[Tuple]:
    return [transform_fn(r) for r in batch]

def load_batch_sqlserver(engine, table: str, columns: List[str], rows: List[Tuple]):
    """
    Bulk load using pyodbc executemany with fast_executemany enabled for better performance.
    """
    if not rows:
        return
    if not rows:
        return
    buffer = io.StringIO()
    # ensure output directory exists
    out_dir = r"C:\Users\Hp\Documents\simback"
    os.makedirs(out_dir, exist_ok=True)
    # unique filename per batch
    # filename = f"batch_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}.dat"
    filename = f"batch_testes.dat"
    filepath = os.path.join(out_dir, filename)
    # file-like wrapper so existing buffer.seek(0) call will close the file after writing
    class _FileWrapper:
        def __init__(self, f):
            self._f = f
        def write(self, s):
            return self._f.write(s)
        def seek(self, pos):
            try:
                self._f.flush()
            except Exception:
                pass
            try:
                self._f.close()
            except Exception:
                pass
    fobj = open(filepath, "a", newline="", encoding="utf-8")
    # ensure csv.writer uses '|' as delimiter for the writer created below
    _csv_writer_orig = csv.writer
    csv.writer = functools.partial(_csv_writer_orig, delimiter='|')
    buffer = _FileWrapper(fobj)
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        writer.writerow(r)
    buffer.seek(0)
    # raw_conn = engine.raw_connection()
    # try:
    #     cur = raw_conn.cursor()
    #     # enable fast_executemany on pyodbc cursor for speed (if supported)
    #     try:
    #         cur.fast_executemany = True
    #     except Exception:
    #         pass
    #     cols = ",".join(columns)
    #     placeholders = ",".join("?" for _ in columns)
    #     sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    #     cur.executemany(sql, rows)
    #     raw_conn.commit()
    # finally:
    #     try:
    #         cur.close()
    #     except Exception:
    #         pass
    #     try:
    #         raw_conn.close()
    #     except Exception:
    #         pass

def run_etl(database_uri: str, src_query: str, query_params: Tuple = (),
            table: str = TARGET_TABLE, batch_size: int = 1000, workers: int = 4):
    engine = create_engine(database_uri, fast_executemany=False)  # fast_executemany set on cursor instead
    start = time.time()
    print("Starting ETL...", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = []
        total_loaded = 0
        for raw_batch in extract_stream(engine, src_query, params=query_params, chunksize=batch_size):
            fut = exe.submit(transform_batch, raw_batch, transform_row)
            futures.append(fut)
            print( "worker:-",workers)
            print( "futures:-",len(futures))
            if len(futures) >= workers * 2:
                for f in as_completed(futures):
                    print("fcs:-",total_loaded)
                    transformed = f.result()
                    load_batch_sqlserver(engine, table, [
                        "id","name","value","address",
                        "c1_int","c2_smallint","c3_bigint","c4_decimal","c5_money","c6_float","c7_bit",
                        "c8_date","c9_datetime2","c10_time","c11_guid","c12_char","c13_varchar",
                        "c14_email","c15_phone","c16_city","c17_state","c18_zip","c19_country",
                        "c20_gender","c21_age","c22_score","c23_status","c24_notes","c25_json",
                        "c27_lat","c28_lng","c29_bin","c30_uuid"
                    ], transformed)
                    total_loaded += len(transformed)
                futures = []

        for f in futures:
            print("ff:-",total_loaded)
            transformed = f.result()
            load_batch_sqlserver(engine, table, [
                        "id","name","value","address",
                        "c1_int","c2_smallint","c3_bigint","c4_decimal","c5_money","c6_float","c7_bit",
                        "c8_date","c9_datetime2","c10_time","c11_guid","c12_char","c13_varchar",
                        "c14_email","c15_phone","c16_city","c17_state","c18_zip","c19_country",
                        "c20_gender","c21_age","c22_score","c23_status","c24_notes","c25_json",
                        "c27_lat","c28_lng","c29_bin","c30_uuid"
                    ], transformed)
            total_loaded += len(transformed)

    elapsed = time.time() - start
    print(f"ETL completed: loaded {total_loaded} rows in {elapsed:.2f}s")

if __name__ == "__main__":
    # Call the method using the provided DATABASE_URI and table1
    run_etl(DATABASE_URI, SRC_QUERY, query_params=(), table=TARGET_TABLE,
            batch_size=BATCH_SIZE, workers=WORKER_THREADS)
