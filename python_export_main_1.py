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
from concurrent.futures import wait, FIRST_COMPLETED

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
            table: str = TARGET_TABLE, batch_size: int = 10000, workers: int = 8):
    """
    Optimized ETL that uses load_batch_sqlserver to write batches to file (instead of DB bulk insert).
    """
    engine = create_engine(database_uri, pool_size=max(5, workers), max_overflow=workers, pool_pre_ping=True)

    start = time.time()
    print("Starting ETL...", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

    columns = [
        "id","name","value","address",
        "c1_int","c2_smallint","c3_bigint","c4_decimal","c5_money","c6_float","c7_bit",
        "c8_date","c9_datetime2","c10_time","c11_guid","c12_char","c13_varchar",
        "c14_email","c15_phone","c16_city","c17_state","c18_zip","c19_country",
        "c20_gender","c21_age","c22_score","c23_status","c24_notes","c25_json",
        "c27_lat","c28_lng","c29_bin","c30_uuid"
    ]

    # loader helper that writes rows using load_batch_sqlserver (which writes to file)
    def _load_rows(rows: List[Tuple]) -> int:
        if not rows:
            return 0
        try:
            load_batch_sqlserver(engine, table, columns, rows)
            return len(rows)
        except Exception as e:
            print("Load task (file write) failed:", e)
            return 0

    # executors: separate transform and load pools
    transform_pool = ThreadPoolExecutor(max_workers=workers)
    load_pool = ThreadPoolExecutor(max_workers=max(1, workers // 2))

    transform_futures = set()
    load_futures = set()
    total_loaded = 0

    # thresholds to avoid unbounded queuing
    transform_threshold = max(2, workers * 3)
    load_threshold = max(2, workers * 4)

    try:
        for raw_batch in extract_stream(engine, src_query, params=query_params, chunksize=batch_size):
            # submit transform work
            transform_futures.add(transform_pool.submit(transform_batch, raw_batch, transform_row))

            # if too many transform tasks queued, wait for some to finish and hand off to loaders
            if len(transform_futures) >= transform_threshold:
                done, transform_futures = wait(transform_futures, return_when=FIRST_COMPLETED)
                for f in done:
                    transformed = f.result()
                    # submit load job (non-blocking)
                    load_futures.add(load_pool.submit(_load_rows, transformed))

            # if loader queue grows too large, wait for some loads to finish and accumulate count
            if len(load_futures) >= load_threshold:
                done_loads, load_futures = wait(load_futures, return_when=FIRST_COMPLETED)
                for lf in done_loads:
                    try:
                        total_loaded += lf.result()
                    except Exception as e:
                        print("Load task failed:", e)

        # finish remaining transforms -> submit remaining loads
        for f in as_completed(transform_futures):
            transformed = f.result()
            load_futures.add(load_pool.submit(_load_rows, transformed))

        # wait for all loads to finish and count
        for lf in as_completed(load_futures):
            try:
                total_loaded += lf.result()
            except Exception as e:
                print("Load task failed:", e)

    finally:
        transform_pool.shutdown(wait=True)
        load_pool.shutdown(wait=True)

    elapsed = time.time() - start
    print(f"ETL completed: exported {total_loaded} rows to file(s) in {elapsed:.2f}s")

def run_etl_import(database_uri: str, src_query: str, query_params: Tuple = (),
            table: str = TARGET_TABLE, batch_size: int = 10000, workers: int = 8):
    """
    Optimized ETL:
    - parallel transform + parallel load (separate executors)
    - use pyodbc fast_executemany on each loader task for high-speed executemany
    - tune pool sizes; avoid building huge in-memory queues
    - recommended: increase batch_size (e.g. 10k-50k) depending on row width and memory
    """
    # tune engine for concurrency
    engine = create_engine(database_uri, pool_size=max(5, workers), max_overflow=workers, pool_pre_ping=True)

    start = time.time()
    print("Starting ETL...", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

    columns = [
        "id","name","value","address",
        "c1_int","c2_smallint","c3_bigint","c4_decimal","c5_money","c6_float","c7_bit",
        "c8_date","c9_datetime2","c10_time","c11_guid","c12_char","c13_varchar",
        "c14_email","c15_phone","c16_city","c17_state","c18_zip","c19_country",
        "c20_gender","c21_age","c22_score","c23_status","c24_notes","c25_json",
        "c27_lat","c28_lng","c29_bin","c30_uuid"
    ]
    placeholders = ",".join("?" for _ in columns)
    insert_sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"

    # loader helper uses fast_executemany on the pyodbc cursor
    def _load_rows(rows: List[Tuple]) -> int:
        if not rows:
            return 0
        raw_conn = engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            try:
                # enable fast_executemany for pyodbc (major speed boost)
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(insert_sql, rows)
                raw_conn.commit()
                return len(rows)
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                raw_conn.close()
            except Exception:
                pass

    # executors: separate transform and load pools
    transform_pool = ThreadPoolExecutor(max_workers=workers)
    load_pool = ThreadPoolExecutor(max_workers=max(1, workers // 2))

    transform_futures = set()
    load_futures = set()
    total_loaded = 0

    # thresholds to avoid unbounded queuing
    transform_threshold = max(2, workers * 3)
    load_threshold = max(2, workers * 4)

    try:
        for raw_batch in extract_stream(engine, src_query, params=query_params, chunksize=batch_size):
            # submit transform work
            transform_futures.add(transform_pool.submit(transform_batch, raw_batch, transform_row))

            # if too many transform tasks queued, wait for some to finish and hand off to loaders
            if len(transform_futures) >= transform_threshold:
                done, transform_futures = wait(transform_futures, return_when=FIRST_COMPLETED)
                for f in done:
                    transformed = f.result()
                    # submit load job (non-blocking)
                    load_futures.add(load_pool.submit(_load_rows, transformed))

            # if loader queue grows too large, wait for some loads to finish and accumulate count
            if len(load_futures) >= load_threshold:
                done_loads, load_futures = wait(load_futures, return_when=FIRST_COMPLETED)
                for lf in done_loads:
                    try:
                        total_loaded += lf.result()
                    except Exception as e:
                        # log and continue (don't crash entire run for single batch failure)
                        print("Load task failed:", e)

        # finish remaining transforms -> submit remaining loads
        for f in as_completed(transform_futures):
            transformed = f.result()
            load_futures.add(load_pool.submit(_load_rows, transformed))

        # wait for all loads to finish and count
        for lf in as_completed(load_futures):
            try:
                total_loaded += lf.result()
            except Exception as e:
                print("Load task failed:", e)

    finally:
        transform_pool.shutdown(wait=True)
        load_pool.shutdown(wait=True)

    elapsed = time.time() - start
    print(f"ETL completed: loaded {total_loaded} rows in {elapsed:.2f}s")


if __name__ == "__main__":
    # Call the method using the provided DATABASE_URI and table1
    run_etl(DATABASE_URI, SRC_QUERY, query_params=(), table=TARGET_TABLE,
            batch_size=BATCH_SIZE, workers=WORKER_THREADS)
