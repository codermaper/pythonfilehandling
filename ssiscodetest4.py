# ssiscodetest4_to_dat.py
# GitHub Copilot
#
# Optimized: streaming fetchmany, threaded transform pipeline, single high-throughput
# writer thread that writes pipe-separated (.dat) file with minimal overhead.
#
import time
import os
import threading
from typing import Iterable, List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from sqlalchemy import create_engine
from queue import Queue
import datetime

# ---------- Configuration ----------
DATABASE_URI = r"mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
SRC_QUERY = "SELECT id,name,value,address,c1_int,c2_smallint,c3_bigint,c4_decimal,c5_money,c6_float,c7_bit,c8_date,c9_datetime2,c10_time,c11_guid,c12_char,c13_varchar,c14_email,c15_phone,c16_city,c17_state,c18_zip,c19_country,c20_gender,c21_age,c22_score,c23_status,c24_notes,c25_json,c27_lat,c28_lng,c29_bin,c30_uuid FROM table1"
OUTPUT_FILE = "output.dat"
BATCH_SIZE = 20000
TRANSFORM_WORKERS = 4
MAX_IN_FLIGHT = 8  # number of transform batches allowed in flight before backpressure
QUEUE_MAXSIZE = 16  # how many transformed batches can queue to writer
# -----------------------------------

def extract_stream(engine, query: str, params: Tuple = (), chunksize: int = 1000) -> Iterable[List[Tuple]]:
    raw_conn = engine.raw_connection()
    cur = None
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
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        try:
            raw_conn.close()
        except Exception:
            pass

def transform_row(row: Tuple) -> Tuple:
    # keep column ordering; lightweight transforms (trim strings)
    # unpack defensively in case of different column counts
    # (trim strings and leave other types as-is)
    out = []
    for v in row:
        if isinstance(v, str):
            out.append(v.strip())
        else:
            out.append(v)
    return tuple(out)

def transform_batch(batch: List[Tuple]) -> List[Tuple]:
    return [transform_row(r) for r in batch]

def _format_field_for_pipe(v: Any) -> str:
    # Convert common types to safe string for pipe-separated file.
    if v is None:
        return ""
    # bytes/bytearray -> hex (fast and safe)
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.hex()
        except Exception:
            return str(v)
    # datetime types -> iso format without microseconds for compactness
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=' ', timespec='seconds')
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, datetime.time):
        return v.isoformat(timespec='seconds')
    # otherwise str, but avoid expensive repr; also remove stray pipes/newlines
    s = str(v)
    # replace delimiter and newlines to keep row integrity (cheap replace)
    if '|' in s or '\n' in s or '\r' in s:
        s = s.replace('|', ' ').replace('\r', ' ').replace('\n', ' ')
    return s

def writer_thread_fn(q: Queue, file_path: str, totals: dict, totals_lock: threading.Lock, chunk_write_threshold: int = 1<<20):
    """
    Writer thread consumes transformed batches from q and writes them to file_path
    using a large in-memory chunk per put to minimize syscall overhead.
    Sentinel: None signals completion.
    """
    # open with a large buffer and UTF-8 encoding
    # using 'w' with newline='' and buffering large (1MB)
    dirpath = os.path.dirname(file_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(file_path, "w", encoding="utf-8", buffering=1<<20, newline="") as fh:
        write_buffer_parts = []
        write_buffer_size = 0
        local_count = 0
        while True:
            batch = q.get()
            if batch is None:
                # flush remaining
                if write_buffer_parts:
                    fh.write(''.join(write_buffer_parts))
                    write_buffer_parts = []
                break
            # build chunk for this batch
            lines = []
            for row in batch:
                # fast per-row join of formatted fields
                vals = [_format_field_for_pipe(v) for v in row]
                lines.append('|'.join(vals))
            chunk = '\n'.join(lines) + '\n'
            write_buffer_parts.append(chunk)
            write_buffer_size += len(chunk)
            local_count += len(batch)
            # flush if buffer large to avoid using too much memory
            if write_buffer_size >= chunk_write_threshold:
                fh.write(''.join(write_buffer_parts))
                write_buffer_parts = []
                write_buffer_size = 0
            # update global counter occasionally
            if local_count >= 1000:
                with totals_lock:
                    totals['written'] += local_count
                local_count = 0
            q.task_done()
        # final flush and totals update
        if write_buffer_parts:
            fh.write(''.join(write_buffer_parts))
        if local_count:
            with totals_lock:
                totals['written'] += local_count

def run_etl_to_dat(database_uri: str, src_query: str, output_file: str = OUTPUT_FILE,
                   batch_size: int = BATCH_SIZE, transform_workers: int = TRANSFORM_WORKERS):
    engine = create_engine(database_uri, pool_pre_ping=True)
    start = time.time()
    print("Starting extract->.dat...", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))
    totals = {'written': 0}
    totals_lock = threading.Lock()

    q: Queue = Queue(maxsize=QUEUE_MAXSIZE)
    writer = threading.Thread(target=writer_thread_fn, args=(q, output_file, totals, totals_lock), daemon=False)
    writer.start()

    transform_pool = ThreadPoolExecutor(max_workers=transform_workers)

    try:
        in_flight = []  # list of transform futures
        extractor = extract_stream(engine, src_query, chunksize=batch_size)

        for raw_batch in extractor:
            # backpressure: wait until in_flight has capacity
            while len(in_flight) >= MAX_IN_FLIGHT:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight.remove(fut)
                    try:
                        transformed = fut.result()
                        # push to writer queue (blocks if queue full -> backpressure)
                        q.put(transformed)
                    except Exception as e:
                        print("Transform failed:", e)
            # submit transform job
            tf = transform_pool.submit(transform_batch, raw_batch)
            in_flight.append(tf)

        # drain remaining transform futures
        while in_flight:
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                in_flight.remove(fut)
                try:
                    transformed = fut.result()
                    q.put(transformed)
                except Exception as e:
                    print("Transform failed during drain:", e)

        # signal writer to finish
        q.put(None)
        # wait for writer to finish consuming
        writer.join()

        # ensure transform pool shutdown
        transform_pool.shutdown(wait=True)

    finally:
        try:
            engine.dispose()
        except Exception:
            pass

    elapsed = time.time() - start
    written = totals['written']
    print(f"Completed: wrote {written} rows to {output_file} in {elapsed:.2f}s")

if __name__ == "__main__":
    if __name__ == "__main__":
        out_dir = r"C:\Users\Hp\Documents\simback"
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "output.dat")
        run_etl_to_dat(DATABASE_URI, SRC_QUERY, output_file=out_path,
                       batch_size=BATCH_SIZE, transform_workers=TRANSFORM_WORKERS)
