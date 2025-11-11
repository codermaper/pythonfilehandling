from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from sqlalchemy import create_engine, text
import math, time, csv, os

# -------------------------------------
# CONFIGURATION
# -------------------------------------
DB_CONFIG = {
    "server": "YOUR_SERVER",
    "database": "YOUR_DATABASE",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "driver": "ODBC Driver 17 for SQL Server"
}

TABLE_NAME = "table1"
CHUNK_SIZE = 1000         # rows per chunk (tune this)
MAX_WORKERS_IO = 5            # threads for I/O (fetch)
MAX_WORKERS_CPU = 4           # processes for CPU transform
OUTPUT_FILE = r"C:\Users\Hp\Documents\simback\output.dat"    # | separated output

# -------------------------------------
# SQLAlchemy engine setup (one engine reused)
# -------------------------------------
def get_engine():
    conn_str = (
        r"mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb"
        r"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )
    return create_engine(conn_str, fast_executemany=True)

ENGINE = get_engine()

# -------------------------------------
# Fetch by primary key range (keyset paging)
# -------------------------------------
def fetch_chunk_range(start_id, end_id):
    query = text(f"""
        SELECT * FROM {TABLE_NAME}
        WHERE Id BETWEEN :start_id AND :end_id
        ORDER BY Id;
    """)
    with ENGINE.connect() as conn:
        result = conn.execute(query, {"start_id": start_id, "end_id": end_id})
        rows = [dict(r._mapping) for r in result.fetchall()]
    print(f"Fetched {len(rows)} rows (Id BETWEEN {start_id} AND {end_id})")
    return rows

# -------------------------------------
# Correct CPU transform (returns list of dicts)
# -------------------------------------
def transform_chunk(rows):
    if not rows:
        return []
    out = []
    for r in rows:
        # example transformation (customize)
        # if "Amount" in r and r["Amount"] is not None:
        #     r["Amount"] = round(math.sqrt(r["Amount"] ** 3), 2)
        out.append(r)
    return out

# -------------------------------------
# Serialized writer (single-threaded writer)
# -------------------------------------
def write_to_file(filename, rows, write_header=False):
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    mode = "w" if write_header else "a"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows (header={write_header})")

# -------------------------------------
# Orchestrate hybrid pipeline with streaming:
# - fetch -> transform -> write are all interleaved
# - writer runs as soon as transformed chunks are ready (even while reads continue)
# -------------------------------------
def hybrid_sql_pipeline():
    start_time = time.time()
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # find id range
    with ENGINE.connect() as conn:
        min_id = conn.execute(text(f"SELECT MIN(Id) FROM {TABLE_NAME}")).scalar() or 0
        max_id = conn.execute(text(f"SELECT MAX(Id) FROM {TABLE_NAME}")).scalar() or -1

    if max_id < min_id:
        print("No rows to process.")
        return

    # build id ranges (inclusive)
    ranges = []
    cur = min_id
    while cur <= max_id:
        end = cur + CHUNK_SIZE - 1
        ranges.append((cur, min(end, max_id)))
        cur = end + 1

    # executors
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_IO) as io_pool, \
         ProcessPoolExecutor(max_workers=MAX_WORKERS_CPU) as cpu_pool, \
         ThreadPoolExecutor(max_workers=1) as writer_pool:

        fetch_futures = {}   # future -> first_chunk_flag
        cpu_futures = {}     # future -> first_chunk_flag

        # submit initial fetches up to MAX_WORKERS_IO
        idx = 0
        if ranges:
            while idx < len(ranges) and len(fetch_futures) < MAX_WORKERS_IO:
                start, end = ranges[idx]
                fut = io_pool.submit(fetch_chunk_range, start, end)
                fetch_futures[fut] = (idx == 0)
                idx += 1

        # Interleave processing of fetch and cpu futures:
        # as soon as a fetch finishes -> submit transform
        # as soon as a transform finishes -> submit write
        from concurrent.futures import FIRST_COMPLETED as _FC, wait as _wait
        while fetch_futures or cpu_futures:
            keys = set()
            keys.update(fetch_futures.keys())
            keys.update(cpu_futures.keys())
            done, _ = _wait(keys, return_when=_FC)
            for f in done:
                # finished fetch -> submit transform, and kick off next fetch if any
                if f in fetch_futures:
                    first_flag = fetch_futures.pop(f)
                    rows = f.result()
                    cf = cpu_pool.submit(transform_chunk, rows)
                    cpu_futures[cf] = first_flag
                    # submit next fetch to keep pipeline full
                    if idx < len(ranges):
                        s, e = ranges[idx]
                        nf = io_pool.submit(fetch_chunk_range, s, e)
                        fetch_futures[nf] = False
                        idx += 1
                # finished transform -> submit write immediately (writer is single-threaded)
                elif f in cpu_futures:
                    first_flag = cpu_futures.pop(f)
                    transformed_rows = f.result()
                    writer_pool.submit(write_to_file, OUTPUT_FILE, transformed_rows, first_flag)

    elapsed = time.time() - start_time
    print(f"\n✅ ETL completed in {elapsed:.2f} seconds")

# -------------------------------------
# Run pipeline
# -------------------------------------
if __name__ == "__main__":
    hybrid_sql_pipeline()
