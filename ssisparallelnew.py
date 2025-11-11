from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
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
CHUNK_SIZE = 100_000          # rows per chunk
MAX_WORKERS_IO = 5            # threads
MAX_WORKERS_CPU = 4           # processes
OUTPUT_FILE = "C:\\Users\\Hp\\Documents\\simback\\output.dat"    # | separated output

# -------------------------------------
# SQLAlchemy engine setup
# -------------------------------------
def get_engine():
    # Use the provided DB URL (trusted connection to local SQLEXPRESS)
    conn_str = r"mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    return create_engine(conn_str, fast_executemany=True)

# -------------------------------------
# Step 1: I/O Bound — Fetch chunk
# -------------------------------------
def fetch_chunk(offset, limit):
    engine = get_engine()
    query = text(f"""
        SELECT * FROM {TABLE_NAME}
        ORDER BY (Id)
        OFFSET :offset ROWS
        FETCH NEXT :limit ROWS ONLY;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"offset": offset, "limit": limit})
        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows (offset={offset})")
    return [dict(r._mapping) for r in rows]

# -------------------------------------
# Step 2: CPU Bound — Transform chunk
# -------------------------------------
def transform_chunk(rows):
    # Example: Perform a numeric transformation on one column
    transformed = []
    # for r in rows:
        # Example: new computed field
        # if "Amount" in r:
        #     r["Amount"] = round(math.sqrt(r["Amount"] ** 3), 2)
    transformed.append(rows)
    return transformed

# -------------------------------------
# Step 3: I/O Bound — Write chunk to file
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
        for row in rows:
            writer.writerow(row)

# -------------------------------------
# Step 4: Orchestrate Hybrid Pipeline
# -------------------------------------
def hybrid_sql_pipeline(total_rows):
    start_time = time.time()
    os.remove(OUTPUT_FILE) if os.path.exists(OUTPUT_FILE) else None

    # Compute chunk offsets
    chunks = [(i, CHUNK_SIZE) for i in range(0, total_rows, CHUNK_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_IO) as io_pool, \
         ProcessPoolExecutor(max_workers=MAX_WORKERS_CPU) as cpu_pool:

        futures = {}
        first_chunk = True

        for offset, limit in chunks:
            future = io_pool.submit(fetch_chunk, offset, limit)
            futures[future] = (offset, limit, first_chunk)
            first_chunk = False

        for future in as_completed(futures):
            offset, limit, first_chunk_flag = futures[future]
            rows = future.result()

            # Send to process pool for CPU-heavy transformation
            transformed_future = cpu_pool.submit(transform_chunk, rows)
            transformed_rows = transformed_future.result()

            # Write transformed data to file asynchronously
            io_pool.submit(write_to_file, OUTPUT_FILE, transformed_rows, write_header=first_chunk_flag)

    print(f"\n✅ ETL completed in {time.time() - start_time:.2f} seconds")

# -------------------------------------
# Step 5: Run pipeline
# -------------------------------------
if __name__ == "__main__":
    # Estimate total rows (or use a count query)
    total_rows = 7119164  # Example — replace with real row count
    hybrid_sql_pipeline(total_rows)
