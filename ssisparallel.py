import os
import math
import subprocess
import multiprocessing as mp
from pathlib import Path

# ---------------- CONFIGURATION ---------------- #
SERVER = "LAPTOP-3KHISIU8\\SQLEXPRESS"                   # local server
DATABASE = "testdb"
# no USER/PWD when using trusted connection
TABLE = "table1"
OUTPUT_FILE = Path("output_data.dat")
CHUNK_SIZE = 5_000_000
NUM_WORKERS = 4
DELIMITER = "|"
# ------------------------------------------------ #

def get_total_rows():
    """Get total number of rows from SQL Server using Windows auth."""
    import pyodbc
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
    total = cursor.fetchone()[0]
    conn.close()
    return total

def run_bcp(offset):
    """Run one parallel bcp export job for a chunk of rows."""
    chunk_start = offset + 1
    chunk_end = offset + CHUNK_SIZE

    query = (
        f"SELECT * FROM ("
        f"  SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn "
        f"  FROM {TABLE}"
        f") t WHERE rn BETWEEN {chunk_start} AND {chunk_end}"
    )

    temp_file = OUTPUT_FILE.parent / f"chunk_{chunk_start}_{chunk_end}.dat"
    cmd = [
        "bcp",
        query,
        "queryout", str(temp_file),
        "-S", SERVER,
        "-d", DATABASE,
        "-T",                 # use trusted connection (integrated auth)
        "-c",
        "-t", DELIMITER,
        "-r", "\n",
    ]

    print(f"▶ Exporting rows {chunk_start:,}–{chunk_end:,} to {temp_file} ...")
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return str(temp_file)

def combine_files(temp_files):
    with open(OUTPUT_FILE, "wb") as wfd:
        for file in temp_files:
            with open(file, "rb") as fd:
                wfd.write(fd.read())
            Path(file).unlink(missing_ok=True)

def main():
    print("Counting total rows...")
    total_rows = get_total_rows()
    print(f"Total rows: {total_rows:,}")

    offsets = list(range(0, total_rows, CHUNK_SIZE))
    print(f"Launching {NUM_WORKERS} parallel bcp workers...")

    with mp.Pool(NUM_WORKERS) as pool:
        temp_files = pool.map(run_bcp, offsets)

    print("Merging all chunks...")
    print(f"Output file: {OUTPUT_FILE}")    
    print(f"file paths: {temp_files}")
    combine_files(temp_files)
    print(f"✅ Export completed: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
