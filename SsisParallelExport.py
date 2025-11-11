"""
ssis_parallel_export.py

SSIS-like parallel export for very large datasets (~50M rows).

Features:
1. Streams data from a database using SQLAlchemy + pandas in chunks.
2. Applies decimal-precision handling per-column (uses Decimal.quantize).
3. Each chunk is processed in parallel (ProcessPoolExecutor) and written to a temporary .dat (pipe-separated) file.
4. Parts are concatenated into a single .dat file (no headers by default) to avoid write-contention.

Requirements:
- Python 3.9+
- pandas
- sqlalchemy
- psycopg2-binary (for PostgreSQL) or the appropriate DB driver

Usage example at bottom of file. Configure DB_URL, SQL_QUERY or TABLE_NAME and run.

Notes & tips:
- Choose chunksize according to DB and network: 100k - 1M depending on row size and memory.
- For extremely high throughput consider using a more low-level cursor (psycopg2) or COPY for Postgres.
- This script keeps memory usage low by streaming and writing per-chunk files.
"""

from __future__ import annotations
import os
import tempfile
import shutil
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from sqlalchemy import create_engine


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("ssis_parallel_export")


def format_decimal_value(value, scale: int) -> Optional[str]:
    """Format a single numeric/decimal value to the specified scale.

    - If value is None/NaN/empty -> return empty string (keeps delimiters intact)
    - Uses Decimal.quantize to avoid floating point artifacts
    - Returns a string without scientific notation and with fixed decimals
    """
    print('format_decimal_value called')
    if value is None:
        return ""
    # pandas may give us numpy.float64 or Decimal or str
    try:
        if isinstance(value, str):
            if value.strip() == "":
                return ""
            d = Decimal(value)
        elif isinstance(value, Decimal):
            d = value
        else:
            # fallback to Decimal from float/int
            d = Decimal(str(value))
    except Exception:
        # If it can't convert, return original as string
        return str(value)

    quant = Decimal((0, (1,), -scale)) if False else Decimal('1').scaleb(-scale)
    # quantize requires a Decimal with the proper exponent: e.g. scale=2 -> Decimal('0.01')
    quant = Decimal(1).scaleb(-scale)

    try:
        d = d.quantize(quant, rounding=ROUND_HALF_UP)
    except Exception:
        # fallback to normalized string
        d = d
    # Normalize to remove trailing zeros? Keep trailing zeros to keep fixed scale
    # Format: use 'f' to ensure decimal point format
    fmt = f"{{0:.{scale}f}}"
    return fmt.format(d)


def apply_decimal_precision(df: pd.DataFrame, decimal_config: Dict[str, int]) -> pd.DataFrame:
    """Apply decimal formatting to specified columns.

    decimal_config: mapping column_name -> scale (number of digits after decimal point)
    Returns a new DataFrame (strings for those columns) to ensure exact representation in output file.
    """
    print('apply_decimal_precision called')
    if not decimal_config:
        return df

    # Work on a copy to avoid modifying caller frame
    df_out = df.copy()
    for col, scale in decimal_config.items():
        if col not in df_out.columns:
            logger.warning("Column '%s' not found in chunk; skipping decimal formatting for it.", col)
            continue

        # Apply vectorized formatting via map to keep memory reasonable
        df_out[col] = df_out[col].map(lambda v: format_decimal_value(v, scale))

    return df_out


def write_chunk_to_temp(df: pd.DataFrame, part_index: int, output_dir: str, include_header: bool = False) -> str:
    """Append DataFrame to a single aggregate .dat file (pipe-separated).

    All workers append to the same file. Note: concurrent append from multiple processes
    can still interleave on some platforms; for robust cross-process coordination
    consider using a file lock (e.g. portalocker) or writing per-process files and
    concatenating in the parent process.
    """
    print('write_chunk_to_temp called')
    os.makedirs(output_dir, exist_ok=True)
    aggregate_path = os.path.join(output_dir, "export_aggregate.dat")

    # Determine if we need to write header (only if include_header True and file is empty/non-existent)
    write_header = False
    try:
        if include_header:
            if not os.path.exists(aggregate_path) or os.path.getsize(aggregate_path) == 0:
                write_header = True
    except Exception:
        # if anything goes wrong checking the file, fallback to writing header only if requested
        write_header = include_header

    # Append to the aggregate file
    # Using mode='a' ensures we append; header only written if file was empty and include_header is True
    df.to_csv(aggregate_path, sep='|', index=False, header=write_header, mode='a', lineterminator='\n')

    return aggregate_path


def stream_query(engine, sql: str, chunksize: int):
    """Yield DataFrame chunks using pandas.read_sql_query with iterator.

    Note: for very large exports and Postgres, consider using server_side cursor directly.
    """
    print('stream_query called')
    logger.info("Starting stream_query with chunksize=%d", chunksize)
    return pd.read_sql(sql, con=engine, chunksize=chunksize)


def process_chunk_worker(args: Tuple[int, pd.DataFrame, Dict[str, int], str, bool]):
    """Worker function executed in a separate process. Receives a picklable tuple.

    Because DataFrames are pickled across processes, keep them reasonably small.
    """
    print('process_chunk_worker called')
    part_index, df_chunk, decimal_config, output_dir, include_header = args
    # Apply decimal formatting
    df_processed = apply_decimal_precision(df_chunk, decimal_config)
    # Write to temp part file
    path = write_chunk_to_temp(df_processed, part_index, "C:\\Users\\Hp\\Documents\\simback\\", include_header)
    return path


def export_parallel(
    db_url: str,
    sql: str,
    output_path: str,
    decimal_config: Dict[str, int],
    chunksize: int = 50000,
    max_workers: Optional[int] = None,
    include_header: bool = False,
    temporary_dir: Optional[str] = None,
):
    """Main orchestration to export table/query to a pipe-separated .dat file in parallel.

    Parameters:
    - db_url: SQLAlchemy DB URL (e.g. postgresql+psycopg2://user:pass@host:port/dbname)
    - sql: SELECT query string
    - output_path: final .dat path
    - decimal_config: mapping column->scale
    - chunksize: number of rows per chunk
    - max_workers: how many processes to use. If None uses max(1, cpu_count()-1).
    - include_header: whether to include header row in the final file
    - temporary_dir: directory to write parts (defaults to system temp dir)
    """
    if max_workers is None:
        import multiprocessing
        max_workers = max(1, multiprocessing.cpu_count() - 1)

    # if temporary_dir is None:
    #     temporary_dir = tempfile.mkdtemp(prefix='ssis_export_')
    # else:
    #     os.makedirs(temporary_dir, exist_ok=True)
    temporary_dir="C:\\Users\\Hp\\Documents\\simback"

    engine = create_engine(db_url)

    part_paths: List[str] = []
    part_index = 0
    try:
        # Prepare iterator
        chunk_iterator = stream_query(engine, sql, chunksize)

        # We'll use ProcessPoolExecutor to process chunks in parallel. We submit as we read so memory remains bounded.
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            submitted = []
            for df_chunk in chunk_iterator:
                # Submit for processing. Send as tuple to worker function (picklable)
                
                args = (part_index, df_chunk, decimal_config, temporary_dir, False)  # header handled later
                future = executor.submit(process_chunk_worker, args)
                submitted.append((part_index, future))
                logger.info("Submitted part %d for processing (rows=%d)", part_index, len(df_chunk))
                part_index += 1

            # Collect results as they complete
            for idx, future in submitted:
                path = future.result()
                logger.info("Part %d completed -> %s", idx, path)
                part_paths.append((idx, path))

        # Sort parts by index
        part_paths.sort(key=lambda x: x[0])
        print(1)
        # Concatenate parts into final file. Optionally write header from first part's columns.
        if os.path.exists(output_path):
            logger.warning("Output path %s already exists and will be overwritten.", output_path)
            os.remove(output_path)
        print(2)
        with open(output_path, 'wb') as outfile:
            # Optionally write header
            if include_header and part_paths:
                # Read header from first part (text mode) and write it
                first_part = part_paths[0][1]
                with open(first_part, 'rb') as fh:
                    header_line = fh.readline()
                    outfile.write(header_line)
                # Now append the remainder of first part (without header)
                with open(first_part, 'rb') as fh:
                    fh.readline()  # skip header
                    shutil.copyfileobj(fh, outfile)
                # append other parts
                for _, part in part_paths[1:]:
                    with open(part, 'rb') as fh:
                        shutil.copyfileobj(fh, outfile)
            else:
                # No header: write all parts in order
                for _, part in part_paths:
                    with open(part, 'rb') as fh:
                        shutil.copyfileobj(fh, outfile)

        logger.info("Export completed: %s", output_path)
    finally:
        # Clean up temporary parts
        try:
            #shutil.rmtree(temporary_dir)
            logger.info("Temporary dir %s removed", temporary_dir)
        except Exception as e:
            logger.warning("Could not remove temporary dir %s: %s", temporary_dir, str(e))


# ------------------------
# Example usage (edit and run)
# ------------------------
if __name__ == '__main__':
    # === USER CONFIG ===
    DB_URL = r'mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    #'postgresql+psycopg2://username:password@db-host:5432/mydb'  # replace with your DB URL
    SQL_QUERY = 'SELECT * FROM table1'  # replace with your query

    # Output file
    OUTPUT_FILE = 'export_result.dat'

    # Which columns need decimal handling and how many decimal places for each
    # Example: {'price': 2, 'tax': 4}
    DECIMAL_CONFIG = {
        # 'price': 2,
        # 'amount': 3,
    }

    # Chunk size: tune based on network, DB and memory
    CHUNKSIZE = 50000

    # Number of parallel workers; None to auto-detect
    MAX_WORKERS = None

    # Whether to include header row
    INCLUDE_HEADER = False

    # Optional temporary directory
    TEMP_DIR = None

    # Run export
    export_parallel(
        db_url=DB_URL,
        sql=SQL_QUERY,
        output_path=OUTPUT_FILE,
        decimal_config=DECIMAL_CONFIG,
        chunksize=CHUNKSIZE,
        max_workers=MAX_WORKERS,
        include_header=INCLUDE_HEADER,
        temporary_dir=TEMP_DIR,
    )
