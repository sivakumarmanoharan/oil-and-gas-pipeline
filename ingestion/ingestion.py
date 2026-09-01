import os
import shutil
import glob
import hashlib
import kagglehub
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import argparse
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("ingest")

load_dotenv()

DATASET = "faheem113141/oil-and-gas"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

DB_HOST = os.getenv("PIPELINE_DB_HOST", "localhost")
DB_PORT = os.getenv("PIPELINE_DB_PORT", "5433")
DB_NAME = os.getenv("PIPELINE_DB_NAME")
DB_USER = os.getenv("PIPELINE_DB_USER")
DB_PASSWORD = os.getenv("PIPELINE_DB_PASSWORD")


def download_dataset(data_dir: str, force_download: bool) -> list[str]:
    """Download the Kaggle dataset and copy all CSVs into data/."""
    os.makedirs(data_dir, exist_ok=True)
    cache_path = kagglehub.dataset_download(DATASET, force_download=force_download)
    logger.info(f"Dataset cache path: {cache_path}")

    csv_files = glob.glob(os.path.join(cache_path, "**", "*.csv"), recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {cache_path}")

    local_files = []
    for src in csv_files:
        dst = os.path.join(data_dir, os.path.basename(src))
        shutil.copy2(src, dst)
        logger.info(f"  Saved: {os.path.basename(src)}")
        local_files.append(dst)

    return local_files

def ensure_schema(engine, schema_name: str) -> None:
    """Ensure that the specified schema exists in the database."""
    with engine.begin() as conn:
        conn.execute( text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    logger.info(f"  Ensured schema '{schema_name}' exists")


def load_csv(filepath: str) -> pd.DataFrame:
    """Load a CSV file into a DataFrame, adding metadata columns and removing duplicates."""
    df = pd.read_csv(filepath)
    df["_source_file"] = os.path.basename(filepath)
    df["_ingested_at"] = pd.Timestamp.now('UTC')
    df["_updated_at"] = pd.Timestamp.now('UTC')
    df["_row_hash"] = add_row_hash(df)
    dupes = df.duplicated(subset="_row_hash", keep="first")
    if dupes.any():
        logger.warning(f"  Dropping {dupes.sum()} duplicate row(s) within {os.path.basename(filepath)} (identical content, keeping first occurrence)")
        df = df[~dupes].reset_index(drop=True)
    logger.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns from {os.path.basename(filepath)}")
    return df


def create_engine_common():
    '''Create a SQLAlchemy engine for PostgreSQL using environment variables.'''
    connection_url = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
    return create_engine(connection_url)

def upsert_to_postgres(df: pd.DataFrame, table_name: str ,engine) -> None:
    """Upsert the DataFrame into the specified PostgreSQL table using a staging table."""
    staging_table = f"staging_{table_name}"
    ensure_schema(engine, "raw")
    update_cols = [c for c in df.columns if c not in ("_ingested_at","_row_hash")]
    all_cols = ", ".join(f'"{c}"' for c in df.columns)
    text_cols = ", ".join(f'"{c}" TEXT UNIQUE' if c == "_row_hash" else f'"{c}" TEXT'for c in df.columns)
    set_clause = ", ".join(f'"{c}" = source."{c}"' for c in update_cols)
    insert_values = ", ".join(f'source."{c}"' for c in df.columns)
    with engine.begin() as conn:
        df.to_sql(staging_table, conn, if_exists="replace", index=False, schema="raw")
        logger.info(f"  Created staging table 'raw.{staging_table}' with {len(df):,} rows")
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS raw."{table_name}" ({text_cols})"""))
        conn.execute(
            text(
                f"""
                MERGE INTO raw."{table_name}" AS target
                USING raw."{staging_table}" AS source
                ON target._row_hash = source._row_hash
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({all_cols})
                    VALUES ({insert_values})
                """
            )
        )
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{staging_table}"'))
    logger.info(f"  Upserted {len(df):,} rows into table '{table_name}'")
    

def add_row_hash(df: pd.DataFrame) -> pd.Series:
    """Compute a SHA256 hash for each row based on its business columns."""
    business_cols = [c for c in df.columns if not c.startswith("_")]
    return df[business_cols].apply(
        lambda row: hashlib.sha256("|".join(str(v) for v in row).encode()).hexdigest(),axis=1,
    )
def run(data_dir: str, force_download: bool, tables: list[str] | None, engine) -> None:
    '''Run the ingestion pipeline: download dataset, load CSVs, and upsert into PostgreSQL.'''
    logger.info("=== Starting ingestion pipeline ===")
    os.makedirs(data_dir, exist_ok=True)
    existing = glob.glob(os.path.join(data_dir, "*.csv"))
    if existing and not force_download:
        logger.info(f"  Using {len(existing)} cached CSV(s) in {data_dir}")
        csv_files = existing                  
    else:
        csv_files = download_dataset(data_dir, force_download)

    for filepath in csv_files:
        raw_name = os.path.splitext(os.path.basename(filepath))[0]
        table_name = raw_name.lower().replace(" ", "_").replace("-", "_")
        if tables and table_name not in tables:
            continue
        logger.info(f"\nProcessing: {os.path.basename(filepath)} -> table '{table_name}'")
        df = load_csv(filepath)
        upsert_to_postgres(df, table_name, engine)

    logger.info("\n=== Ingestion complete ===")


if __name__ == "__main__":
    '''Run the ingestion pipeline with command-line arguments.
    Usage:
        python ingestion.py --data-dir ./data --force-download --tables table1 table2
    '''
    parser= argparse.ArgumentParser()
    parser.add_argument("--data-dir", default = os.path.join(PROJECT_DIR,"data"))
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--tables", nargs="*")
    args=parser.parse_args()
    engine = create_engine_common()
    run(args.data_dir, args.force_download, args.tables, engine)