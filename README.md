# Oil, Gas & XOM Ingestion Pipeline

An idempotent, containerized ingestion pipeline that pulls the [Crude Oil WTI, Natural Gas & ExxonMobil (XOM)](https://www.kaggle.com/datasets/faheem113141/oil-and-gas) dataset from Kaggle and loads it into a Postgres `raw` schema — built as a portfolio project to practice real-world Data Engineering fundamentals: idempotency, containerization, and defensive data handling.

## What this does

1. Downloads the dataset from Kaggle (or reuses a local cached copy).
2. Loads the source CSV into a Postgres `raw` schema.
3. Tags every row with lineage metadata (`_source_file`, `_ingested_at`, `_updated_at`).
4. Computes a content hash (`_row_hash`) per row and uses it as an idempotency key, so re-running the pipeline never creates duplicate rows.
5. Detects and safely drops duplicate rows **within** a single source file, logging a warning rather than crashing or silently corrupting data.

## Why this is harder than it looks

The source dataset turned out to be a single CSV (`OG_SAC_Data.csv`) containing all three instruments — WTI crude, natural gas, and XOM — distinguished by a `Ticker` column, rather than three separate files. Splitting them into separate logical views is treated as a **transformation** concern, not an ingestion one — raw data should always mirror its source as closely as possible. Any WTI/NatGas/XOM-specific tables belong in a later staging/transform layer, not here.

## Architecture

```
Kaggle CSV → pandas → staging table (raw._staging_<table>) → MERGE → raw.<table>
```

- **Staging + merge, not direct insert.** Each run lands the batch in a disposable staging table, then merges it into the real table with a single `MERGE ... WHEN MATCHED / WHEN NOT MATCHED` statement. `pandas.to_sql()` has no native upsert support, so this two-step pattern is the standard way to do bulk upserts in Postgres.
- **Idempotency via content hashing.** Every row's business columns are hashed (SHA-256) into `_row_hash`, which carries a `UNIQUE` constraint on the target table. Re-running the pipeline against unchanged source data updates `_updated_at` but never creates duplicate rows.
- **Raw columns are stored as `TEXT`.** Type casting and validation are deliberately deferred to a downstream transform layer, so ingestion never fails because of a malformed numeric value — it flows through as text and gets caught later, where there's visibility into it.
- **In-batch duplicate handling.** If the source file itself contains two rows with identical content, the pipeline drops the duplicate (keeping the first occurrence) and logs a `WARNING` — a deliberate choice to keep the pipeline running rather than crash on a rare but real data-quality issue upstream.

## Metadata columns

| Column | Meaning |
|---|---|
| `_source_file` | Filename the row was loaded from |
| `_ingested_at` | Timestamp the row was **first** inserted — never updated after that |
| `_updated_at` | Timestamp the row was **last seen** by a pipeline run — refreshes every time |
| `_row_hash` | SHA-256 hash of the row's business columns — the idempotency/merge key |

## Project structure

```
oil-and-gas-pipeline/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
└── ingestion/
    ├── ingestion.py
    ├── requirements.txt
    ├── Dockerfile
    └── data/              # downloaded CSVs land here (gitignored)
```

## Running it

### With Docker (recommended)

```bash
cp .env.example .env
# edit .env with your real Postgres and Kaggle credentials
docker compose up --build
```

This starts Postgres, waits for it to report healthy, then runs the ingestion job once.

**Verify idempotency:** run the ingestion service again —
```bash
docker compose run --rm ingestion
```
Row counts in `raw.<table>` should stay identical between runs; only `_updated_at` should advance.

### Without Docker (local dev)

```bash
cd ingestion
pip install -r requirements.txt
python ingestion.py --tables og_sac_data     # process a specific table only
python ingestion.py --force-download          # bypass the local CSV cache
```

## Verifying the data

```bash
docker exec -it <postgres_container_name> psql -U <db_user> -d <db_name>
```

```sql
-- confirm the table landed
\dt raw.*

-- confirm no duplicate content survived (should return 0 rows)
SELECT _row_hash, COUNT(*)
FROM raw.og_sac_data
GROUP BY _row_hash
HAVING COUNT(*) > 1;

-- inspect lineage
SELECT _source_file, MIN(_ingested_at), MAX(_updated_at), COUNT(*)
FROM raw.og_sac_data
GROUP BY 1;
```

## Environment variables

See `.env.example` for the full list. At minimum you need Postgres connection details (`DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`) and a Kaggle API token (`KAGGLE_USERNAME`, `KAGGLE_KEY` — generate one at kaggle.com → Account → Create New API Token).

## Design decisions worth knowing (and defending in an interview)

- **Why a content hash instead of a natural key?** The source data has no column guaranteed to be unique and stable across re-exports. Hashing the row's own content sidesteps that entirely and generalizes to any dataset.
- **Why `MERGE` instead of `INSERT ... ON CONFLICT`?** Both are valid; `MERGE` (Postgres 15+) was chosen here as the primary upsert mechanism, with the `_row_hash` unique constraint as a safety net underneath it. A single source batch containing two identical hashes is handled by pre-merge deduplication rather than relying on `MERGE`'s per-statement conflict handling.
- **Why is the raw layer untyped?** Ingestion should never fail because of a bad value in one row. Type safety belongs in the transform layer, where a failing value can be caught, logged, and inspected — not silently block the whole load.

## Roadmap

- [x] Phase 1 — Idempotent, containerized ingestion
- [ ] Phase 2 — Orchestration (Airflow DAG: extract → validate → load)
- [ ] Phase 3 — Transformation (dbt models splitting WTI / NatGas / XOM into staging views)
- [ ] Phase 4 — Data quality gates (dbt tests / Great Expectations)
- [ ] Phase 5 — Analytics layer (Metabase dashboards on price correlation, volatility)
