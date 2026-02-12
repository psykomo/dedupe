# Inmate Deduplication Starter (Source MySQL + Splink + DuckDB)

This starter keeps production MySQL read-only and persists all dedupe outputs in a local DuckDB file.

## What You Get

- Splink-based entity resolution (DuckDB backend)
- `unique_inmates` table (one canonical inmate per entity)
- `record_entity_map` table (every source record mapped to `entity_id`)
- `review_queue` table (manual verification for mid-confidence matches)
- `dedupe_run` table (run audit trail)

## Architecture

1. Read source records from `source_mysql` (read-only).
2. Normalize key identity fields (`nik`, `nomor_induk_nasional`, `nama_lengkap`, aliases, DOB, gender, phone, address, parent names).
3. Run Splink matching and clustering in DuckDB.
4. Store outputs in DuckDB file (`dedupe_duckdb.path`) only.

No writes are made to production source tables.

## Core Files

- `/Users/hap/Documents/dev/sdp/dedupe/sql/duckdb_schema.sql`
- `/Users/hap/Documents/dev/sdp/dedupe/config.example.yaml`
- `/Users/hap/Documents/dev/sdp/dedupe/src/inmate_dedupe/cli.py`
- `/Users/hap/Documents/dev/sdp/dedupe/src/inmate_dedupe/pipeline.py`
- `/Users/hap/Documents/dev/sdp/dedupe/src/inmate_dedupe/splink_runner.py`
- `/Users/hap/Documents/dev/sdp/dedupe/src/inmate_dedupe/normalize.py`
- `/Users/hap/Documents/dev/sdp/dedupe/src/inmate_dedupe/mysql_store.py`

## Quick Start

```bash
cd /Users/hap/Documents/dev/sdp/dedupe
uv sync
cp config.example.yaml config.yaml
```

Edit `config.yaml`:
- `source_mysql.*` points to your source server/table (or custom source query).
- `dedupe_duckdb.path` points to your local dedupe database file.
- map `columns.*` to your actual source columns.

Initialize schema:

```bash
uv run inmate-dedupe --config config.yaml init-schema --sql sql/duckdb_schema.sql
```

Run source preflight checks (connectivity, query/table access, mapped columns, sample quality):

```bash
uv run inmate-dedupe --config config.yaml validate-source --sample-rows 500
```

Run full dedupe:

```bash
uv run inmate-dedupe --config config.yaml full
```

Run incremental dedupe:

```bash
uv run inmate-dedupe --config config.yaml incremental
```

Run bootstrap backfill in batches (recommended for large historical loads):

```bash
uv run inmate-dedupe --config config.yaml bootstrap
```

Override batch size per run:

```bash
uv run inmate-dedupe --config config.yaml bootstrap --max-rows 200000
```

## Custom Source Query Mode

Use `source_mysql.custom_query` when you only have read access and need joins/filters from multiple tables.

Rules:
- Keep `source_mysql.source_table` as a stable logical name (used for run/state metadata).
- `custom_query` must output all columns mapped in `columns.*`.
- `custom_query` must output `source_id_column`.
- `custom_query` should output `source_updated_at_column` if you want updated-at cursor mode.
- Do not include `LIMIT` in `custom_query`; pipeline controls batching and pagination.

Example:

```yaml
source_mysql:
  source_table: "identitas_filtered"
  custom_query: |
    SELECT it.*
    FROM identitas it
    WHERE it.NAMA_LENGKAP <> ''
      AND it.NAMA_LENGKAP NOT LIKE 'NAMA WBP%'
      AND it.NOMOR_INDUK NOT LIKE '11f066%'
      AND EXISTS (
        SELECT 1
        FROM perkara p
        WHERE p.NOMOR_INDUK = it.NOMOR_INDUK
          AND p.ID_PERKARA IS NOT NULL
          AND p.ID_UPT IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM cif_mapping cm
        WHERE cm.NOMOR_INDUK = it.NOMOR_INDUK
      )
  source_id_column: "NOMOR_INDUK"
  source_updated_at_column: "UPDATED_AT"
```

## Cursor Modes

- `updated_at` cursor mode (default when `source_updated_at_column` is set):
  - incremental filter: `source_updated_at > watermark`
  - bootstrap filter: up to a fixed cutoff timestamp
- `source_id` cursor mode (automatic when `source_updated_at_column: null`):
  - incremental/bootstrap filter: `source_id_column > last_cursor`
  - ordering: `ORDER BY source_id_column ASC`
  - designed for lexically sortable IDs such as `NOMOR_INDUK`

`validate-source` now reports the active `cursor_mode` and sample parseability of `NOMOR_INDUK` into:
- `id_upt` (1-3)
- `year` (4-7)
- `month` (8-9)
- `day` (10-11)
- `sequence` (12-15)

Bootstrap mode behavior:
- First bootstrap run pins a cutoff timestamp and stores cursor state in DuckDB (`pipeline_bootstrap_state`).
- `run.bootstrap_state_file` remains as a legacy local mirror for observability.
- Each bootstrap run processes the next historical slice up to that cutoff.
- Within each bootstrap batch, unresolved new records are deduped against each other before creating new entities.
- Bootstrap writes (entity/map/review), run completion, and cursor/watermark updates are committed in one DuckDB transaction.
- If a bootstrap batch errors before commit, it is marked failed and safely retried from the previous cursor.
- If the process is killed hard (for example `SIGKILL`), that run row may remain `running`, but entity/map/review/state writes are still transaction-safe and next bootstrap resumes from the last committed cursor.
- Normal `incremental` automatically starts from that cutoff while bootstrap is still running.
- When bootstrap finishes, the normal watermark is advanced and only new records are processed.
- Bootstrap is done when a run returns `records_processed: 0` (or `< batch size` on the final non-zero run).

## Dummy Source With Docker Compose

Use this to spin up a local MariaDB source and seed high-volume synthetic inmate data.

Start source DB:

```bash
docker compose up -d source-db
```

Start Adminer (DB UI):

```bash
docker compose up -d adminer
```

Adminer URL:
- `http://localhost:8081`

Adminer login for dummy source:
- System: `MySQL`
- Server: `source-db`
- Username: `readonly_source_user` (or `root`)
- Password: `source_pass` (or `rootpass` for root)
- Database: `corrections_prod`

Seed data (default: 500,000 rows):

```bash
docker compose run --rm source-seed
```

Seed a different volume (up to 1,000,000 with current generator):

```bash
docker compose run --rm -e SEED_ROWS=1000000 source-seed
```

Force rebuild seeded data:

```bash
docker compose run --rm -e FORCE_RESEED=true -e SEED_ROWS=500000 source-seed
```

Use prewired config for this stack:

```bash
uv run inmate-dedupe --config config.dummy.yaml init-schema --sql sql/duckdb_schema.sql
uv run inmate-dedupe --config config.dummy.yaml bootstrap
# repeat bootstrap until records_processed becomes 0
uv run inmate-dedupe --config config.dummy.yaml incremental
```

Stop local source:

```bash
docker compose down
```

Clean reset (start testing from absolute zero):

```bash
# from /Users/hap/Documents/dev/sdp/dedupe
docker compose down -v --remove-orphans
rm -f ./work/dedupe_dummy.duckdb
rm -f ./work/watermark_dummy.json
rm -f ./work/bootstrap_state_dummy.json
rm -rf ./work/run_*
docker compose up -d source-db adminer
docker compose run --rm -e FORCE_RESEED=true -e SEED_ROWS=10000 source-seed
uv run inmate-dedupe --config config.dummy.yaml init-schema --sql sql/duckdb_schema.sql
```

## Output Tables

- `unique_inmates`: canonical deduped inmate entities
  - includes `cif_number` (`VARCHAR`) with temporary deterministic format: `CIF` + zero-padded 12-digit `entity_id` (example: `CIF000000001234`)
- `record_entity_map`: source `record_id -> entity_id`, score, decision
- `review_queue`: unresolved candidates for reviewer action
- `dedupe_run`: status + metrics for each run
- `entity_audit_view`: canonical row + all member records in one query (cluster audit/lineage)

Canonical survivorship behavior:
- Canonical anchor is still deterministic (`canonical_record_id`).
- For each canonical field, if anchor value is empty, pipeline fills from other members using latest `source_updated_at` (then `record_id` tie-break).
- Field-level provenance is stored in `unique_inmates.canonical_trace_json`.

Audit examples:

```sql
-- one entity, show canonical + every member mapped to it
SELECT *
FROM entity_audit_view
WHERE entity_id = 123
ORDER BY is_canonical_member DESC, member_record_id;

-- find entities with more than one member
SELECT entity_id, MAX(member_count) AS member_count
FROM entity_audit_view
GROUP BY entity_id
HAVING MAX(member_count) > 1
ORDER BY member_count DESC, entity_id
LIMIT 50;

-- show all entities with their members (entity CIF repeated per member row)
SELECT
  entity_id,
  cif_number,
  member_record_id,
  is_canonical_member,
  member_count,
  decision,
  best_match_probability
FROM entity_audit_view
WHERE member_record_id IS NOT NULL
ORDER BY entity_id, is_canonical_member DESC, member_record_id
LIMIT 500;
```

```sql
-- trace where one canonical field came from
SELECT
  entity_id,
  canonical_record_id,
  canonical_nik,
  json_extract_string(canonical_trace_json, '$.canonical_nik.source_record_id') AS nik_from_record_id,
  json_extract_string(canonical_trace_json, '$.canonical_nik.selection') AS nik_selection
FROM unique_inmates
WHERE entity_id = 123;
```

If your DuckDB file already exists, rerun schema init once to create/update the view:

```bash
uv run inmate-dedupe --config config.dummy.yaml init-schema --sql sql/duckdb_schema.sql
```

## Suggested Downstream Pattern

Use `unique_inmates` as your primary “unique inmate” dataset, and join with `record_entity_map` when you need source lineage/history per inmate.
