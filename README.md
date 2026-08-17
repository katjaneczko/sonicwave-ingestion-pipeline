# SonicWave Ingestion Pipeline

Bronze-to-Silver ingestion pipeline for SonicWave's `plays` (event feed) and
`users` (SCD2 dimension) daily source snapshots, built with PySpark.

## Intro

Two source tables: `plays` and `users`. Both have working Bronze → Silver
pipelines, thin CLI entry-points, and tests. CI (lint → type-check → test)
and a release workflow (build wheel + sdist) run on GitHub Actions.

## Repository layout

```
seed/generate_seed.py          the provided seed generator (source data)
scripts/run_plays.py           thin entry-point for the plays pipeline
scripts/run_users.py           thin entry-point for the users pipeline
src/sonicwave_ingestion_pipeline/
    bronze.py                  shared: permissive read, provenance + partitioned write
    silver.py                  shared: dedup, quarantine split, cast, dimension write
    scd2.py                    shared: SCD2 recompute (union + window functions)
    plays.py                   plays-specific schema, validation rule, orchestration
    users.py                   users-specific schema, validation rule, orchestration
tests/                         pytest + a session-scoped local SparkSession fixture
```

Shared source → Bronze → Silver transformations live in `bronze.py`/`silver.py`/
`scd2.py`; each table's schema, validation rule, and orchestration function
live in its own module and call into the shared helpers. The entry-point
scripts only parse arguments and call the package, no business logic.

## Setup

```
uv sync
uv run pre-commit install
```

## Generate the seed data

```
uv run python seed/generate_seed.py            # -> ./data/source
```

Writes three daily snapshots (`2026-03-01`, `2026-03-02`, `2026-03-03`) for
`plays` and `users` into `./data/source/<table>/<snapshot_date>/`.

## Run the pipeline

```
uv run python scripts/run_plays.py --source ./data/source/plays --snapshot-date 2026-03-01
uv run python scripts/run_users.py --source ./data/source/users --snapshot-date 2026-03-01
```

Run for `2026-03-01`, `2026-03-02`, `2026-03-03` in order to see: Silver
`plays` partitioned by `snapshot_date` with a late-arriving event correctly
dated by `event_date`; Silver `users` accumulating SCD2 history (a closed
version and a current one for every user whose `plan_tier`/`country`
changed); and malformed rows from both tables quarantined under
`data/reject/`.

To prove idempotency: re-run the **most recent** snapshot again and diff
`data/silver/` before/after to see nothing moves (see "Known limitation" below
explainig why only the most recent day run is possible for this project).

## Run the tests / quality gate

```
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```

The same four commands run in CI (`.github/workflows/ci.yml`) on every PR.
`.github/workflows/release.yml` builds the wheel + sdist on a version tag.

## Design decisions

### Bronze: explicit schema, not `inferSchema`

`read_source_permissive` takes a required `schema` argument and reads with
`spark.read.schema(schema).json(path)` rather than letting Spark infer one.
With inferSchema Spark silently drops a column if field is `NULL` 
in every record of a given day's file (e.g. `plays.updated_at`,
which the seed never populates), so Bronze's columns could differ from one 
snapshot to the next depending on which fields happened to be non-null that day. 
An explicit schema guarantees Bronze's shape stays the same.

### Bronze: partitioned Parquet, dynamic partition overwrite

`write_bronze`/`write_silver` partition their output by `snapshot_date` and
write with `spark.sql.sources.partitionOverwriteMode = "dynamic"`. Spark's
default ("static") overwrite mode replaces the entire target path on every
write: re-running the pipeline for one day would remove every other day's data. 
Dynamic mode scopes the overwrite only to the partitions present in the DataFrame being written.
Covered by
`test_write_bronze_partition_overwrite_is_dynamic` and
`test_rerunning_same_snapshot_does_not_duplicate_rows`.

### Bronze partition columns stay untyped

Reading a partitioned path back with Spark's default settings infers a type
for the partition column from the folder name (e.g. `snapshot_date=2026-03-01`
comes back as a `DateType`, not the string that was written). Bronze should
land data as-is, so `spark.sql.sources.partitionColumnTypeInference` is
disabled where `SparkSession` is created:
`spark_session.build_spark_session` (used by both `scripts/run_*.py`
entry-points) and
`tests/conftest.py`'s `spark` fixture so `snapshot_date` reads back as plain
string it was written

### Provenance columns

Every Bronze row gets `ingested_at` (write time), `source_file` (which part
file it came from), and `snapshot_date` (which day's drop it belongs to) added in `write_bronze`.

### Silver: `try_cast`, not `cast`

`cast_to_schema` casts Bronze's all-string columns to their typed Silver
equivalents using `Column.try_cast`, not `Column.cast`. Under Spark's default
ANSI mode, casting a malformed value (e.g. `ms_played = "NaN"` to `long`) 
with a plain `cast` raises error and aborts the write. `try_cast` returns
`NULL` instead so the row survives to be caught and quarantined by `split_valid_invalid`.

### Silver: quarantine is null-safe

`split_valid_invalid` runs `is_valid` condition through `F.coalesce(is_valid, F.lit(False))`
before splitting. Without it a row with check resulting in `NULL` (e.g. comparing a field
that is itself `NULL`) would not satisfy `filter(is_valid)` nor `filter(~is_valid)` and dropped.
Coalescing to `False` guarantees every row lands in exactly one of the two outputs.

### `plays`: partition by `snapshot_date`, date by `event_date`

Silver `plays` is partitioned by `snapshot_date` (the drop it arrived in),
not `event_date` (when it actually happened). A late-arriving event (played
on day T, recorded in the T+1 drop) is physically stored in the T+1
partition - re-running T+1 only rewrites T+1's data but carries an
`event_date` column derived from `played_at`. Query by event time sees date pf the event set to T.
Partitioning Silver by event_date would force the later drop to rewrite 
an earlier partition and rows already there, which is what this partitioning design avoids.

### `users`: SCD2 without `MERGE`

`scd2.apply_scd2` recomputes the whole dimension each run: it compares
today's deduplicated observations against each key's currently open version,
and unions three groups - historical rows, closed old versions, newly
opened versions. `valid_to`/`is_current` are then recomputed by `F.lead()` 
window function over `valid_from` per key - a version's `valid_to` is the next version's `valid_from`
and `is_current` to FALSE meaning there is no next version. Re-applying the same day's
data changes no tracked attribute, so no version opens twice - idempotent.

Versions are ordered by `coalesce(updated_at, created_at)`: `updated_at` is
`NULL` until a row changes, so this is `created_at` until then, and
`updated_at` after. The same expression is the dedup key for collapsing
duplicate rows within a day's drop.

### Known limitation: out-of-order re-run of `users`

Re-running the **most recent** snapshot is fully idempotent (verified against
the seed). Re-running an **older** one *after* later snapshots changed
the tracked attributes is not: `apply_scd2` compares against the
dimension's *current* state, not the state of older date, so it can
misread already applied history as new. For this operation to be indepotent it would need
a point-in-time join against `valid_from`/`valid_to` instead of an `is_current` filter.

### Windows-local Spark settings

Two local-environment fixes in `seed/generate_seed.py`,
`scripts/run_*.py`, and `tests/conftest.py`:
- `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` are pinned to `sys.executable`.
  On Windows, a bare `python` on `PATH` can resolve to the Microsoft Store's
  App Execution Alias stub instead of this project's venv interpreter, which
  makes the PySpark worker process fail to start (`Python worker failed to
  connect back`).
- `spark.driver.host` / `spark.driver.bindAddress` are pinned to `127.0.0.1`
  to avoid Spark trying to bind to a non-loopback network interface.

*These are workarounds only for the local machine development*

## Testing approach

Unit tests (`test_bronze.py`, `test_silver.py`, `test_scd2.py`) drive each
helper with small, hand-built DataFrames or JSON fixtures, each asserting one
operation (deduplication, the null-condition quarantine, an SCD2 version close).
Integration tests (`test_plays.py`, `test_users.py`) run the full
`run_plays`/`run_users` chain through real `tmp_path` file I/O
and catch connecting issues, e.g. computed column (`effective_at`) never attached via `.withColumn`, 
or `snapshot_date` dropped by `cast_to_schema` before needed.
