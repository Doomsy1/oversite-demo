# Oversite: ML Image Derived Layer

This repo builds a latest-state derived layer from image-bearing Supabase `events` and related `sessions`. It ships the pipeline code, SQL schema/views, a local SQLite review path, and tests so the review surface is fast and concrete.

## Review This Repo In 3 Places

- [`src/oversite_pipeline/pipeline.py`](src/oversite_pipeline/pipeline.py)
- [`src/oversite_pipeline/transform/normalize.py`](src/oversite_pipeline/transform/normalize.py)
- [`sql/001_create_derived_schema.sql`](sql/001_create_derived_schema.sql)

Short context:

- [`docs/review-notes.md`](docs/review-notes.md)

## What's Included

- Latest-state `derived` tables for pipeline runs, sessions, images, flags, assets, observations, graph nodes, and graph edges
- SQL schema and read views for inspection
- CLI entrypoint for dry runs and full runs
- Local SQLite mirror for review without changing live project settings
- Tests covering normalization, extraction, graph building, CLI flow, assets, and local inspection

## Run Locally

Install:

```powershell
py -3 -m pip install -e .
```

Set these values in `.env` from `.env.example`:

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`
- `SUPABASE_SCHEMA`
- `DERIVED_SCHEMA`
- `SOURCE_BUCKET`
- `PIPELINE_PAGE_SIZE`
- `PIPELINE_FETCH_TIMEOUT_SECONDS`
- `PIPELINE_EXPORT_PATH`

Read-only dry run:

```powershell
oversite-pipeline run --dry-run --limit 10
```

Local SQLite review path:

```powershell
py -3 scripts/run_local_pipeline.py --limit 20
```

Full run after applying the SQL files:

```powershell
oversite-pipeline run --limit 50 --export-manifest
```

## Current Live Constraint

Live writes require the Supabase project to expose the `derived` schema through the REST API settings.
