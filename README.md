# Oversite: ML Data Pipeline — Supabase Image Store

This submission builds the pipeline requested in the task:

- read image-bearing walkthrough events from Supabase
- normalize them into a separate derived layer
- expose a lightweight queryable graph in Postgres terms
- document the source contract and schema choices clearly

## Submission Status

The live Oversite Supabase project was available to me for read access on the source tables and storage paths, but I did not have the project-level access needed to expose a new `derived` schema through the Supabase REST API settings.

That means:

- I **did validate the live source contract** against `public.events`, `public.sessions`, and storage URL patterns
- I **did build the write path** for `derived.*` through the Supabase REST API
- I **could not execute the final write step on the live project** because the `derived` schema was not exposed and I could not change that project setting myself

To make the submission still verifiable end to end, I included:

- the SQL files needed to create the `derived` schema and views
- a live read-only dry-run path against the configured Supabase project
- a local SQLite inspection path that mirrors the derived tables and views closely enough to exercise the pipeline behavior
- tests covering normalization, graph construction, asset inspection, CLI flow, and local query surfaces

## What I Built

- extraction from `public.events` and `public.sessions`
- normalization into `derived.pipeline_runs`, `derived.sessions`, `derived.images`, and `derived.flags`
- a lightweight graph in `derived.graph_nodes` and `derived.graph_edges`
- read views for inspection and downstream querying
- a CLI for dry runs and full pipeline execution once the `derived` schema is exposed

The extraction rule is source-backed and simple: if an `events` row has a non-null `blob_url`, it is treated as an image-bearing record.

## Live Source Validation

This pipeline depends on:

- image-bearing records are `events` rows with `blob_url is not null`
- the authoritative join is `events.session_id -> sessions.session_id`
- `priority`, `trade`, and `location` are the main metadata fields worth normalizing in v1
- `blob_url` encodes stable storage provenance that can be parsed into bucket and path

One important limitation of the current source is that `location` behaves like a raw hint rather than a canonical place model. In the live data, values can differ in formatting and specificity, for example `east wall`, `East wall, stairwell`, and `near the east wall`. I therefore keep it as `location_hint` in v1 and treat normalization or place-resolution as a later step rather than overstating what the source already supports.

## Why The Derived Layer Is Separate

I kept the ML-facing structure in a separate `derived` schema so the pipeline does not mutate or overload product telemetry tables. That makes the system easier to reason about and keeps source truth distinct from normalization and graph logic.

The core graph is intentionally relational and lightweight:

- image `occurred_in_session` session
- session `belongs_to_company` company when company context exists
- session `captured_by_device` device
- image `has_flag` flag
- image `associated_with_trade` trade
- image `associated_with_location_hint` location_hint

That is enough to support useful queries now without claiming stronger semantics than the source can defend.

## Extra Additions Beyond The Minimal V1

I added a few pieces beyond the strict minimum because I think they improve the handoff and make the dataset more inspectable, but they are intentionally kept separate from the core normalized layer:

- `derived.image_assets`: technical provenance for downloaded assets such as fetch status, MIME type, dimensions, byte size, and hash
- `derived.observations`: deterministic weak labels derived from explicit metadata and narrow text rules
- `derived.v_ml_dataset_manifest`: a manifest-style view for downstream inspection and export

These are not meant as ground-truth visual annotations. They are lightweight, explainable helpers for dataset inspection and future iteration.

## What I Intentionally Did Not Do

- no CV inference
- no canonical site hierarchy
- no durable object identity across sessions
- no speculative graph infrastructure beyond Postgres tables
- no claim that weak text-derived labels are equivalent to model-grade visual labels

## How To Run

Install the package:

```powershell
py -3 -m pip install -e .
```

Populate `.env` from `.env.example`:

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`
- `SUPABASE_SCHEMA`
- `DERIVED_SCHEMA`
- `SOURCE_BUCKET`
- `PIPELINE_PAGE_SIZE`
- `PIPELINE_FETCH_TIMEOUT_SECONDS`
- `PIPELINE_EXPORT_PATH`

### Read-Only Live Check

This path only reads from the configured Supabase source tables:

```powershell
oversite-pipeline run --dry-run --limit 10
```

### Full Write Run

This path is ready once the project exposes the `derived` schema through the Supabase API settings and the SQL below has been applied:

- `sql/001_create_derived_schema.sql`
- `sql/002_create_derived_views.sql`

Then run:

```powershell
oversite-pipeline run --limit 50 --export-manifest
```

Useful flags:

- `--skip-fetch`
- `--window-start`
- `--window-end`
- `--manifest-path`

### Local Inspection Path

For a self-contained end-to-end inspection without changing the live project settings:

```powershell
py -3 scripts/run_local_pipeline.py --limit 20
```

That path reads from the live source, builds the derived records locally, and loads them into a SQLite mirror so the query surfaces can still be inspected.

## What To Inspect After Enablement

Core tables:

- `derived.sessions`
- `derived.images`
- `derived.flags`
- `derived.graph_nodes`
- `derived.graph_edges`

Useful extensions:

- `derived.image_assets`
- `derived.observations`
- `derived.v_image_enriched`
- `derived.v_graph_edges_enriched`
- `derived.v_ml_dataset_manifest`

If I were walking through it live, I would inspect:

1. `derived.images` to show the normalized image records.
2. `derived.flags` to show explicit issue state outside raw JSON.
3. `derived.v_graph_edges_enriched` to show the graph relationships.
4. `derived.v_ml_dataset_manifest` to show the downstream dataset interface.

## Repo Map

- `src/oversite_pipeline`: pipeline code
- `sql`: derived schema and views
- `scripts`: helper scripts, including the local SQLite inspection path
- `tests`: unit and smoke-style tests

The best technical starting points are:

- `src/oversite_pipeline/pipeline.py`
- `src/oversite_pipeline/transform/normalize.py`
- `src/oversite_pipeline/transform/graph.py`
- `sql/001_create_derived_schema.sql`
