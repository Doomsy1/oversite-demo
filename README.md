# Oversite: ML Data Pipeline — Supabase Image Store

I finished the image-pipeline take-home as far as the current Supabase permissions allowed.

The core deliverable is here:

- image-bearing event extraction from Supabase
- normalized `derived` tables for sessions, images, flags, and pipeline runs
- a lightweight queryable graph in Postgres terms
- SQL + Python + tests + a local inspection path so the whole thing is easy to review

I kept the implementation pretty practical. The point of this version was not to get fancy with graph infrastructure or pretend the source is cleaner than it is. The point was to build a solid first derived layer on top of real walkthrough data and make the design choices easy to defend in a walkthrough.

## What I Shipped

- extraction from `public.events` and `public.sessions`
- normalization into `derived.pipeline_runs`, `derived.sessions`, `derived.images`, and `derived.flags`
- a lightweight graph in `derived.graph_nodes` and `derived.graph_edges`
- read views for inspection and downstream querying
- a CLI for dry runs and full pipeline execution once the `derived` schema is exposed
- a local SQLite mirror so the pipeline can still be reviewed end to end without changing project settings
- tests covering normalization, graph construction, asset inspection, CLI flow, pagination, and local query surfaces

The extraction rule is intentionally simple and source-backed: if an `events` row has a non-null `blob_url`, I treat it as an image-bearing record.

## What I Validated Against The Live Supabase Project

I did a read-only pass against the configured Oversite project and built the pipeline around what was actually there, not around guessed schema.

The important source contract is:

- image-bearing records are `events` rows with `blob_url is not null`
- the main join is `events.session_id -> sessions.session_id`
- `priority`, `trade`, and `location` are the most useful metadata fields to normalize in v1
- `blob_url` contains stable storage provenance that can be parsed into bucket and path

One thing I did not want to overclaim: `location` is useful, but it is not canonical. In the live data it behaves more like a free-text hint than a clean place model, so I kept it as `location_hint` instead of pretending there is already a real site hierarchy hiding in the source.

## Current Limitation

I had read access to the live source tables and storage paths, but I did not have the project-level permission needed to expose a new `derived` schema through Supabase REST API settings.

So the status is:

- I **did validate the live source contract** against `public.events`, `public.sessions`, and storage URL patterns
- I **did build the write path** for `derived.*`
- I **could not execute the final live write step myself** because `derived` was not exposed and I could not change that project setting

To keep the submission reviewable anyway, I included:

- the SQL files needed to create the `derived` schema and views
- a live read-only dry-run path against the configured Supabase project
- a local SQLite inspection path that mirrors the derived tables and views closely enough to exercise pipeline behavior
- a green automated test suite

## Why I Kept The Derived Layer Separate

I kept the ML-facing structure in a separate `derived` schema so the pipeline does not mutate or overload product telemetry tables. That keeps source truth separate from normalization and graph logic, which makes the pipeline easier to reason about and easier to extend later.

The core graph is intentionally lightweight:

- image `occurred_in_session` session
- session `belongs_to_company` company when company context exists
- session `captured_by_device` device
- image `has_flag` flag
- image `associated_with_trade` trade
- image `associated_with_location_hint` location_hint

That gives a useful relational layer right away without pretending the source can already support stronger semantics than it actually can.

## Extra Pieces I Added

I added a few extras beyond the bare minimum because they make the dataset easier to inspect and more useful for follow-on ML work, but I kept them separate from the core normalized layer:

- `derived.image_assets`: technical provenance for downloaded assets such as fetch status, MIME type, dimensions, byte size, and hash
- `derived.observations`: deterministic weak labels derived from explicit metadata and narrow text rules
- `derived.v_ml_dataset_manifest`: a manifest-style view for downstream inspection and export

The key point here is that `observations` are weak labels from source metadata and source text. They are not visual annotations, not CV output, and not model-grade labels. I added them as a lightweight inspection layer, not as a claim that the image-labeling problem is already solved.

## What I Deliberately Did Not Do

- no CV inference
- no canonical site hierarchy
- no durable object identity across sessions
- no speculative graph infrastructure beyond Postgres tables
- no claim that weak metadata/text-derived labels are equivalent to model-grade visual labels

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

This is the quickest way to verify the live source contract without touching the project:

```powershell
oversite-pipeline run --dry-run --limit 10
```

### Full Write Run

Once the project exposes the `derived` schema through Supabase API settings and the SQL below has been applied, this is ready to run:

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

For a self-contained review path without changing live project settings:

```powershell
py -3 scripts/run_local_pipeline.py --limit 20
```

That path reads from the live source, builds the derived records locally, and loads them into a SQLite mirror so the normalized tables, graph, and derived views can all be inspected.

## How I Would Walk Through It

If we were reviewing this live, I would start here:

- `derived.sessions`
- `derived.images`
- `derived.flags`
- `derived.graph_nodes`
- `derived.graph_edges`

Then I would use these as the fastest way to inspect the result:

- `derived.image_assets`
- `derived.observations`
- `derived.v_image_enriched`
- `derived.v_graph_edges_enriched`
- `derived.v_ml_dataset_manifest`

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

## If I Kept Going Next

If I had the extra permissions and a bit more time, the next steps would be pretty straightforward:

- run the pipeline directly into the live `derived` schema
- add a cleaner review/query surface for recurring hazards by trade and location
- harden incremental sync behavior for larger source volumes
- add a proper labeling/enrichment layer once there is a decision on how much of that should come from metadata, human review, or vision models

For this take-home, I intentionally stopped before inventing semantics the current source does not really support.
