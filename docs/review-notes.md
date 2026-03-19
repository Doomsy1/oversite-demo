# Review Notes

## Source Contract Used

- Image-bearing rows are `events` records where `blob_url is not null`
- The main join is `events.session_id -> sessions.session_id`
- V1 normalizes `priority`, `trade`, `location`, and `site` when present
- The pipeline is intentionally built around image-bearing events, not one hard-coded event type

## Run Model

- `pipeline_runs` is an execution log
- `sessions`, `images`, `flags`, `image_assets`, `observations`, `graph_nodes`, and `graph_edges` are latest-state materializations
- Reruns update the latest materialized rows and stamp `last_materialized_run_id`; they do not create full per-run snapshots

## V1 Intentionally Does Not Do

- CV inference
- Canonical site hierarchy
- Durable cross-session object identity
- Canonical location resolution from free-text hints

Start with the schema, then normalization, then the pipeline entrypoint:

- [`../sql/001_create_derived_schema.sql`](../sql/001_create_derived_schema.sql)
- [`../src/oversite_pipeline/transform/normalize.py`](../src/oversite_pipeline/transform/normalize.py)
- [`../src/oversite_pipeline/pipeline.py`](../src/oversite_pipeline/pipeline.py)
