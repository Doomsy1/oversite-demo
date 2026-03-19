create extension if not exists pgcrypto;

create schema if not exists derived;

create table if not exists derived.pipeline_runs (
    id uuid primary key default gen_random_uuid(),
    started_at timestamptz not null default now(),
    completed_at timestamptz null,
    status text not null,
    code_version text null,
    notes text null,
    source_window_start timestamptz null,
    source_window_end timestamptz null,
    constraint pipeline_runs_status_check
        check (status in ('running', 'completed', 'failed'))
);

create table if not exists derived.sessions (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    source_session_row_id text not null,
    source_session_id text not null,
    company_id text null,
    device_id text null,
    vertical_id text null,
    started_at timestamptz null,
    ended_at timestamptz null,
    status text null,
    source_event_count integer null,
    source_flag_count integer null,
    created_at timestamptz null,
    raw_session jsonb not null
);

create unique index if not exists derived_sessions_source_session_row_id_uq
    on derived.sessions (source_session_row_id);

create unique index if not exists derived_sessions_source_session_id_uq
    on derived.sessions (source_session_id);

create index if not exists derived_sessions_pipeline_run_id_idx
    on derived.sessions (last_materialized_run_id);

create index if not exists derived_sessions_company_id_idx
    on derived.sessions (company_id);

create index if not exists derived_sessions_device_id_idx
    on derived.sessions (device_id);

create table if not exists derived.images (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    derived_session_id uuid not null references derived.sessions(id),
    source_event_id text not null,
    source_session_id text not null,
    company_id text null,
    device_id text null,
    user_id text null,
    event_timestamp timestamptz not null,
    event_type text null,
    event_summary text null,
    priority text null,
    blob_url text not null,
    public_image_url text null,
    storage_bucket text not null,
    storage_path text not null,
    trade text null,
    location_hint text null,
    site_hint text null,
    is_flagged boolean not null default false,
    image_fetch_status text not null default 'pending',
    image_sha256 text null,
    image_mime_type text null,
    image_width integer null,
    image_height integer null,
    image_bytes bigint null,
    raw_metadata jsonb null,
    raw_event jsonb not null
);

create unique index if not exists derived_images_source_event_id_uq
    on derived.images (source_event_id);

create index if not exists derived_images_pipeline_run_id_idx
    on derived.images (last_materialized_run_id);

create index if not exists derived_images_derived_session_id_idx
    on derived.images (derived_session_id);

create index if not exists derived_images_source_session_id_idx
    on derived.images (source_session_id);

create index if not exists derived_images_company_id_idx
    on derived.images (company_id);

create index if not exists derived_images_device_id_idx
    on derived.images (device_id);

create index if not exists derived_images_event_timestamp_idx
    on derived.images (event_timestamp);

create index if not exists derived_images_event_type_idx
    on derived.images (event_type);

create index if not exists derived_images_is_flagged_idx
    on derived.images (is_flagged);

create index if not exists derived_images_priority_idx
    on derived.images (priority);

create index if not exists derived_images_trade_idx
    on derived.images (trade);

create index if not exists derived_images_location_hint_idx
    on derived.images (location_hint);

create table if not exists derived.flags (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    derived_image_id uuid not null references derived.images(id),
    flag_type text not null,
    flag_value text not null,
    source_kind text not null,
    raw_fragment jsonb null
);

create unique index if not exists derived_flags_image_type_value_uq
    on derived.flags (derived_image_id, flag_type, flag_value);

create index if not exists derived_flags_pipeline_run_id_idx
    on derived.flags (last_materialized_run_id);

create index if not exists derived_flags_derived_image_id_idx
    on derived.flags (derived_image_id);

create index if not exists derived_flags_flag_type_idx
    on derived.flags (flag_type);

create index if not exists derived_flags_flag_value_idx
    on derived.flags (flag_value);

create table if not exists derived.image_assets (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    derived_image_id uuid not null references derived.images(id),
    source_bucket text not null,
    source_path text not null,
    source_url text not null,
    fetch_status text not null,
    fetch_http_status integer null,
    fetched_at timestamptz null,
    byte_size bigint null,
    sha256 text null,
    mime_type text null,
    width integer null,
    height integer null,
    curated_bucket text null,
    curated_path text null,
    raw_headers jsonb null,
    raw_error jsonb null
);

create unique index if not exists derived_image_assets_image_id_uq
    on derived.image_assets (derived_image_id);

create index if not exists derived_image_assets_fetch_status_idx
    on derived.image_assets (fetch_status);

create table if not exists derived.observations (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    derived_image_id uuid not null references derived.images(id),
    observation_family text not null,
    observation_label text not null,
    canonical_key text not null,
    confidence text not null,
    source_kind text not null,
    rule_id text not null,
    evidence_text text null,
    evidence_metadata jsonb null,
    raw_fragment jsonb null
);

create unique index if not exists derived_observations_image_family_key_uq
    on derived.observations (derived_image_id, observation_family, canonical_key);

create index if not exists derived_observations_family_idx
    on derived.observations (observation_family);

create index if not exists derived_observations_image_id_idx
    on derived.observations (derived_image_id);

create table if not exists derived.graph_nodes (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    node_type text not null,
    node_key text not null,
    display_label text not null,
    source_kind text not null,
    attributes jsonb null
);

create unique index if not exists derived_graph_nodes_type_key_uq
    on derived.graph_nodes (node_type, node_key);

create index if not exists derived_graph_nodes_pipeline_run_id_idx
    on derived.graph_nodes (last_materialized_run_id);

create index if not exists derived_graph_nodes_node_type_idx
    on derived.graph_nodes (node_type);

create table if not exists derived.graph_edges (
    id uuid primary key default gen_random_uuid(),
    last_materialized_run_id uuid not null references derived.pipeline_runs(id),
    src_node_id uuid not null references derived.graph_nodes(id),
    edge_type text not null,
    dst_node_id uuid not null references derived.graph_nodes(id),
    evidence_image_id uuid null references derived.images(id),
    evidence_session_id uuid null references derived.sessions(id),
    source_kind text not null,
    attributes jsonb null
);

create unique index if not exists derived_graph_edges_src_type_dst_uq
    on derived.graph_edges (src_node_id, edge_type, dst_node_id);

create index if not exists derived_graph_edges_pipeline_run_id_idx
    on derived.graph_edges (last_materialized_run_id);

create index if not exists derived_graph_edges_src_node_id_idx
    on derived.graph_edges (src_node_id);

create index if not exists derived_graph_edges_dst_node_id_idx
    on derived.graph_edges (dst_node_id);

create index if not exists derived_graph_edges_edge_type_idx
    on derived.graph_edges (edge_type);
