from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


TABLE_COLUMNS = {
    "pipeline_runs": (
        "id",
        "started_at",
        "completed_at",
        "status",
        "code_version",
        "notes",
        "source_window_start",
        "source_window_end",
    ),
    "sessions": (
        "id",
        "last_materialized_run_id",
        "source_session_row_id",
        "source_session_id",
        "company_id",
        "device_id",
        "vertical_id",
        "started_at",
        "ended_at",
        "status",
        "source_event_count",
        "source_flag_count",
        "created_at",
        "raw_session",
    ),
    "images": (
        "id",
        "last_materialized_run_id",
        "derived_session_id",
        "source_event_id",
        "source_session_id",
        "company_id",
        "device_id",
        "user_id",
        "event_timestamp",
        "event_type",
        "event_summary",
        "priority",
        "blob_url",
        "public_image_url",
        "storage_bucket",
        "storage_path",
        "trade",
        "location_hint",
        "site_hint",
        "is_flagged",
        "image_fetch_status",
        "image_sha256",
        "image_mime_type",
        "image_width",
        "image_height",
        "image_bytes",
        "raw_metadata",
        "raw_event",
    ),
    "flags": (
        "id",
        "last_materialized_run_id",
        "derived_image_id",
        "flag_type",
        "flag_value",
        "source_kind",
        "raw_fragment",
    ),
    "image_assets": (
        "id",
        "last_materialized_run_id",
        "derived_image_id",
        "source_bucket",
        "source_path",
        "source_url",
        "fetch_status",
        "fetch_http_status",
        "fetched_at",
        "byte_size",
        "sha256",
        "mime_type",
        "width",
        "height",
        "curated_bucket",
        "curated_path",
        "raw_headers",
        "raw_error",
    ),
    "observations": (
        "id",
        "last_materialized_run_id",
        "derived_image_id",
        "observation_family",
        "observation_label",
        "canonical_key",
        "confidence",
        "source_kind",
        "rule_id",
        "evidence_text",
        "evidence_metadata",
        "raw_fragment",
    ),
    "graph_nodes": (
        "id",
        "last_materialized_run_id",
        "node_type",
        "node_key",
        "display_label",
        "source_kind",
        "attributes",
    ),
    "graph_edges": (
        "id",
        "last_materialized_run_id",
        "src_node_id",
        "edge_type",
        "dst_node_id",
        "evidence_image_id",
        "evidence_session_id",
        "source_kind",
        "attributes",
    ),
}


def open_local_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute("attach database ? as derived", (str(db_path),))
    return conn


def initialize_local_sqlite(conn: sqlite3.Connection) -> None:
    statements = (
        """
        create table if not exists derived.pipeline_runs (
            id text primary key,
            started_at text null,
            completed_at text null,
            status text not null,
            code_version text null,
            notes text null,
            source_window_start text null,
            source_window_end text null,
            check (status in ('running', 'completed', 'failed'))
        )
        """,
        """
        create table if not exists derived.sessions (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            source_session_row_id text not null unique,
            source_session_id text not null unique,
            company_id text null,
            device_id text null,
            vertical_id text null,
            started_at text null,
            ended_at text null,
            status text null,
            source_event_count integer null,
            source_flag_count integer null,
            created_at text null,
            raw_session text not null
        )
        """,
        """
        create table if not exists derived.images (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            derived_session_id text not null references sessions(id),
            source_event_id text not null unique,
            source_session_id text not null,
            company_id text null,
            device_id text null,
            user_id text null,
            event_timestamp text not null,
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
            is_flagged integer not null default 0 check (is_flagged in (0, 1)),
            image_fetch_status text not null default 'pending',
            image_sha256 text null,
            image_mime_type text null,
            image_width integer null,
            image_height integer null,
            image_bytes integer null,
            raw_metadata text null,
            raw_event text not null
        )
        """,
        """
        create table if not exists derived.flags (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            derived_image_id text not null references images(id),
            flag_type text not null,
            flag_value text not null,
            source_kind text not null,
            raw_fragment text null,
            unique (derived_image_id, flag_type, flag_value)
        )
        """,
        """
        create table if not exists derived.image_assets (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            derived_image_id text not null unique references images(id),
            source_bucket text not null,
            source_path text not null,
            source_url text not null,
            fetch_status text not null,
            fetch_http_status integer null,
            fetched_at text null,
            byte_size integer null,
            sha256 text null,
            mime_type text null,
            width integer null,
            height integer null,
            curated_bucket text null,
            curated_path text null,
            raw_headers text null,
            raw_error text null
        )
        """,
        """
        create table if not exists derived.observations (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            derived_image_id text not null references images(id),
            observation_family text not null,
            observation_label text not null,
            canonical_key text not null,
            confidence text not null,
            source_kind text not null,
            rule_id text not null,
            evidence_text text null,
            evidence_metadata text null,
            raw_fragment text null,
            unique (derived_image_id, observation_family, canonical_key)
        )
        """,
        """
        create table if not exists derived.graph_nodes (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            node_type text not null,
            node_key text not null,
            display_label text not null,
            source_kind text not null,
            attributes text null,
            unique (node_type, node_key)
        )
        """,
        """
        create table if not exists derived.graph_edges (
            id text primary key,
            last_materialized_run_id text not null references pipeline_runs(id),
            src_node_id text not null references graph_nodes(id),
            edge_type text not null,
            dst_node_id text not null references graph_nodes(id),
            evidence_image_id text null references images(id),
            evidence_session_id text null references sessions(id),
            source_kind text not null,
            attributes text null,
            unique (src_node_id, edge_type, dst_node_id)
        )
        """,
        """
        create index if not exists derived.derived_sessions_pipeline_run_id_idx
        on sessions (last_materialized_run_id)
        """,
        """
        create index if not exists derived.derived_sessions_company_id_idx
        on sessions (company_id)
        """,
        """
        create index if not exists derived.derived_sessions_device_id_idx
        on sessions (device_id)
        """,
        """
        create index if not exists derived.derived_images_pipeline_run_id_idx
        on images (last_materialized_run_id)
        """,
        """
        create index if not exists derived.derived_images_derived_session_id_idx
        on images (derived_session_id)
        """,
        """
        create index if not exists derived.derived_images_source_session_id_idx
        on images (source_session_id)
        """,
        """
        create index if not exists derived.derived_images_company_id_idx
        on images (company_id)
        """,
        """
        create index if not exists derived.derived_images_device_id_idx
        on images (device_id)
        """,
        """
        create index if not exists derived.derived_images_event_timestamp_idx
        on images (event_timestamp)
        """,
        """
        create index if not exists derived.derived_images_event_type_idx
        on images (event_type)
        """,
        """
        create index if not exists derived.derived_images_is_flagged_idx
        on images (is_flagged)
        """,
        """
        create index if not exists derived.derived_images_priority_idx
        on images (priority)
        """,
        """
        create index if not exists derived.derived_images_trade_idx
        on images (trade)
        """,
        """
        create index if not exists derived.derived_images_location_hint_idx
        on images (location_hint)
        """,
        """
        create index if not exists derived.derived_flags_pipeline_run_id_idx
        on flags (last_materialized_run_id)
        """,
        """
        create index if not exists derived.derived_flags_derived_image_id_idx
        on flags (derived_image_id)
        """,
        """
        create index if not exists derived.derived_flags_flag_type_idx
        on flags (flag_type)
        """,
        """
        create index if not exists derived.derived_flags_flag_value_idx
        on flags (flag_value)
        """,
        """
        create index if not exists derived.derived_image_assets_fetch_status_idx
        on image_assets (fetch_status)
        """,
        """
        create index if not exists derived.derived_observations_family_idx
        on observations (observation_family)
        """,
        """
        create index if not exists derived.derived_observations_image_id_idx
        on observations (derived_image_id)
        """,
        """
        create index if not exists derived.derived_graph_nodes_pipeline_run_id_idx
        on graph_nodes (last_materialized_run_id)
        """,
        """
        create index if not exists derived.derived_graph_nodes_node_type_idx
        on graph_nodes (node_type)
        """,
        """
        create index if not exists derived.derived_graph_edges_pipeline_run_id_idx
        on graph_edges (last_materialized_run_id)
        """,
        """
        create index if not exists derived.derived_graph_edges_src_node_id_idx
        on graph_edges (src_node_id)
        """,
        """
        create index if not exists derived.derived_graph_edges_dst_node_id_idx
        on graph_edges (dst_node_id)
        """,
        """
        create index if not exists derived.derived_graph_edges_edge_type_idx
        on graph_edges (edge_type)
        """,
        """
        create view if not exists derived.v_image_enriched as
        with flag_rollup as (
            select
                derived_image_id,
                count(*) as flag_count,
                group_concat(flag_type || ':' || flag_value, ', ') as flag_summary
            from flags
            group by derived_image_id
        )
        select
            i.id as image_id,
            i.last_materialized_run_id,
            i.derived_session_id,
            i.source_event_id,
            i.source_session_id,
            i.company_id,
            i.device_id,
            i.user_id,
            i.event_timestamp,
            i.event_type,
            i.event_summary,
            i.priority,
            i.blob_url,
            i.public_image_url,
            i.storage_bucket,
            i.storage_path,
            i.trade,
            i.location_hint,
            i.site_hint,
            i.is_flagged,
            i.image_fetch_status,
            i.image_sha256,
            i.image_mime_type,
            i.image_width,
            i.image_height,
            i.image_bytes,
            coalesce(fr.flag_count, 0) as flag_count,
            fr.flag_summary,
            s.started_at as session_started_at,
            s.ended_at as session_ended_at,
            s.status as session_status
        from images i
        join sessions s
            on s.id = i.derived_session_id
        left join flag_rollup fr
            on fr.derived_image_id = i.id
        """,
        """
        create view if not exists derived.v_graph_edges_enriched as
        select
            e.id as edge_id,
            e.last_materialized_run_id,
            e.edge_type,
            src.node_type as src_node_type,
            src.node_key as src_node_key,
            src.display_label as src_node_label,
            dst.node_type as dst_node_type,
            dst.node_key as dst_node_key,
            dst.display_label as dst_node_label,
            e.evidence_image_id,
            e.evidence_session_id,
            e.source_kind,
            e.attributes,
            coalesce(
                cast(json_extract(e.attributes, '$.evidence_image_count') as integer),
                case when e.evidence_image_id is null then 0 else 1 end
            ) as evidence_image_count,
            coalesce(
                cast(json_extract(e.attributes, '$.evidence_session_count') as integer),
                case when e.evidence_session_id is null then 0 else 1 end
            ) as evidence_session_count,
            json_extract(e.attributes, '$.first_seen_at') as first_seen_at,
            json_extract(e.attributes, '$.last_seen_at') as last_seen_at
        from graph_edges e
        join graph_nodes src
            on src.id = e.src_node_id
        join graph_nodes dst
            on dst.id = e.dst_node_id
        """,
        """
        create view if not exists derived.v_entity_location_recurrence as
        select
            edge_type,
            src_node_type as entity_type,
            src_node_label as entity_label,
            dst_node_label as location_hint,
            evidence_image_count,
            evidence_session_count,
            first_seen_at,
            last_seen_at
        from derived.v_graph_edges_enriched
        where edge_type in ('hazard_near_location_hint', 'equipment_at_location_hint')
        order by
            evidence_session_count desc,
            evidence_image_count desc,
            edge_type,
            entity_label,
            location_hint
        """,
        """
        create view if not exists derived.v_hazard_location_recurrence as
        select
            entity_label as hazard,
            location_hint,
            evidence_image_count,
            evidence_session_count,
            first_seen_at,
            last_seen_at
        from derived.v_entity_location_recurrence
        where edge_type = 'hazard_near_location_hint'
        order by
            evidence_session_count desc,
            evidence_image_count desc,
            hazard,
            location_hint
        """,
        """
        create view if not exists derived.v_trade_location_recurrence as
        with trade_observations as (
            select
                derived_image_id,
                observation_label as trade_label,
                canonical_key as normalized_trade_key
            from observations
            where observation_family = 'trade'
        ),
        location_observations as (
            select
                derived_image_id,
                observation_label as location_label,
                canonical_key as normalized_location_hint_key
            from observations
            where observation_family = 'location_hint'
        )
        select
            min(t.trade_label) as trade,
            min(l.location_label) as location_hint,
            count(distinct i.image_id) as image_count,
            count(distinct i.source_session_id) as session_count,
            sum(case when i.is_flagged = 1 then 1 else 0 end) as flagged_image_count,
            min(i.event_timestamp) as first_seen_at,
            max(i.event_timestamp) as last_seen_at,
            t.normalized_trade_key,
            l.normalized_location_hint_key
        from derived.v_image_enriched i
        join trade_observations t
            on t.derived_image_id = i.image_id
        join location_observations l
            on l.derived_image_id = i.image_id
        group by
            t.normalized_trade_key,
            l.normalized_location_hint_key
        order by
            session_count desc,
            image_count desc,
            trade,
            location_hint
        """,
        """
        create view if not exists derived.v_session_image_summary as
        select
            s.id as derived_session_id,
            s.source_session_id,
            s.company_id,
            s.device_id,
            count(i.id) as image_count,
            sum(case when i.is_flagged = 1 then 1 else 0 end) as flagged_image_count,
            s.started_at,
            s.ended_at,
            s.status
        from sessions s
        left join images i
            on i.derived_session_id = s.id
        group by
            s.id,
            s.source_session_id,
            s.company_id,
            s.device_id,
            s.started_at,
            s.ended_at,
            s.status
        """,
        """
        create view if not exists derived.v_ml_dataset_manifest as
        with observation_rollup as (
            select
                derived_image_id,
                json_group_array(
                    json_object(
                        'family', observation_family,
                        'label', observation_label,
                        'canonical_key', canonical_key,
                        'confidence', confidence,
                        'source_kind', source_kind
                    )
                ) as observations_json
            from observations
            group by derived_image_id
        )
        select
            i.id as image_id,
            i.source_event_id,
            i.source_session_id,
            a.fetch_status,
            a.source_bucket,
            a.source_path,
            a.source_url,
            a.sha256,
            a.mime_type,
            a.width,
            a.height,
            a.byte_size,
            i.event_timestamp,
            i.event_type,
            i.event_summary,
            i.priority,
            i.trade,
            i.location_hint,
            i.site_hint,
            i.company_id,
            i.device_id,
            i.user_id,
            coalesce(oroll.observations_json, '[]') as observations_json
        from images i
        left join image_assets a
            on a.derived_image_id = i.id
        left join observation_rollup oroll
            on oroll.derived_image_id = i.id
        """,
    )
    for statement in statements:
        conn.execute(statement)
    conn.commit()


def _encode_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    return value


def _prepare_row(table: str, row: dict[str, Any]) -> dict[str, Any]:
    prepared: dict[str, Any] = {}
    for column in TABLE_COLUMNS[table]:
        prepared[column] = _encode_value(row.get(column))
    return prepared


def insert_local_bundle(conn: sqlite3.Connection, bundle: dict[str, list[dict[str, Any]]]) -> None:
    table_order = (
        "pipeline_runs",
        "sessions",
        "images",
        "flags",
        "image_assets",
        "observations",
        "graph_nodes",
        "graph_edges",
    )
    for table in table_order:
        rows = bundle.get(table) or []
        if not rows:
            continue
        columns = TABLE_COLUMNS[table]
        column_sql = ", ".join(columns)
        placeholder_sql = ", ".join(f":{column}" for column in columns)
        sql = f"insert into derived.{table} ({column_sql}) values ({placeholder_sql})"
        conn.executemany(sql, [_prepare_row(table, row) for row in rows])
    conn.commit()
