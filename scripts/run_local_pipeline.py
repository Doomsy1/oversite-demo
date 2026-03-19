from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.assets import (
    build_image_asset_record,
    fetch_image_asset,
    update_image_from_download,
)
from oversite_pipeline.clients.supabase_rest import SupabaseRestClient
from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.extract.source import fetch_image_events, fetch_sessions_for_events
from oversite_pipeline.load.prepare import dedupe_graph_rows, materialize_graph_edges
from oversite_pipeline.local_sqlite import (
    initialize_local_sqlite,
    insert_local_bundle,
    open_local_connection,
)
from oversite_pipeline.observations import derive_observations
from oversite_pipeline.pipeline import EDGE_NODE_TYPES
from oversite_pipeline.transform.graph import build_graph_bundle
from oversite_pipeline.transform.normalize import (
    build_flag_records,
    normalize_image_record,
    normalize_session_record,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid4())


def _assign_id(row: dict[str, object]) -> dict[str, object]:
    return {**row, "id": _new_id()}


def build_local_bundle(*, config: PipelineConfig, limit: int | None) -> dict[str, list[dict[str, object]]]:
    client = SupabaseRestClient(
        base_url=config.supabase_url,
        secret_key=config.supabase_secret_key,
    )
    events = fetch_image_events(client, schema=config.source_schema, limit=limit)
    sessions_by_source_id = fetch_sessions_for_events(
        client,
        schema=config.source_schema,
        events=events,
    )

    pipeline_run_id = _new_id()
    started_at = _utc_now_iso()

    pipeline_runs = [
        {
            "id": pipeline_run_id,
            "started_at": started_at,
            "completed_at": started_at,
            "status": "completed",
            "notes": "local sqlite inspection run",
        }
    ]

    session_rows: list[dict[str, object]] = []
    session_map: dict[str, dict[str, object]] = {}
    for session in sessions_by_source_id.values():
        row = _assign_id(normalize_session_record(session=session, pipeline_run_id=pipeline_run_id))
        session_rows.append(row)
        session_map[row["source_session_id"]] = row

    image_rows: list[dict[str, object]] = []
    event_to_image: dict[str, dict[str, object]] = {}
    for event in events:
        session_row = session_map.get(event["session_id"])
        session = sessions_by_source_id.get(event["session_id"])
        if session_row is None or session is None:
            continue
        row = normalize_image_record(
            event=event,
            session=session,
            pipeline_run_id=pipeline_run_id,
            derived_session_id=session_row["id"],
        )
        download = fetch_image_asset(
            blob_url=row["blob_url"],
            timeout_seconds=config.fetch_timeout_seconds,
        )
        row = _assign_id(update_image_from_download(row, download))
        image_rows.append(row)
        event_to_image[row["source_event_id"]] = row
    image_assets = [
        _assign_id(
            build_image_asset_record(
                pipeline_run_id=pipeline_run_id,
                image_record=image_row,
                download={
                    "fetch_status": image_row["image_fetch_status"],
                    "fetch_http_status": 200 if image_row["image_fetch_status"] == "fetched" else None,
                    "fetched_at": started_at,
                    "raw_headers": None,
                    "raw_error": None,
                    "image_sha256": image_row["image_sha256"],
                    "image_mime_type": image_row["image_mime_type"],
                    "image_width": image_row["image_width"],
                    "image_height": image_row["image_height"],
                    "image_bytes": image_row["image_bytes"],
                },
            )
        )
        for image_row in image_rows
    ]

    flag_rows: list[dict[str, object]] = []
    observation_rows: list[dict[str, object]] = []
    raw_nodes: list[dict[str, object]] = []
    raw_edges: list[dict[str, object]] = []
    for event in events:
        image_row = event_to_image.get(event["id"])
        session_row = session_map.get(event["session_id"])
        if image_row is None or session_row is None:
            continue
        event_flags = build_flag_records(image_row)
        for flag in event_flags:
            flag_rows.append(_assign_id(flag))
        observations = derive_observations(image_row)
        for observation in observations:
            observation_rows.append(_assign_id(observation))
        bundle = build_graph_bundle(session_row, image_row, event_flags, observations)
        raw_nodes.extend(bundle["nodes"])
        raw_edges.extend(bundle["edges"])

    deduped_graph = dedupe_graph_rows(nodes=raw_nodes, edges=raw_edges)
    graph_nodes: list[dict[str, object]] = []
    node_map: dict[tuple[str, str], dict[str, object]] = {}
    for node in deduped_graph["nodes"]:
        prepared = {**node, "id": _new_id()}
        graph_nodes.append(prepared)
        node_map[(prepared["node_type"], prepared["node_key"])] = prepared

    graph_edges = materialize_graph_edges(
        edges=deduped_graph["edges"],
        node_map=node_map,
        edge_node_types=EDGE_NODE_TYPES,
    )
    for edge in graph_edges:
        edge["id"] = _new_id()

    return {
        "pipeline_runs": pipeline_runs,
        "sessions": session_rows,
        "images": image_rows,
        "flags": flag_rows,
        "image_assets": image_assets,
        "observations": observation_rows,
        "graph_nodes": graph_nodes,
        "graph_edges": graph_edges,
    }


def inspect_local_db(db_path: Path) -> dict[str, object]:
    conn = open_local_connection(db_path)
    try:
        counts = {
            "pipeline_runs": conn.execute("select count(*) from derived.pipeline_runs").fetchone()[0],
            "sessions": conn.execute("select count(*) from derived.sessions").fetchone()[0],
            "images": conn.execute("select count(*) from derived.images").fetchone()[0],
            "flags": conn.execute("select count(*) from derived.flags").fetchone()[0],
            "image_assets": conn.execute("select count(*) from derived.image_assets").fetchone()[0],
            "observations": conn.execute("select count(*) from derived.observations").fetchone()[0],
            "graph_nodes": conn.execute("select count(*) from derived.graph_nodes").fetchone()[0],
            "graph_edges": conn.execute("select count(*) from derived.graph_edges").fetchone()[0],
        }
        sample_images = [
            dict(row)
            for row in conn.execute(
                "select source_event_id, priority, trade, location_hint, image_fetch_status, event_summary, flag_summary "
                "from derived.v_image_enriched order by event_timestamp asc limit 5"
            ).fetchall()
        ]
        edge_counts = [
            dict(row)
            for row in conn.execute(
                "select edge_type, count(*) as edge_count "
                "from derived.graph_edges group by edge_type order by edge_type"
            ).fetchall()
        ]
        observation_counts = [
            dict(row)
            for row in conn.execute(
                "select observation_family, count(*) as observation_count "
                "from derived.observations group by observation_family order by observation_family"
            ).fetchall()
        ]
        return {
            "db_path": str(db_path),
            "counts": counts,
            "sample_images": sample_images,
            "edge_counts": edge_counts,
            "observation_counts": observation_counts,
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Oversite pipeline into a local SQLite derived database."
    )
    parser.add_argument("--env-file", default=".env", help="Path to env file")
    parser.add_argument("--limit", type=int, default=20, help="Optional event limit")
    parser.add_argument(
        "--db-path",
        default="local/derived_inspection.db",
        help="Path for the local SQLite file",
    )
    args = parser.parse_args()

    config = PipelineConfig.from_env_file(args.env_file)
    db_path = Path(args.db_path)
    if db_path.exists():
        db_path.unlink()

    bundle = build_local_bundle(config=config, limit=args.limit)
    conn = open_local_connection(db_path)
    try:
        initialize_local_sqlite(conn)
        insert_local_bundle(conn, bundle)
    finally:
        conn.close()

    print(json.dumps(inspect_local_db(db_path), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
