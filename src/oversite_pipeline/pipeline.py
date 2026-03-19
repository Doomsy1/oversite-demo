from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from oversite_pipeline.assets import (
    build_image_asset_record,
    fetch_image_asset,
    update_image_from_download,
)
from oversite_pipeline.clients.supabase_rest import SupabaseRestClient
from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.extract.source import fetch_image_events, fetch_sessions_for_events
from oversite_pipeline.load.prepare import dedupe_graph_rows, materialize_graph_edges
from oversite_pipeline.observations import derive_observations
from oversite_pipeline.transform.graph import build_graph_bundle
from oversite_pipeline.transform.normalize import (
    build_flag_records,
    normalize_image_record,
    normalize_session_record,
)


EDGE_NODE_TYPES = {
    "belongs_to_company": ("session", "company"),
    "captured_by_device": ("session", "device"),
    "occurred_in_session": ("image", "session"),
    "has_flag": ("image", "flag"),
    "associated_with_trade": ("image", "trade"),
    "associated_with_location_hint": ("image", "location_hint"),
    "associated_with_site": ("image", "site"),
    "has_hazard": ("image", "hazard"),
    "hazard_near_location_hint": ("hazard", "location_hint"),
    "contains_equipment": ("image", "equipment"),
    "equipment_at_location_hint": ("equipment", "location_hint"),
    "contains_material": ("image", "material"),
    "shows_worker_activity": ("image", "worker_activity"),
    "references_documentation_artifact": ("image", "documentation_artifact"),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_code_version() -> str | None:
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    version = result.stdout.strip()
    return version or None


def _skipped_download() -> dict[str, Any]:
    return {
        "fetch_status": "skipped",
        "fetch_http_status": None,
        "fetched_at": _utc_now_iso(),
        "raw_headers": None,
        "raw_error": None,
        "image_sha256": None,
        "image_mime_type": None,
        "image_width": None,
        "image_height": None,
        "image_bytes": None,
    }


def _build_manifest_rows(
    *,
    sessions: dict[str, dict[str, Any]],
    images: list[dict[str, Any]],
    image_assets: list[dict[str, Any]],
    flags: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    assets_by_image = {row["derived_image_id"]: row for row in image_assets}
    flags_by_image: dict[str, list[dict[str, Any]]] = {}
    for row in flags:
        flags_by_image.setdefault(row["derived_image_id"], []).append(row)
    observations_by_image: dict[str, list[dict[str, Any]]] = {}
    for row in observations:
        observations_by_image.setdefault(row["derived_image_id"], []).append(row)

    manifest_rows: list[dict[str, Any]] = []
    for image in images:
        asset = assets_by_image.get(image["id"], {})
        session = sessions.get(image["source_session_id"], {})
        manifest_rows.append(
            {
                "image_id": image["id"],
                "source_event_id": image["source_event_id"],
                "source_session_id": image["source_session_id"],
                "blob_url": image["blob_url"],
                "public_image_url": image["public_image_url"],
                "storage_bucket": image["storage_bucket"],
                "storage_path": image["storage_path"],
                "fetch_status": asset.get("fetch_status", image.get("image_fetch_status")),
                "sha256": asset.get("sha256", image.get("image_sha256")),
                "mime_type": asset.get("mime_type", image.get("image_mime_type")),
                "width": asset.get("width", image.get("image_width")),
                "height": asset.get("height", image.get("image_height")),
                "byte_size": asset.get("byte_size", image.get("image_bytes")),
                "event_timestamp": image["event_timestamp"],
                "event_type": image.get("event_type"),
                "event_summary": image.get("event_summary"),
                "priority": image.get("priority"),
                "trade": image.get("trade"),
                "location_hint": image.get("location_hint"),
                "site_hint": image.get("site_hint"),
                "company_id": image.get("company_id"),
                "device_id": image.get("device_id"),
                "user_id": image.get("user_id"),
                "session_started_at": session.get("started_at"),
                "session_ended_at": session.get("ended_at"),
                "session_status": session.get("status"),
                "flags": flags_by_image.get(image["id"], []),
                "observations": observations_by_image.get(image["id"], []),
            }
        )
    return manifest_rows


def _write_manifest_rows(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def run_pipeline(
    *,
    config: PipelineConfig,
    limit: int | None = None,
    dry_run: bool = False,
    window_start: str | None = None,
    window_end: str | None = None,
    skip_fetch: bool = False,
    export_manifest: bool = False,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    client = SupabaseRestClient(
        base_url=config.supabase_url,
        secret_key=config.supabase_secret_key,
    )

    events = fetch_image_events(
        client,
        schema=config.source_schema,
        limit=limit,
        window_start=window_start,
        window_end=window_end,
        page_size=config.pipeline_page_size,
    )
    sessions_by_source_id = fetch_sessions_for_events(
        client,
        schema=config.source_schema,
        events=events,
        page_size=config.pipeline_page_size,
    )

    if dry_run:
        return {
            "dry_run": True,
            "image_event_count": len(events),
            "session_count": len(sessions_by_source_id),
            "window_start": window_start,
            "window_end": window_end,
            "session_ids_missing": sorted(
                {
                    event["session_id"]
                    for event in events
                    if event["session_id"] not in sessions_by_source_id
                }
            ),
        }

    code_version = _get_code_version()
    run_row = client.insert(
        schema=config.derived_schema,
        table="pipeline_runs",
        rows=[
            {
                "status": "running",
                "code_version": code_version,
                "notes": "oversite ml image pipeline run",
                "source_window_start": window_start,
                "source_window_end": window_end,
            }
        ],
    )[0]
    pipeline_run_id = run_row["id"]

    try:
        session_rows = [
            normalize_session_record(session=session, pipeline_run_id=pipeline_run_id)
            for session in sessions_by_source_id.values()
        ]
        inserted_sessions = client.upsert(
            schema=config.derived_schema,
            table="sessions",
            rows=session_rows,
            on_conflict="source_session_id",
        )
        session_map = {row["source_session_id"]: row for row in inserted_sessions}

        image_rows: list[dict[str, Any]] = []
        for event in events:
            session_row = session_map.get(event["session_id"])
            if session_row is None:
                continue
            image_rows.append(
                normalize_image_record(
                    event=event,
                    session=sessions_by_source_id[event["session_id"]],
                    pipeline_run_id=pipeline_run_id,
                    derived_session_id=session_row["id"],
                )
            )

        inserted_images = client.upsert(
            schema=config.derived_schema,
            table="images",
            rows=image_rows,
            on_conflict="source_event_id",
        )
        image_map = {row["source_event_id"]: row for row in inserted_images}

        asset_rows: list[dict[str, Any]] = []
        refreshed_images: list[dict[str, Any]] = []
        for event in events:
            image_row = image_map.get(event["id"])
            if image_row is None:
                continue
            download = (
                _skipped_download()
                if skip_fetch
                else fetch_image_asset(
                    blob_url=image_row["blob_url"],
                    timeout_seconds=config.fetch_timeout_seconds,
                )
            )
            refreshed_images.append(update_image_from_download(image_row, download))
            asset_rows.append(
                build_image_asset_record(
                    pipeline_run_id=pipeline_run_id,
                    image_record=image_row,
                    download=download,
                )
            )

        inserted_images = client.upsert(
            schema=config.derived_schema,
            table="images",
            rows=refreshed_images,
            on_conflict="source_event_id",
        )
        image_map = {row["source_event_id"]: row for row in inserted_images}

        inserted_assets = client.upsert(
            schema=config.derived_schema,
            table="image_assets",
            rows=asset_rows,
            on_conflict="derived_image_id",
        )

        flag_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []
        raw_nodes: list[dict[str, Any]] = []
        raw_edges: list[dict[str, Any]] = []
        for event in events:
            image_row = image_map.get(event["id"])
            session_row = session_map.get(event["session_id"])
            if image_row is None or session_row is None:
                continue
            image_with_id = {**image_row, "pipeline_run_id": pipeline_run_id}
            flags = build_flag_records(image_with_id)
            observations = derive_observations(image_with_id)
            flag_rows.extend(flags)
            observation_rows.extend(observations)
            bundle = build_graph_bundle(session_row, image_with_id, flags, observations)
            raw_nodes.extend(bundle["nodes"])
            raw_edges.extend(bundle["edges"])

        inserted_flags = client.upsert(
            schema=config.derived_schema,
            table="flags",
            rows=flag_rows,
            on_conflict="derived_image_id,flag_type,flag_value",
        )
        inserted_observations = client.upsert(
            schema=config.derived_schema,
            table="observations",
            rows=observation_rows,
            on_conflict="derived_image_id,observation_family,canonical_key",
        )

        deduped_graph = dedupe_graph_rows(nodes=raw_nodes, edges=raw_edges)
        inserted_nodes = client.upsert(
            schema=config.derived_schema,
            table="graph_nodes",
            rows=deduped_graph["nodes"],
            on_conflict="node_type,node_key",
        )
        node_map = {(row["node_type"], row["node_key"]): row for row in inserted_nodes}
        graph_edges = materialize_graph_edges(
            edges=deduped_graph["edges"],
            node_map=node_map,
            edge_node_types=EDGE_NODE_TYPES,
        )
        inserted_edges = client.upsert(
            schema=config.derived_schema,
            table="graph_edges",
            rows=graph_edges,
            on_conflict="src_node_id,edge_type,dst_node_id",
        )

        manifest_rows = _build_manifest_rows(
            sessions=sessions_by_source_id,
            images=inserted_images,
            image_assets=inserted_assets,
            flags=inserted_flags,
            observations=inserted_observations,
        )
        if export_manifest:
            _write_manifest_rows(
                manifest_rows,
                manifest_path or Path(config.export_path),
            )

        client.upsert(
            schema=config.derived_schema,
            table="pipeline_runs",
            rows=[
                {
                    "id": pipeline_run_id,
                    "status": "completed",
                    "code_version": code_version,
                    "completed_at": _utc_now_iso(),
                }
            ],
            on_conflict="id",
        )

        return {
            "dry_run": False,
            "pipeline_run_id": pipeline_run_id,
            "image_event_count": len(events),
            "session_count": len(inserted_sessions),
            "image_count": len(inserted_images),
            "asset_count": len(inserted_assets),
            "flag_count": len(inserted_flags),
            "observation_count": len(inserted_observations),
            "graph_node_count": len(inserted_nodes),
            "graph_edge_count": len(inserted_edges),
            "manifest_row_count": len(manifest_rows),
        }
    except Exception:
        client.upsert(
            schema=config.derived_schema,
            table="pipeline_runs",
            rows=[
                {
                    "id": pipeline_run_id,
                    "status": "failed",
                    "code_version": code_version,
                    "completed_at": _utc_now_iso(),
                }
            ],
            on_conflict="id",
        )
        raise
