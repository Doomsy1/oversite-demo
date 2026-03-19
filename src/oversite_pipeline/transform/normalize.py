from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_blob_url(blob_url: str) -> dict[str, str]:
    parsed = urlparse(blob_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 6:
        raise ValueError(f"Unexpected blob URL path: {parsed.path}")
    if path_parts[:4] != ["storage", "v1", "object", "public"]:
        raise ValueError(f"Unexpected blob URL prefix: {parsed.path}")
    storage_bucket = path_parts[4]
    storage_path = "/".join(path_parts[5:])
    return {
        "storage_bucket": storage_bucket,
        "storage_path": storage_path,
    }


def normalize_image_record(
    *,
    event: dict[str, Any],
    session: dict[str, Any],
    pipeline_run_id: str,
    derived_session_id: str,
) -> dict[str, Any]:
    metadata = event.get("metadata") or {}
    blob_parts = parse_blob_url(event["blob_url"])
    return {
        "pipeline_run_id": pipeline_run_id,
        "derived_session_id": derived_session_id,
        "source_event_id": event["id"],
        "source_session_id": event["session_id"],
        "company_id": event["company_id"],
        "device_id": event.get("device_id") or session.get("device_id"),
        "user_id": event.get("user_id"),
        "event_timestamp": event["timestamp"],
        "event_type": event.get("type"),
        "event_summary": _clean_text(event.get("text_content")),
        "priority": _clean_text(metadata.get("priority")),
        "blob_url": event["blob_url"],
        "public_image_url": event["blob_url"],
        "storage_bucket": blob_parts["storage_bucket"],
        "storage_path": blob_parts["storage_path"],
        "trade": _clean_text(metadata.get("trade")),
        "location_hint": _clean_text(metadata.get("location")),
        "site_hint": _clean_text(metadata.get("site")),
        "is_flagged": event.get("type") == "issue_flagged",
        "image_fetch_status": "pending",
        "image_sha256": None,
        "image_mime_type": None,
        "image_width": None,
        "image_height": None,
        "image_bytes": None,
        "raw_metadata": metadata or None,
        "raw_event": event,
    }


def normalize_session_record(*, session: dict[str, Any], pipeline_run_id: str) -> dict[str, Any]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "source_session_row_id": session["id"],
        "source_session_id": session["session_id"],
        "company_id": session["company_id"],
        "device_id": session.get("device_id"),
        "vertical_id": session.get("vertical_id"),
        "started_at": session.get("started_at"),
        "ended_at": session.get("ended_at"),
        "status": session.get("status"),
        "source_event_count": session.get("event_count"),
        "source_flag_count": session.get("flag_count"),
        "created_at": session.get("created_at"),
        "raw_session": session,
    }


def build_flag_records(image_record: dict[str, Any]) -> list[dict[str, Any]]:
    if image_record.get("event_type") != "issue_flagged":
        return []
    return [
        {
            "pipeline_run_id": image_record["pipeline_run_id"],
            "derived_image_id": image_record["id"],
            "flag_type": "event_type",
            "flag_value": "issue_flagged",
            "source_kind": "source_event_type",
            "raw_fragment": {"event_type": "issue_flagged"},
        }
    ]
