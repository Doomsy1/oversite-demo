from __future__ import annotations

from typing import Any

from oversite_pipeline.clients.supabase_rest import SupabaseRestClient
from oversite_pipeline.transform.normalize import parse_blob_url


IMAGE_EVENT_COLUMNS = (
    "id,timestamp,session_id,type,vertical_id,text_content,metadata,blob_url,"
    "device_id,created_at,company_id,user_id"
)
SESSION_COLUMNS = (
    "id,session_id,company_id,device_id,vertical_id,started_at,ended_at,status,"
    "event_count,flag_count,created_at"
)


def collect_session_ids(events: list[dict[str, Any]]) -> list[str]:
    session_ids = {event["session_id"] for event in events if event.get("session_id")}
    return sorted(session_ids)


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def fetch_image_events(
    client: SupabaseRestClient,
    *,
    schema: str,
    source_bucket: str,
    limit: int | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    filters: list[tuple[str, str]] = [("blob_url", "not.is.null")]
    if window_start:
        filters.append(("timestamp", f"gte.{window_start}"))
    if window_end:
        filters.append(("timestamp", f"lte.{window_end}"))

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        request_limit = page_size if limit is None else min(page_size, limit - len(rows))
        if request_limit <= 0:
            break
        page = client.select(
            schema=schema,
            table="events",
            columns=IMAGE_EVENT_COLUMNS,
            filters=filters,
            order="timestamp.asc",
            limit=request_limit,
            offset=offset,
        )
        rows.extend(
            row
            for row in page
            if _matches_source_bucket(row.get("blob_url"), source_bucket=source_bucket)
        )
        if limit is not None and len(rows) >= limit:
            return rows[:limit]
        if len(page) < request_limit:
            break
        offset += len(page)
    return rows


def _matches_source_bucket(blob_url: Any, *, source_bucket: str) -> bool:
    if not isinstance(blob_url, str) or not blob_url:
        return False
    try:
        return parse_blob_url(blob_url)["storage_bucket"] == source_bucket
    except ValueError:
        return False


def fetch_sessions_for_events(
    client: SupabaseRestClient,
    *,
    schema: str,
    events: list[dict[str, Any]],
    page_size: int = 200,
) -> dict[str, dict[str, Any]]:
    session_ids = collect_session_ids(events)
    if not session_ids:
        return {}
    rows: list[dict[str, Any]] = []
    for session_batch in _chunked(session_ids, page_size):
        session_filter = f"in.({','.join(session_batch)})"
        rows.extend(
            client.select(
                schema=schema,
                table="sessions",
                columns=SESSION_COLUMNS,
                filters={"session_id": session_filter},
            )
        )
    return {row["session_id"]: row for row in rows}
