from __future__ import annotations

from typing import Any

from oversite_pipeline.clients.supabase_rest import SupabaseRestClient


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


def fetch_image_events(
    client: SupabaseRestClient,
    *,
    schema: str,
    limit: int | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[tuple[str, str]] = [("blob_url", "not.is.null")]
    if window_start:
        filters.append(("timestamp", f"gte.{window_start}"))
    if window_end:
        filters.append(("timestamp", f"lte.{window_end}"))
    return client.select(
        schema=schema,
        table="events",
        columns=IMAGE_EVENT_COLUMNS,
        filters=filters,
        order="timestamp.asc",
        limit=limit,
    )


def fetch_sessions_for_events(
    client: SupabaseRestClient,
    *,
    schema: str,
    events: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    session_ids = collect_session_ids(events)
    if not session_ids:
        return {}
    session_filter = f"in.({','.join(session_ids)})"
    rows = client.select(
        schema=schema,
        table="sessions",
        columns=SESSION_COLUMNS,
        filters={"session_id": session_filter},
    )
    return {row["session_id"]: row for row in rows}
