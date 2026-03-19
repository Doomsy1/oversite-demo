from __future__ import annotations

from typing import Any


def _dedupe_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        marker = tuple(row[key] for key in keys)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(row)
    return deduped


def _aggregate_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for edge in edges:
        marker = tuple(edge[key] for key in ("src_node_key", "edge_type", "dst_node_key"))
        if marker not in grouped:
            grouped[marker] = {
                **edge,
                "_image_ids": set(),
                "_session_ids": set(),
                "_timestamps": [],
            }
        group = grouped[marker]
        image_id = edge.get("evidence_image_id")
        if image_id is not None:
            group["_image_ids"].add(image_id)
        session_id = edge.get("evidence_session_id")
        if session_id is not None:
            group["_session_ids"].add(session_id)
        timestamp = (edge.get("attributes") or {}).get("event_timestamp")
        if timestamp:
            group["_timestamps"].append(timestamp)

    aggregated: list[dict[str, Any]] = []
    for group in grouped.values():
        timestamps = sorted(group.pop("_timestamps"))
        image_count = len(group.pop("_image_ids"))
        session_count = len(group.pop("_session_ids"))
        group["attributes"] = {
            "evidence_image_count": image_count,
            "evidence_session_count": session_count,
            "first_seen_at": timestamps[0] if timestamps else None,
            "last_seen_at": timestamps[-1] if timestamps else None,
        }
        aggregated.append(group)
    return aggregated


def dedupe_graph_rows(
    *, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    return {
        "nodes": _dedupe_rows(nodes, ("node_type", "node_key")),
        "edges": _aggregate_edges(edges),
    }


def materialize_graph_edges(
    *,
    edges: list[dict[str, Any]],
    node_map: dict[tuple[str, str], dict[str, Any]],
    edge_node_types: dict[str, tuple[str, str]],
) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    for edge in edges:
        src_type, dst_type = edge_node_types[edge["edge_type"]]
        src_node = node_map[(src_type, edge["src_node_key"])]
        dst_node = node_map[(dst_type, edge["dst_node_key"])]
        materialized.append(
            {
                "last_materialized_run_id": edge["last_materialized_run_id"],
                "src_node_id": src_node["id"],
                "edge_type": edge["edge_type"],
                "dst_node_id": dst_node["id"],
                "evidence_image_id": edge.get("evidence_image_id"),
                "evidence_session_id": edge.get("evidence_session_id"),
                "source_kind": edge["source_kind"],
                "attributes": edge.get("attributes"),
            }
        )
    return materialized
