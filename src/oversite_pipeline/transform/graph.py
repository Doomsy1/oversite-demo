from __future__ import annotations

from typing import Any


def _node(node_type: str, node_key: str, display_label: str, pipeline_run_id: str) -> dict[str, Any]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "node_type": node_type,
        "node_key": node_key,
        "display_label": display_label,
        "source_kind": "derived_rule",
        "attributes": None,
    }


def _edge(
    edge_type: str,
    src_node_key: str,
    dst_node_key: str,
    pipeline_run_id: str,
    evidence_image_id: str | None,
    evidence_session_id: str | None,
    event_timestamp: str | None,
) -> dict[str, Any]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "src_node_key": src_node_key,
        "edge_type": edge_type,
        "dst_node_key": dst_node_key,
        "evidence_image_id": evidence_image_id,
        "evidence_session_id": evidence_session_id,
        "source_kind": "derived_rule",
        "attributes": {"event_timestamp": event_timestamp} if event_timestamp else None,
    }


def build_graph_bundle(
    session_record: dict[str, Any],
    image_record: dict[str, Any],
    flag_records: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    pipeline_run_id = image_record["pipeline_run_id"]
    event_timestamp = image_record.get("event_timestamp")
    session_key = f"session:{session_record['source_session_id']}"
    image_key = f"image:{image_record['source_event_id']}"
    nodes = [
        _node("session", session_key, session_record["source_session_id"], pipeline_run_id),
        _node("image", image_key, image_record["source_event_id"], pipeline_run_id),
    ]
    edges = [
        _edge(
            "occurred_in_session",
            image_key,
            session_key,
            pipeline_run_id,
            image_record["id"],
            session_record["id"],
            event_timestamp,
        ),
    ]

    company_id = session_record.get("company_id")
    if company_id:
        company_key = f"company:{company_id}"
        nodes.append(_node("company", company_key, company_id, pipeline_run_id))
        edges.append(
            _edge(
                "belongs_to_company",
                session_key,
                company_key,
                pipeline_run_id,
                None,
                session_record["id"],
                event_timestamp,
            )
        )

    device_id = session_record.get("device_id")
    if device_id:
        device_key = f"device:{device_id}"
        nodes.append(_node("device", device_key, device_id, pipeline_run_id))
        edges.append(
            _edge(
                "captured_by_device",
                session_key,
                device_key,
                pipeline_run_id,
                None,
                session_record["id"],
                event_timestamp,
            )
        )

    for flag in flag_records:
        flag_key = f"flag:{flag['flag_type']}:{flag['flag_value']}"
        nodes.append(_node("flag", flag_key, flag["flag_value"], pipeline_run_id))
        edges.append(
            _edge(
                "has_flag",
                image_key,
                flag_key,
                pipeline_run_id,
                image_record["id"],
                session_record["id"],
                event_timestamp,
            )
        )

    observation_edges = {
        "trade": "associated_with_trade",
        "location_hint": "associated_with_location_hint",
        "site": "associated_with_site",
        "hazard": "has_hazard",
        "equipment": "contains_equipment",
        "material": "contains_material",
        "worker_activity": "shows_worker_activity",
        "documentation_artifact": "references_documentation_artifact",
    }
    observation_keys: dict[str, list[str]] = {}
    for observation in observations:
        family = observation["observation_family"]
        node_key = f"{family}:{observation['canonical_key']}"
        observation_keys.setdefault(family, []).append(node_key)
        nodes.append(
            _node(
                family,
                node_key,
                observation["observation_label"],
                pipeline_run_id,
            )
        )
        edge_type = observation_edges.get(family)
        if edge_type is None:
            continue
        edges.append(
            _edge(
                edge_type,
                image_key,
                node_key,
                pipeline_run_id,
                image_record["id"],
                session_record["id"],
                event_timestamp,
            )
        )

    for location_key in observation_keys.get("location_hint", []):
        for hazard_key in observation_keys.get("hazard", []):
            edges.append(
                _edge(
                    "hazard_near_location_hint",
                    hazard_key,
                    location_key,
                    pipeline_run_id,
                    image_record["id"],
                    session_record["id"],
                    event_timestamp,
                )
            )
        for equipment_key in observation_keys.get("equipment", []):
            edges.append(
                _edge(
                    "equipment_at_location_hint",
                    equipment_key,
                    location_key,
                    pipeline_run_id,
                    image_record["id"],
                    session_record["id"],
                    event_timestamp,
                )
            )

    return {"nodes": nodes, "edges": edges}
