from __future__ import annotations

import re
from typing import Any


def canonicalize_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _append_observation(
    rows: list[dict[str, Any]],
    *,
    image_record: dict[str, Any],
    family: str,
    label: str,
    confidence: str,
    source_kind: str,
    rule_id: str,
    evidence_text: str | None,
    evidence_metadata: dict[str, Any] | None,
    raw_fragment: dict[str, Any] | None,
) -> None:
    rows.append(
        {
            "last_materialized_run_id": image_record.get("last_materialized_run_id"),
            "derived_image_id": image_record.get("id"),
            "observation_family": family,
            "observation_label": label,
            "canonical_key": canonicalize_key(label),
            "confidence": confidence,
            "source_kind": source_kind,
            "rule_id": rule_id,
            "evidence_text": evidence_text,
            "evidence_metadata": evidence_metadata,
            "raw_fragment": raw_fragment,
        }
    )


TEXT_RULES = (
    ("hazard", "exposed wiring", r"\bexposed wiring\b", "text.hazard.exposed_wiring"),
    ("hazard", "tripping hazard", r"\btripping hazard\b", "text.hazard.tripping_hazard"),
    ("equipment", "ladder", r"\bladder\b", "text.equipment.ladder"),
    ("material", "drywall", r"\bdrywall\b", "text.material.drywall"),
    (
        "worker_activity",
        "worker on ladder",
        r"\bworker\b.*\bladder\b|\bladder\b.*\bworker\b",
        "text.worker_activity.worker_on_ladder",
    ),
    (
        "documentation_artifact",
        "daily site report",
        r"\bdaily site report\b",
        "text.documentation.daily_site_report",
    ),
)


def derive_observations(image_record: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    metadata = image_record.get("raw_metadata") or {}
    event_summary = image_record.get("event_summary") or ""

    for family, field, rule_id in (
        ("trade", "trade", "metadata.trade"),
        ("location_hint", "location_hint", "metadata.location"),
        ("site", "site_hint", "metadata.site"),
    ):
        value = image_record.get(field)
        if value:
            _append_observation(
                rows,
                image_record=image_record,
                family=family,
                label=str(value),
                confidence="high",
                source_kind="source_metadata",
                rule_id=rule_id,
                evidence_text=None,
                evidence_metadata=metadata or None,
                raw_fragment={field: value},
            )

    for family, label, pattern, rule_id in TEXT_RULES:
        if re.search(pattern, event_summary, flags=re.IGNORECASE):
            _append_observation(
                rows,
                image_record=image_record,
                family=family,
                label=label,
                confidence="medium",
                source_kind="source_text",
                rule_id=rule_id,
                evidence_text=event_summary,
                evidence_metadata=None,
                raw_fragment={"event_summary": event_summary},
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        marker = (row["observation_family"], row["canonical_key"])
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(row)
    return deduped
