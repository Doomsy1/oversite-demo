import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.local_sqlite import (
    initialize_local_sqlite,
    insert_local_bundle,
    open_local_connection,
)


class LocalSqliteTests(unittest.TestCase):
    def test_initialize_and_insert_bundle_supports_enriched_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "derived.db"
            conn = open_local_connection(db_path)
            try:
                initialize_local_sqlite(conn)
                bundle = {
                    "pipeline_runs": [
                        {"id": "run-1", "status": "completed"},
                    ],
                    "sessions": [
                        {
                            "id": "session-row-1",
                            "last_materialized_run_id": "run-1",
                            "source_session_row_id": "source-session-row-1",
                            "source_session_id": "session-1",
                            "company_id": "company-1",
                            "device_id": "device-1",
                            "vertical_id": "construction",
                            "started_at": "2026-03-18T22:01:07.928+00:00",
                            "ended_at": "2026-03-18T22:02:48.746+00:00",
                            "status": "ended",
                            "source_event_count": 31,
                            "source_flag_count": 1,
                            "created_at": "2026-03-18T22:01:11.481606+00:00",
                            "raw_session": "{}",
                        },
                        {
                            "id": "session-row-2",
                            "last_materialized_run_id": "run-1",
                            "source_session_row_id": "source-session-row-2",
                            "source_session_id": "session-2",
                            "company_id": "company-1",
                            "device_id": "device-1",
                            "vertical_id": "construction",
                            "started_at": "2026-03-19T22:01:07.928+00:00",
                            "ended_at": "2026-03-19T22:02:48.746+00:00",
                            "status": "ended",
                            "source_event_count": 27,
                            "source_flag_count": 1,
                            "created_at": "2026-03-19T22:01:11.481606+00:00",
                            "raw_session": "{}",
                        }
                    ],
                    "images": [
                        {
                            "id": "image-1",
                            "last_materialized_run_id": "run-1",
                            "derived_session_id": "session-row-1",
                            "source_event_id": "event-1",
                            "source_session_id": "session-1",
                            "company_id": "company-1",
                            "device_id": "device-1",
                            "user_id": "user-1",
                            "event_timestamp": "2026-03-18T22:02:05.602+00:00",
                            "event_type": "issue_flagged",
                            "event_summary": "Power cables creating a tripping hazard.",
                            "priority": "High",
                            "blob_url": "https://example/image.jpg",
                            "public_image_url": "https://example/image.jpg",
                            "storage_bucket": "engagement-blobs",
                            "storage_path": "DEVICE/SESSION/image.jpg",
                            "trade": "Safety",
                            "location_hint": "Near desk",
                            "site_hint": None,
                            "is_flagged": 1,
                            "image_fetch_status": "fetched",
                            "image_sha256": "abc123",
                            "image_mime_type": "image/png",
                            "image_width": 1,
                            "image_height": 1,
                            "image_bytes": 68,
                            "raw_metadata": "{}",
                            "raw_event": "{}",
                        },
                        {
                            "id": "image-2",
                            "last_materialized_run_id": "run-1",
                            "derived_session_id": "session-row-2",
                            "source_event_id": "event-2",
                            "source_session_id": "session-2",
                            "company_id": "company-1",
                            "device_id": "device-1",
                            "user_id": "user-1",
                            "event_timestamp": "2026-03-19T22:02:05.602+00:00",
                            "event_type": "issue_flagged",
                            "event_summary": "Worker used ladder near desk.",
                            "priority": "Medium",
                            "blob_url": "https://example/image-2.jpg",
                            "public_image_url": "https://example/image-2.jpg",
                            "storage_bucket": "engagement-blobs",
                            "storage_path": "DEVICE/SESSION/image-2.jpg",
                            "trade": "safety",
                            "location_hint": "Near desk!",
                            "site_hint": None,
                            "is_flagged": 1,
                            "image_fetch_status": "fetched",
                            "image_sha256": "def456",
                            "image_mime_type": "image/png",
                            "image_width": 1,
                            "image_height": 1,
                            "image_bytes": 68,
                            "raw_metadata": "{}",
                            "raw_event": "{}",
                        }
                    ],
                    "flags": [
                        {
                            "id": "flag-1",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-1",
                            "flag_type": "event_type",
                            "flag_value": "issue_flagged",
                            "source_kind": "source_event_type",
                            "raw_fragment": "{}",
                        },
                        {
                            "id": "flag-2",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-2",
                            "flag_type": "event_type",
                            "flag_value": "issue_flagged",
                            "source_kind": "source_event_type",
                            "raw_fragment": "{}",
                        }
                    ],
                    "image_assets": [
                        {
                            "id": "asset-1",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-1",
                            "source_bucket": "engagement-blobs",
                            "source_path": "DEVICE/SESSION/image.jpg",
                            "source_url": "https://example/image.jpg",
                            "fetch_status": "fetched",
                            "fetch_http_status": 200,
                            "fetched_at": "2026-03-19T12:00:00+00:00",
                            "byte_size": 68,
                            "sha256": "abc123",
                            "mime_type": "image/png",
                            "width": 1,
                            "height": 1,
                            "curated_bucket": None,
                            "curated_path": None,
                            "raw_headers": "{}",
                            "raw_error": None,
                        },
                        {
                            "id": "asset-2",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-2",
                            "source_bucket": "engagement-blobs",
                            "source_path": "DEVICE/SESSION/image-2.jpg",
                            "source_url": "https://example/image-2.jpg",
                            "fetch_status": "fetched",
                            "fetch_http_status": 200,
                            "fetched_at": "2026-03-20T12:00:00+00:00",
                            "byte_size": 68,
                            "sha256": "def456",
                            "mime_type": "image/png",
                            "width": 1,
                            "height": 1,
                            "curated_bucket": None,
                            "curated_path": None,
                            "raw_headers": "{}",
                            "raw_error": None,
                        }
                    ],
                    "observations": [
                        {
                            "id": "obs-1",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-1",
                            "observation_family": "trade",
                            "observation_label": "Safety",
                            "canonical_key": "safety",
                            "confidence": "high",
                            "source_kind": "source_metadata",
                            "rule_id": "metadata.trade",
                            "evidence_text": None,
                            "evidence_metadata": "{\"trade\":\"Safety\"}",
                            "raw_fragment": "{}",
                        },
                        {
                            "id": "obs-location-1",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-1",
                            "observation_family": "location_hint",
                            "observation_label": "Near desk",
                            "canonical_key": "near desk",
                            "confidence": "high",
                            "source_kind": "source_metadata",
                            "rule_id": "metadata.location",
                            "evidence_text": None,
                            "evidence_metadata": "{\"location\":\"Near desk\"}",
                            "raw_fragment": "{}",
                        },
                        {
                            "id": "obs-2",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-2",
                            "observation_family": "trade",
                            "observation_label": "safety",
                            "canonical_key": "safety",
                            "confidence": "high",
                            "source_kind": "source_metadata",
                            "rule_id": "metadata.trade",
                            "evidence_text": None,
                            "evidence_metadata": "{\"trade\":\"safety\"}",
                            "raw_fragment": "{}",
                        },
                        {
                            "id": "obs-location-2",
                            "last_materialized_run_id": "run-1",
                            "derived_image_id": "image-2",
                            "observation_family": "location_hint",
                            "observation_label": "Near desk!",
                            "canonical_key": "near desk",
                            "confidence": "high",
                            "source_kind": "source_metadata",
                            "rule_id": "metadata.location",
                            "evidence_text": None,
                            "evidence_metadata": "{\"location\":\"Near desk!\"}",
                            "raw_fragment": "{}",
                        }
                    ],
                    "graph_nodes": [
                        {
                            "id": "node-hazard-1",
                            "last_materialized_run_id": "run-1",
                            "node_type": "hazard",
                            "node_key": "hazard:exposed wiring",
                            "display_label": "Exposed wiring",
                            "source_kind": "derived_rule",
                            "attributes": None,
                        },
                        {
                            "id": "node-location-1",
                            "last_materialized_run_id": "run-1",
                            "node_type": "location_hint",
                            "node_key": "location_hint:near desk",
                            "display_label": "Near desk",
                            "source_kind": "derived_rule",
                            "attributes": None,
                        },
                    ],
                    "graph_edges": [
                        {
                            "id": "edge-1",
                            "last_materialized_run_id": "run-1",
                            "src_node_id": "node-hazard-1",
                            "edge_type": "hazard_near_location_hint",
                            "dst_node_id": "node-location-1",
                            "evidence_image_id": "image-1",
                            "evidence_session_id": "session-row-1",
                            "source_kind": "derived_rule",
                            "attributes": {
                                "evidence_image_count": 2,
                                "evidence_session_count": 2,
                                "first_seen_at": "2026-03-18T22:02:05.602+00:00",
                                "last_seen_at": "2026-03-19T22:02:05.602+00:00",
                            },
                        }
                    ],
                }

                insert_local_bundle(conn, bundle)
                row = conn.execute(
                    "select image_id, priority, trade, image_fetch_status, flag_summary "
                    "from derived.v_image_enriched"
                ).fetchone()
                manifest_row = conn.execute(
                    "select image_id, fetch_status, observations_json "
                    "from derived.v_ml_dataset_manifest"
                ).fetchone()
                graph_row = conn.execute(
                    "select edge_type, evidence_session_count, first_seen_at "
                    "from derived.v_graph_edges_enriched"
                ).fetchone()
                recurrence_row = conn.execute(
                    "select entity_label, location_hint, evidence_image_count "
                    "from derived.v_entity_location_recurrence "
                    "where edge_type = 'hazard_near_location_hint'"
                ).fetchone()
                hazard_row = conn.execute(
                    "select hazard, evidence_session_count "
                    "from derived.v_hazard_location_recurrence"
                ).fetchone()
                trade_row = conn.execute(
                    "select trade, location_hint, image_count, session_count, "
                    "normalized_trade_key, normalized_location_hint_key "
                    "from derived.v_trade_location_recurrence"
                ).fetchone()
            finally:
                conn.close()

        self.assertEqual(row["image_id"], "image-1")
        self.assertEqual(row["priority"], "High")
        self.assertEqual(row["trade"], "Safety")
        self.assertEqual(row["image_fetch_status"], "fetched")
        self.assertEqual(row["flag_summary"], "event_type:issue_flagged")
        self.assertEqual(manifest_row["image_id"], "image-1")
        self.assertEqual(manifest_row["fetch_status"], "fetched")
        self.assertIn("Safety", manifest_row["observations_json"])
        self.assertEqual(graph_row["edge_type"], "hazard_near_location_hint")
        self.assertEqual(graph_row["evidence_session_count"], 2)
        self.assertEqual(graph_row["first_seen_at"], "2026-03-18T22:02:05.602+00:00")
        self.assertEqual(recurrence_row["entity_label"], "Exposed wiring")
        self.assertEqual(recurrence_row["location_hint"], "Near desk")
        self.assertEqual(recurrence_row["evidence_image_count"], 2)
        self.assertEqual(hazard_row["hazard"], "Exposed wiring")
        self.assertEqual(hazard_row["evidence_session_count"], 2)
        self.assertEqual(trade_row["trade"], "Safety")
        self.assertEqual(trade_row["location_hint"], "Near desk")
        self.assertEqual(trade_row["image_count"], 2)
        self.assertEqual(trade_row["session_count"], 2)
        self.assertEqual(trade_row["normalized_trade_key"], "safety")
        self.assertEqual(trade_row["normalized_location_hint_key"], "near desk")

    def test_graph_edges_view_defaults_missing_evidence_counts_to_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "derived.db"
            conn = open_local_connection(db_path)
            try:
                initialize_local_sqlite(conn)
                bundle = {
                    "pipeline_runs": [{"id": "run-1", "status": "completed"}],
                    "sessions": [
                        {
                            "id": "session-row-1",
                            "last_materialized_run_id": "run-1",
                            "source_session_row_id": "source-session-row-1",
                            "source_session_id": "session-1",
                            "company_id": "company-1",
                            "device_id": None,
                            "vertical_id": None,
                            "started_at": None,
                            "ended_at": None,
                            "status": None,
                            "source_event_count": None,
                            "source_flag_count": None,
                            "created_at": None,
                            "raw_session": "{}",
                        }
                    ],
                    "images": [],
                    "flags": [],
                    "image_assets": [],
                    "observations": [],
                    "graph_nodes": [
                        {
                            "id": "node-session-1",
                            "last_materialized_run_id": "run-1",
                            "node_type": "session",
                            "node_key": "session:session-1",
                            "display_label": "session-1",
                            "source_kind": "derived_rule",
                            "attributes": None,
                        },
                        {
                            "id": "node-company-1",
                            "last_materialized_run_id": "run-1",
                            "node_type": "company",
                            "node_key": "company:company-1",
                            "display_label": "company-1",
                            "source_kind": "derived_rule",
                            "attributes": None,
                        },
                    ],
                    "graph_edges": [
                        {
                            "id": "edge-1",
                            "last_materialized_run_id": "run-1",
                            "src_node_id": "node-session-1",
                            "edge_type": "belongs_to_company",
                            "dst_node_id": "node-company-1",
                            "evidence_image_id": None,
                            "evidence_session_id": "session-row-1",
                            "source_kind": "derived_rule",
                            "attributes": {},
                        }
                    ],
                }

                insert_local_bundle(conn, bundle)
                row = conn.execute(
                    "select evidence_image_count, evidence_session_count "
                    "from derived.v_graph_edges_enriched"
                ).fetchone()
            finally:
                conn.close()

        self.assertEqual(row["evidence_image_count"], 0)
        self.assertEqual(row["evidence_session_count"], 1)

    def test_initialize_local_sqlite_creates_supporting_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "derived.db"
            conn = open_local_connection(db_path)
            try:
                initialize_local_sqlite(conn)
                indexes = {
                    row["name"]
                    for row in conn.execute(
                        "select name from derived.sqlite_master where type = 'index'"
                    ).fetchall()
                }
            finally:
                conn.close()

        self.assertIn("derived_images_event_timestamp_idx", indexes)
        self.assertIn("derived_image_assets_fetch_status_idx", indexes)
        self.assertIn("derived_observations_family_idx", indexes)
        self.assertIn("derived_flags_flag_value_idx", indexes)
        self.assertIn("derived_graph_edges_edge_type_idx", indexes)


if __name__ == "__main__":
    unittest.main()
