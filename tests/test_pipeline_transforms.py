import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.transform.graph import build_graph_bundle
from oversite_pipeline.transform.normalize import (
    build_flag_records,
    normalize_image_record,
    normalize_session_record,
    parse_blob_url,
)


class ParseBlobUrlTests(unittest.TestCase):
    def test_parse_blob_url_extracts_bucket_and_storage_path(self) -> None:
        blob = (
            "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
            "DEVICE123/SESSION456/2026-03-18T22-02-05-055Z.jpg"
        )

        parsed = parse_blob_url(blob)

        self.assertEqual(parsed["storage_bucket"], "engagement-blobs")
        self.assertEqual(
            parsed["storage_path"],
            "DEVICE123/SESSION456/2026-03-18T22-02-05-055Z.jpg",
        )


class NormalizeImageRecordTests(unittest.TestCase):
    def test_normalize_image_record_maps_source_fields(self) -> None:
        event = {
            "id": "event-1",
            "session_id": "session-1",
            "company_id": "company-1",
            "device_id": "device-1",
            "user_id": "user-1",
            "timestamp": "2026-03-18T22:02:05.602+00:00",
            "type": "issue_flagged",
            "text_content": "Power cables creating a tripping hazard.",
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE123/SESSION456/2026-03-18T22-02-05-055Z.jpg"
            ),
            "metadata": {
                "priority": "High",
                "trade": "Safety",
                "location": "Near the desk and wall outlet",
            },
        }
        session = {"session_id": "session-1"}

        record = normalize_image_record(
            event=event,
            session=session,
            pipeline_run_id="run-1",
            derived_session_id="derived-session-1",
        )

        self.assertEqual(record["pipeline_run_id"], "run-1")
        self.assertEqual(record["derived_session_id"], "derived-session-1")
        self.assertEqual(record["source_event_id"], "event-1")
        self.assertEqual(record["event_type"], "issue_flagged")
        self.assertEqual(record["event_summary"], "Power cables creating a tripping hazard.")
        self.assertEqual(record["priority"], "High")
        self.assertEqual(record["trade"], "Safety")
        self.assertEqual(record["location_hint"], "Near the desk and wall outlet")
        self.assertEqual(record["public_image_url"], event["blob_url"])
        self.assertEqual(record["image_fetch_status"], "pending")
        self.assertIsNone(record["site_hint"])
        self.assertTrue(record["is_flagged"])
        self.assertEqual(record["storage_bucket"], "engagement-blobs")

    def test_normalize_image_record_treats_blank_metadata_values_as_null(self) -> None:
        event = {
            "id": "event-2",
            "session_id": "session-2",
            "company_id": "company-1",
            "device_id": "device-1",
            "user_id": "user-1",
            "timestamp": "2026-03-18T22:02:05.602+00:00",
            "type": "issue_flagged",
            "text_content": "Loose duct",
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE123/SESSION456/2026-03-18T22-02-05-055Z.jpg"
            ),
            "metadata": {"priority": "  ", "trade": "", "location": "   "},
        }
        session = {"session_id": "session-2"}

        record = normalize_image_record(
            event=event,
            session=session,
            pipeline_run_id="run-1",
            derived_session_id="derived-session-2",
        )

        self.assertIsNone(record["priority"])
        self.assertIsNone(record["trade"])
        self.assertIsNone(record["location_hint"])


class NormalizeSessionRecordTests(unittest.TestCase):
    def test_normalize_session_record_maps_source_session_fields(self) -> None:
        session = {
            "id": "row-1",
            "session_id": "session-1",
            "company_id": "company-1",
            "device_id": "device-1",
            "vertical_id": "construction",
            "started_at": "2026-03-18T22:01:07.928+00:00",
            "ended_at": "2026-03-18T22:02:48.746+00:00",
            "status": "ended",
            "event_count": 31,
            "flag_count": 1,
            "created_at": "2026-03-18T22:01:11.481606+00:00",
        }

        record = normalize_session_record(session=session, pipeline_run_id="run-1")

        self.assertEqual(record["pipeline_run_id"], "run-1")
        self.assertEqual(record["source_session_row_id"], "row-1")
        self.assertEqual(record["source_session_id"], "session-1")
        self.assertEqual(record["source_event_count"], 31)
        self.assertEqual(record["source_flag_count"], 1)
        self.assertEqual(record["raw_session"], session)


class BuildFlagRecordsTests(unittest.TestCase):
    def test_build_flag_records_only_emits_explicit_flag_event(self) -> None:
        image_record = {
            "pipeline_run_id": "run-1",
            "id": "image-1",
            "event_type": "issue_flagged",
            "raw_metadata": {"priority": "High"},
        }

        flags = build_flag_records(image_record)

        self.assertEqual(
            flags,
            [
                {
                    "pipeline_run_id": "run-1",
                    "derived_image_id": "image-1",
                    "flag_type": "event_type",
                    "flag_value": "issue_flagged",
                    "source_kind": "source_event_type",
                    "raw_fragment": {"event_type": "issue_flagged"},
                }
            ],
        )


class BuildGraphBundleTests(unittest.TestCase):
    def test_build_graph_bundle_creates_nodes_and_edges_for_issue_context(self) -> None:
        session_record = {
            "id": "derived-session-1",
            "pipeline_run_id": "run-1",
            "source_session_id": "session-1",
            "company_id": "company-1",
            "device_id": "device-1",
        }
        image_record = {
            "id": "image-1",
            "pipeline_run_id": "run-1",
            "source_event_id": "event-1",
            "event_timestamp": "2026-03-18T22:02:05.602+00:00",
        }
        observations = [
            {"observation_family": "trade", "canonical_key": "safety", "observation_label": "Safety"},
            {
                "observation_family": "location_hint",
                "canonical_key": "near the desk and wall outlet",
                "observation_label": "Near the desk and wall outlet",
            },
            {
                "observation_family": "hazard",
                "canonical_key": "exposed wiring",
                "observation_label": "Exposed wiring",
            },
            {"observation_family": "equipment", "canonical_key": "ladder", "observation_label": "Ladder"},
            {"observation_family": "material", "canonical_key": "drywall", "observation_label": "Drywall"},
            {
                "observation_family": "worker_activity",
                "canonical_key": "worker on ladder",
                "observation_label": "Worker on ladder",
            },
            {
                "observation_family": "documentation_artifact",
                "canonical_key": "daily site report",
                "observation_label": "Daily Site Report",
            },
        ]
        flag_records = [
            {
                "flag_type": "event_type",
                "flag_value": "issue_flagged",
            }
        ]

        bundle = build_graph_bundle(session_record, image_record, flag_records, observations)

        node_types = {node["node_type"] for node in bundle["nodes"]}
        edge_types = {edge["edge_type"] for edge in bundle["edges"]}

        self.assertEqual(
            node_types,
            {
                "company",
                "session",
                "image",
                "device",
                "flag",
                "trade",
                "location_hint",
                "hazard",
                "equipment",
                "material",
                "worker_activity",
                "documentation_artifact",
            },
        )
        self.assertEqual(
            edge_types,
            {
                "belongs_to_company",
                "captured_by_device",
                "occurred_in_session",
                "has_flag",
                "associated_with_trade",
                "associated_with_location_hint",
                "has_hazard",
                "contains_equipment",
                "hazard_near_location_hint",
                "equipment_at_location_hint",
                "contains_material",
                "shows_worker_activity",
                "references_documentation_artifact",
            },
        )

    def test_build_graph_bundle_skips_company_when_source_company_is_missing(self) -> None:
        session_record = {
            "id": "derived-session-1",
            "pipeline_run_id": "run-1",
            "source_session_id": "session-1",
            "company_id": None,
            "device_id": "device-1",
        }
        image_record = {
            "id": "image-1",
            "pipeline_run_id": "run-1",
            "source_event_id": "event-1",
            "event_timestamp": "2026-03-18T22:02:05.602+00:00",
        }

        bundle = build_graph_bundle(session_record, image_record, [], [])

        node_types = {node["node_type"] for node in bundle["nodes"]}
        edge_types = {edge["edge_type"] for edge in bundle["edges"]}

        self.assertEqual(node_types, {"session", "image", "device"})
        self.assertEqual(edge_types, {"captured_by_device", "occurred_in_session"})


if __name__ == "__main__":
    unittest.main()
