import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.pipeline import run_pipeline


class _FakeClient:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, object]]] = {}
        self._next_id = 1

    def insert(self, *, schema: str, table: str, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        del schema
        inserted = [self._with_id(row) for row in rows]
        self.tables.setdefault(table, []).extend(inserted)
        return inserted

    def upsert(
        self,
        *,
        schema: str,
        table: str,
        rows: list[dict[str, object]],
        on_conflict: str,
    ) -> list[dict[str, object]]:
        del schema
        del on_conflict
        inserted = [self._with_id(row) for row in rows]
        self.tables[table] = inserted
        return inserted

    def _with_id(self, row: dict[str, object]) -> dict[str, object]:
        if row.get("id"):
            return dict(row)
        new_row = dict(row)
        new_row["id"] = f"generated-{self._next_id}"
        self._next_id += 1
        return new_row


class RunPipelineTests(unittest.TestCase):
    def test_run_pipeline_writes_assets_observations_graph_and_manifest(self) -> None:
        fake_client = _FakeClient()
        event = {
            "id": "event-1",
            "timestamp": "2026-03-19T12:00:00+00:00",
            "session_id": "session-1",
            "type": "issue_flagged",
            "vertical_id": "construction",
            "text_content": "Worker on ladder near exposed wiring beside drywall stack.",
            "metadata": {
                "priority": "High",
                "trade": "Electrical",
                "location": "East wall stairwell",
            },
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE123/SESSION456/2026-03-18T22-02-05-055Z.jpg"
            ),
            "device_id": "device-1",
            "created_at": "2026-03-19T12:00:00+00:00",
            "company_id": "company-1",
            "user_id": "user-1",
        }
        session = {
            "id": "session-row-1",
            "session_id": "session-1",
            "company_id": "company-1",
            "device_id": "device-1",
            "vertical_id": "construction",
            "started_at": "2026-03-19T11:55:00+00:00",
            "ended_at": "2026-03-19T12:10:00+00:00",
            "status": "ended",
            "event_count": 10,
            "flag_count": 1,
            "created_at": "2026-03-19T11:55:00+00:00",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.jsonl"
            config = PipelineConfig.from_mapping(
                {
                    "SUPABASE_URL": "https://example.supabase.co",
                    "SUPABASE_SECRET_KEY": "secret",
                    "PIPELINE_EXPORT_PATH": str(manifest_path),
                }
            )
            with (
                patch("oversite_pipeline.pipeline.SupabaseRestClient", return_value=fake_client),
                patch("oversite_pipeline.pipeline.fetch_image_events", return_value=[event]),
                patch(
                    "oversite_pipeline.pipeline.fetch_sessions_for_events",
                    return_value={"session-1": session},
                ),
                patch(
                    "oversite_pipeline.pipeline.fetch_image_asset",
                    return_value={
                        "fetch_status": "fetched",
                        "fetch_http_status": 200,
                        "fetched_at": "2026-03-19T12:00:05+00:00",
                        "raw_headers": {"Content-Type": "image/jpeg"},
                        "raw_error": None,
                        "image_sha256": "abc123",
                        "image_mime_type": "image/jpeg",
                        "image_width": 640,
                        "image_height": 480,
                        "image_bytes": 1200,
                    },
                ),
            ):
                summary = run_pipeline(
                    config=config,
                    limit=1,
                    dry_run=False,
                    window_start=None,
                    window_end=None,
                    skip_fetch=False,
                    export_manifest=True,
                    manifest_path=manifest_path,
                )
                manifest_rows = [
                    json.loads(line)
                    for line in manifest_path.read_text(encoding="utf-8").splitlines()
                ]

        self.assertEqual(summary["image_event_count"], 1)
        self.assertEqual(summary["asset_count"], 1)
        self.assertEqual(summary["observation_count"], 6)
        self.assertEqual(len(fake_client.tables["image_assets"]), 1)
        self.assertEqual(len(fake_client.tables["observations"]), 6)
        self.assertEqual(manifest_rows[0]["fetch_status"], "fetched")
        self.assertIn("observations", manifest_rows[0])

    def test_run_pipeline_keeps_non_flagged_image_events_without_creating_flag_rows(self) -> None:
        fake_client = _FakeClient()
        event = {
            "id": "event-2",
            "timestamp": "2026-03-19T12:05:00+00:00",
            "session_id": "session-2",
            "type": "task_delegated",
            "vertical_id": "construction",
            "text_content": "Assigned image follow-up for west stairwell.",
            "metadata": {
                "priority": "Medium",
                "trade": "General",
                "location": "West stairwell",
            },
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE999/SESSION999/2026-03-19T12-05-00-000Z.jpg"
            ),
            "device_id": "device-2",
            "created_at": "2026-03-19T12:05:00+00:00",
            "company_id": "company-2",
            "user_id": "user-2",
        }
        session = {
            "id": "session-row-2",
            "session_id": "session-2",
            "company_id": "company-2",
            "device_id": "device-2",
            "vertical_id": "construction",
            "started_at": "2026-03-19T12:00:00+00:00",
            "ended_at": "2026-03-19T12:10:00+00:00",
            "status": "ended",
            "event_count": 3,
            "flag_count": 0,
            "created_at": "2026-03-19T12:00:00+00:00",
        }
        config = PipelineConfig.from_mapping(
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_KEY": "secret",
            }
        )

        with (
            patch("oversite_pipeline.pipeline.SupabaseRestClient", return_value=fake_client),
            patch("oversite_pipeline.pipeline.fetch_image_events", return_value=[event]),
            patch(
                "oversite_pipeline.pipeline.fetch_sessions_for_events",
                return_value={"session-2": session},
            ),
            patch(
                "oversite_pipeline.pipeline.fetch_image_asset",
                return_value={
                    "fetch_status": "fetched",
                    "fetch_http_status": 200,
                    "fetched_at": "2026-03-19T12:05:05+00:00",
                    "raw_headers": {"Content-Type": "image/jpeg"},
                    "raw_error": None,
                    "image_sha256": "def456",
                    "image_mime_type": "image/jpeg",
                    "image_width": 320,
                    "image_height": 240,
                    "image_bytes": 900,
                },
            ),
        ):
            summary = run_pipeline(
                config=config,
                limit=1,
                dry_run=False,
                window_start=None,
                window_end=None,
                skip_fetch=False,
            )

        self.assertEqual(summary["image_count"], 1)
        self.assertEqual(summary["flag_count"], 0)
        self.assertEqual(fake_client.tables["images"][0]["event_type"], "task_delegated")
        self.assertFalse(fake_client.tables["images"][0]["is_flagged"])
        self.assertEqual(fake_client.tables["flags"], [])

    def test_run_pipeline_records_code_version_when_available(self) -> None:
        fake_client = _FakeClient()
        event = {
            "id": "event-3",
            "timestamp": "2026-03-19T12:00:00+00:00",
            "session_id": "session-3",
            "type": "issue_flagged",
            "vertical_id": "construction",
            "text_content": "Loose cable near panel.",
            "metadata": {"priority": "High", "trade": "Electrical", "location": "Panel room"},
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE111/SESSION111/2026-03-19T12-00-00-000Z.jpg"
            ),
            "device_id": "device-3",
            "created_at": "2026-03-19T12:00:00+00:00",
            "company_id": "company-3",
            "user_id": "user-3",
        }
        session = {
            "id": "session-row-3",
            "session_id": "session-3",
            "company_id": "company-3",
            "device_id": "device-3",
            "vertical_id": "construction",
            "started_at": "2026-03-19T11:55:00+00:00",
            "ended_at": "2026-03-19T12:10:00+00:00",
            "status": "ended",
            "event_count": 2,
            "flag_count": 1,
            "created_at": "2026-03-19T11:55:00+00:00",
        }
        config = PipelineConfig.from_mapping(
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_KEY": "secret",
            }
        )

        with (
            patch("oversite_pipeline.pipeline.SupabaseRestClient", return_value=fake_client),
            patch("oversite_pipeline.pipeline.fetch_image_events", return_value=[event]),
            patch(
                "oversite_pipeline.pipeline.fetch_sessions_for_events",
                return_value={"session-3": session},
            ),
            patch(
                "oversite_pipeline.pipeline.fetch_image_asset",
                return_value={
                    "fetch_status": "fetched",
                    "fetch_http_status": 200,
                    "fetched_at": "2026-03-19T12:00:05+00:00",
                    "raw_headers": {"Content-Type": "image/jpeg"},
                    "raw_error": None,
                    "image_sha256": "ghi789",
                    "image_mime_type": "image/jpeg",
                    "image_width": 640,
                    "image_height": 480,
                    "image_bytes": 1200,
                },
            ),
            patch("oversite_pipeline.pipeline._get_code_version", return_value="abc1234"),
        ):
            run_pipeline(
                config=config,
                limit=1,
                dry_run=False,
                window_start=None,
                window_end=None,
                skip_fetch=False,
            )

        self.assertEqual(fake_client.tables["pipeline_runs"][0]["code_version"], "abc1234")

    def test_run_pipeline_leaves_code_version_null_when_unavailable(self) -> None:
        fake_client = _FakeClient()
        event = {
            "id": "event-4",
            "timestamp": "2026-03-19T12:00:00+00:00",
            "session_id": "session-4",
            "type": "issue_flagged",
            "vertical_id": "construction",
            "text_content": "Loose cable near panel.",
            "metadata": {"priority": "High", "trade": "Electrical", "location": "Panel room"},
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE222/SESSION222/2026-03-19T12-00-00-000Z.jpg"
            ),
            "device_id": "device-4",
            "created_at": "2026-03-19T12:00:00+00:00",
            "company_id": "company-4",
            "user_id": "user-4",
        }
        session = {
            "id": "session-row-4",
            "session_id": "session-4",
            "company_id": "company-4",
            "device_id": "device-4",
            "vertical_id": "construction",
            "started_at": "2026-03-19T11:55:00+00:00",
            "ended_at": "2026-03-19T12:10:00+00:00",
            "status": "ended",
            "event_count": 2,
            "flag_count": 1,
            "created_at": "2026-03-19T11:55:00+00:00",
        }
        config = PipelineConfig.from_mapping(
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_KEY": "secret",
            }
        )

        with (
            patch("oversite_pipeline.pipeline.SupabaseRestClient", return_value=fake_client),
            patch("oversite_pipeline.pipeline.fetch_image_events", return_value=[event]),
            patch(
                "oversite_pipeline.pipeline.fetch_sessions_for_events",
                return_value={"session-4": session},
            ),
            patch(
                "oversite_pipeline.pipeline.fetch_image_asset",
                return_value={
                    "fetch_status": "fetched",
                    "fetch_http_status": 200,
                    "fetched_at": "2026-03-19T12:00:05+00:00",
                    "raw_headers": {"Content-Type": "image/jpeg"},
                    "raw_error": None,
                    "image_sha256": "jkl012",
                    "image_mime_type": "image/jpeg",
                    "image_width": 640,
                    "image_height": 480,
                    "image_bytes": 1200,
                },
            ),
            patch("oversite_pipeline.pipeline._get_code_version", return_value=None),
        ):
            run_pipeline(
                config=config,
                limit=1,
                dry_run=False,
                window_start=None,
                window_end=None,
                skip_fetch=False,
            )

        self.assertIsNone(fake_client.tables["pipeline_runs"][0]["code_version"])
