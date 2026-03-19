import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.config import PipelineConfig


_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_local_pipeline.py"
_SPEC = importlib.util.spec_from_file_location("run_local_pipeline_script", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
run_local_pipeline = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(run_local_pipeline)


class LocalPipelineScriptTests(unittest.TestCase):
    def test_build_local_bundle_preserves_download_metadata_in_image_assets(self) -> None:
        event = {
            "id": "event-1",
            "timestamp": "2026-03-19T12:00:00+00:00",
            "session_id": "session-1",
            "type": "issue_flagged",
            "vertical_id": "construction",
            "text_content": "Loose cable near panel.",
            "metadata": {"priority": "High", "trade": "Electrical", "location": "Panel room"},
            "blob_url": (
                "https://example.supabase.co/storage/v1/object/public/engagement-blobs/"
                "DEVICE111/SESSION111/2026-03-19T12-00-00-000Z.jpg"
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
            "event_count": 2,
            "flag_count": 1,
            "created_at": "2026-03-19T11:55:00+00:00",
        }
        download = {
            "fetch_status": "failed",
            "fetch_http_status": 403,
            "fetched_at": "2026-03-19T12:00:05+00:00",
            "raw_headers": {"Content-Type": "image/jpeg", "X-Test": "1"},
            "raw_error": {"type": "HTTPError", "message": "forbidden"},
            "image_sha256": None,
            "image_mime_type": None,
            "image_width": None,
            "image_height": None,
            "image_bytes": None,
        }
        config = PipelineConfig.from_mapping(
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_KEY": "secret",
            }
        )

        with (
            patch.object(run_local_pipeline, "SupabaseRestClient"),
            patch.object(run_local_pipeline, "fetch_image_events", return_value=[event]),
            patch.object(
                run_local_pipeline,
                "fetch_sessions_for_events",
                return_value={"session-1": session},
            ),
            patch.object(run_local_pipeline, "fetch_image_asset", return_value=download),
        ):
            bundle = run_local_pipeline.build_local_bundle(config=config, limit=1)

        asset_row = bundle["image_assets"][0]
        self.assertEqual(asset_row["fetch_status"], "failed")
        self.assertEqual(asset_row["fetch_http_status"], 403)
        self.assertEqual(asset_row["fetched_at"], "2026-03-19T12:00:05+00:00")
        self.assertEqual(asset_row["raw_headers"], {"Content-Type": "image/jpeg", "X-Test": "1"})
        self.assertEqual(asset_row["raw_error"], {"type": "HTTPError", "message": "forbidden"})


if __name__ == "__main__":
    unittest.main()
