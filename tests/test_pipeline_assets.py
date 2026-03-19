import hashlib
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.assets import build_image_asset_record, inspect_image_bytes


PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
    b"\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x00\x05\xfe\x02\xfeA\x8d\x89\xb0"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class InspectImageBytesTests(unittest.TestCase):
    def test_inspect_image_bytes_extracts_sha_size_mime_and_dimensions(self) -> None:
        inspection = inspect_image_bytes(PNG_1X1, content_type="image/png")

        self.assertEqual(inspection["image_mime_type"], "image/png")
        self.assertEqual(inspection["image_width"], 1)
        self.assertEqual(inspection["image_height"], 1)
        self.assertEqual(inspection["image_bytes"], len(PNG_1X1))
        self.assertEqual(inspection["image_sha256"], hashlib.sha256(PNG_1X1).hexdigest())


class BuildImageAssetRecordTests(unittest.TestCase):
    def test_build_image_asset_record_preserves_fetch_and_image_metadata(self) -> None:
        image_record = {
            "id": "image-1",
            "source_event_id": "event-1",
            "blob_url": "https://example.supabase.co/storage/v1/object/public/engagement-blobs/device/session/a.png",
            "storage_bucket": "engagement-blobs",
            "storage_path": "device/session/a.png",
        }
        download = {
            "fetch_status": "fetched",
            "fetch_http_status": 200,
            "fetched_at": "2026-03-19T12:00:00+00:00",
            "raw_headers": {"Content-Type": "image/png"},
            "raw_error": None,
            "image_sha256": "abc123",
            "image_mime_type": "image/png",
            "image_width": 1,
            "image_height": 1,
            "image_bytes": 68,
        }

        asset_record = build_image_asset_record(
            pipeline_run_id="run-1",
            image_record=image_record,
            download=download,
        )

        self.assertEqual(asset_record["pipeline_run_id"], "run-1")
        self.assertEqual(asset_record["derived_image_id"], "image-1")
        self.assertEqual(asset_record["source_bucket"], "engagement-blobs")
        self.assertEqual(asset_record["source_path"], "device/session/a.png")
        self.assertEqual(asset_record["fetch_status"], "fetched")
        self.assertEqual(asset_record["fetch_http_status"], 200)
        self.assertEqual(asset_record["sha256"], "abc123")
        self.assertEqual(asset_record["mime_type"], "image/png")
        self.assertEqual(asset_record["width"], 1)
        self.assertEqual(asset_record["height"], 1)
        self.assertEqual(asset_record["byte_size"], 68)
