import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.observations import canonicalize_key, derive_observations


class CanonicalizeKeyTests(unittest.TestCase):
    def test_canonicalize_key_normalizes_case_and_spacing(self) -> None:
        self.assertEqual(canonicalize_key("  East wall,  Stairwell  "), "east wall stairwell")


class DeriveObservationsTests(unittest.TestCase):
    def test_derive_observations_emits_source_and_text_backed_labels(self) -> None:
        image_record = {
            "id": "image-1",
            "last_materialized_run_id": "run-1",
            "trade": "Electrical",
            "location_hint": "East wall stairwell",
            "site_hint": None,
            "event_summary": (
                "Worker on ladder near exposed wiring beside drywall stack. "
                "Daily site report posted nearby."
            ),
            "raw_metadata": {
                "trade": "Electrical",
                "location": "East wall stairwell",
            },
        }

        observations = derive_observations(image_record)

        pairs = {(row["observation_family"], row["canonical_key"]) for row in observations}
        self.assertIn(("trade", "electrical"), pairs)
        self.assertIn(("location_hint", "east wall stairwell"), pairs)
        self.assertIn(("hazard", "exposed wiring"), pairs)
        self.assertIn(("equipment", "ladder"), pairs)
        self.assertIn(("material", "drywall"), pairs)
        self.assertIn(("worker_activity", "worker on ladder"), pairs)
        self.assertIn(("documentation_artifact", "daily site report"), pairs)

        trade_row = next(row for row in observations if row["observation_family"] == "trade")
        self.assertEqual(trade_row["confidence"], "high")
        self.assertEqual(trade_row["source_kind"], "source_metadata")

        hazard_row = next(row for row in observations if row["observation_family"] == "hazard")
        self.assertEqual(hazard_row["confidence"], "medium")
        self.assertEqual(hazard_row["source_kind"], "source_text")
