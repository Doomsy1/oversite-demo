import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.cli import main


class PipelineCliTests(unittest.TestCase):
    def test_run_subcommand_delegates_to_pipeline_and_prints_summary(self) -> None:
        stdout = io.StringIO()
        with (
            patch("oversite_pipeline.cli.PipelineConfig.from_env_file", return_value="config"),
            patch(
                "oversite_pipeline.cli.run_pipeline",
                return_value={"dry_run": True, "image_event_count": 3},
            ) as run_pipeline_mock,
            patch("sys.stdout", stdout),
        ):
            exit_code = main(["run", "--env-file", "custom.env", "--dry-run", "--limit", "3"])

        self.assertEqual(exit_code, 0)
        run_pipeline_mock.assert_called_once()
        _, kwargs = run_pipeline_mock.call_args
        self.assertEqual(kwargs["config"], "config")
        self.assertEqual(kwargs["limit"], 3)
        self.assertTrue(kwargs["dry_run"])
        self.assertEqual(json.loads(stdout.getvalue()), {"dry_run": True, "image_event_count": 3})
