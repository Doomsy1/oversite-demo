from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect source coverage using the pipeline's dry-run path."
    )
    parser.add_argument("--env-file", default=".env", help="Path to env file")
    parser.add_argument("--limit", type=int, default=10, help="Optional event limit")
    args = parser.parse_args()

    config = PipelineConfig.from_env_file(args.env_file)
    summary = run_pipeline(config=config, limit=args.limit, dry_run=True)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
