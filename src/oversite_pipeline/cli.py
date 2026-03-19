from __future__ import annotations

import argparse
import json
from pathlib import Path

from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.pipeline import run_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Oversite ML image pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the Supabase-first pipeline")
    run_parser.add_argument("--env-file", default=".env", help="Path to env file")
    run_parser.add_argument("--window-start", default=None, help="Inclusive ISO timestamp filter")
    run_parser.add_argument("--window-end", default=None, help="Inclusive ISO timestamp filter")
    run_parser.add_argument("--limit", type=int, default=None, help="Optional image event limit")
    run_parser.add_argument("--dry-run", action="store_true", help="Read and summarize only")
    run_parser.add_argument("--skip-fetch", action="store_true", help="Skip asset download stage")
    run_parser.add_argument(
        "--export-manifest",
        action="store_true",
        help="Export manifest rows to JSON lines after the run",
    )
    run_parser.add_argument("--manifest-path", default=None, help="Path for exported manifest")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "run":
        parser.error("Unknown command")

    config = PipelineConfig.from_env_file(args.env_file)
    summary = run_pipeline(
        config=config,
        limit=args.limit,
        dry_run=args.dry_run,
        window_start=args.window_start,
        window_end=args.window_end,
        skip_fetch=args.skip_fetch,
        export_manifest=args.export_manifest,
        manifest_path=Path(args.manifest_path) if args.manifest_path else None,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
