from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.cli import main as cli_main


def main() -> int:
    return cli_main(["run", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
