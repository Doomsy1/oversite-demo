from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.local_sqlite import open_local_connection


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect a local SQLite derived database.")
    parser.add_argument(
        "--db-path",
        default="local/derived_inspection.db",
        help="Path to the local SQLite file",
    )
    args = parser.parse_args()

    conn = open_local_connection(Path(args.db_path))
    try:
        summary = {
            "priority_counts": [
                dict(row)
                for row in conn.execute(
                    "select coalesce(priority, '<null>') as priority, count(*) as image_count "
                    "from derived.images "
                    "group by coalesce(priority, '<null>') "
                    "order by image_count desc, priority"
                ).fetchall()
            ],
            "trade_counts": [
                dict(row)
                for row in conn.execute(
                    "select coalesce(trade, '<null>') as trade, count(*) as image_count "
                    "from derived.images "
                    "group by coalesce(trade, '<null>') "
                    "order by image_count desc, trade "
                    "limit 12"
                ).fetchall()
            ],
            "node_type_counts": [
                dict(row)
                for row in conn.execute(
                    "select node_type, count(*) as node_count "
                    "from derived.graph_nodes "
                    "group by node_type "
                    "order by node_type"
                ).fetchall()
            ],
            "non_flagged_images": [
                dict(row)
                for row in conn.execute(
                    "select source_event_id, event_type, event_summary "
                    "from derived.images "
                    "where is_flagged = 0 "
                    "order by event_timestamp"
                ).fetchall()
            ],
            "fetch_status_counts": [
                dict(row)
                for row in conn.execute(
                    "select fetch_status, count(*) as asset_count "
                    "from derived.image_assets "
                    "group by fetch_status "
                    "order by fetch_status"
                ).fetchall()
            ],
            "observation_family_counts": [
                dict(row)
                for row in conn.execute(
                    "select observation_family, count(*) as observation_count "
                    "from derived.observations "
                    "group by observation_family "
                    "order by observation_family"
                ).fetchall()
            ],
            "sample_edges": [
                dict(row)
                for row in conn.execute(
                    "select edge_type, src_node_type, src_node_label, dst_node_type, dst_node_label "
                    "from derived.v_graph_edges_enriched "
                    "order by edge_type, src_node_label, dst_node_label "
                    "limit 12"
                ).fetchall()
            ],
            "top_sessions": [
                dict(row)
                for row in conn.execute(
                    "select source_session_id, image_count, flagged_image_count, status "
                    "from derived.v_session_image_summary "
                    "order by image_count desc, source_session_id "
                    "limit 10"
                ).fetchall()
            ],
            "manifest_preview": [
                dict(row)
                for row in conn.execute(
                    "select image_id, fetch_status, priority, trade, location_hint, observations_json "
                    "from derived.v_ml_dataset_manifest "
                    "order by event_timestamp "
                    "limit 5"
                ).fetchall()
            ],
        }
    finally:
        conn.close()

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
