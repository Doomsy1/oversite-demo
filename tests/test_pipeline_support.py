import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oversite_pipeline.clients.supabase_rest import build_headers, build_select_url
from oversite_pipeline.config import PipelineConfig
from oversite_pipeline.extract.source import (
    collect_session_ids,
    fetch_image_events,
    fetch_sessions_for_events,
)
from oversite_pipeline.load.prepare import dedupe_graph_rows, materialize_graph_edges


class PipelineConfigTests(unittest.TestCase):
    def test_from_mapping_builds_config_with_defaults(self) -> None:
        config = PipelineConfig.from_mapping(
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_KEY": "secret",
            }
        )

        self.assertEqual(config.supabase_url, "https://example.supabase.co")
        self.assertEqual(config.supabase_secret_key, "secret")
        self.assertEqual(config.source_schema, "public")
        self.assertEqual(config.derived_schema, "derived")
        self.assertEqual(config.storage_bucket, "engagement-blobs")


class SupabaseRequestBuilderTests(unittest.TestCase):
    def test_build_select_url_includes_filters_order_and_limit(self) -> None:
        url = build_select_url(
            base_url="https://example.supabase.co",
            schema="public",
            table="events",
            columns="id,timestamp",
            filters={"blob_url": "not.is.null", "type": "eq.issue_flagged"},
            order="timestamp.desc",
            limit=10,
        )

        self.assertIn("/rest/v1/events?", url)
        self.assertIn("select=id%2Ctimestamp", url)
        self.assertIn("blob_url=not.is.null", url)
        self.assertIn("type=eq.issue_flagged", url)
        self.assertIn("order=timestamp.desc", url)
        self.assertIn("limit=10", url)

    def test_build_headers_sets_schema_profiles(self) -> None:
        headers = build_headers(
            secret_key="secret",
            schema="derived",
            method="POST",
            prefer="resolution=merge-duplicates",
        )

        self.assertEqual(headers["apikey"], "secret")
        self.assertEqual(headers["Authorization"], "Bearer secret")
        self.assertEqual(headers["Content-Profile"], "derived")
        self.assertEqual(headers["Prefer"], "resolution=merge-duplicates")


class ExtractSupportTests(unittest.TestCase):
    def test_collect_session_ids_returns_distinct_source_session_ids(self) -> None:
        session_ids = collect_session_ids(
            [
                {"session_id": "session-2"},
                {"session_id": "session-1"},
                {"session_id": "session-2"},
            ]
        )

        self.assertEqual(session_ids, ["session-1", "session-2"])

    def test_fetch_image_events_applies_start_window_without_end(self) -> None:
        client = _RecordingSelectClient()

        fetch_image_events(
            client,
            schema="public",
            limit=5,
            window_start="2026-03-18T00:00:00+00:00",
            window_end=None,
        )

        self.assertEqual(
            client.calls[0]["filters"],
            [
                ("blob_url", "not.is.null"),
                ("timestamp", "gte.2026-03-18T00:00:00+00:00"),
            ],
        )

    def test_fetch_image_events_applies_end_window_without_start(self) -> None:
        client = _RecordingSelectClient()

        fetch_image_events(
            client,
            schema="public",
            limit=5,
            window_start=None,
            window_end="2026-03-19T00:00:00+00:00",
        )

        self.assertEqual(
            client.calls[0]["filters"],
            [
                ("blob_url", "not.is.null"),
                ("timestamp", "lte.2026-03-19T00:00:00+00:00"),
            ],
        )

    def test_fetch_image_events_applies_both_window_bounds(self) -> None:
        client = _RecordingSelectClient()

        fetch_image_events(
            client,
            schema="public",
            limit=5,
            window_start="2026-03-18T00:00:00+00:00",
            window_end="2026-03-19T00:00:00+00:00",
        )

        self.assertEqual(
            client.calls[0]["filters"],
            [
                ("blob_url", "not.is.null"),
                ("timestamp", "gte.2026-03-18T00:00:00+00:00"),
                ("timestamp", "lte.2026-03-19T00:00:00+00:00"),
            ],
        )

    def test_fetch_image_events_paginates_until_source_exhaustion(self) -> None:
        client = _RecordingSelectClient(
            pages=[
                [
                    {"id": "event-1"},
                    {"id": "event-2"},
                ],
                [
                    {"id": "event-3"},
                ],
            ]
        )

        rows = fetch_image_events(
            client,
            schema="public",
            limit=None,
            window_start=None,
            window_end=None,
            page_size=2,
        )

        self.assertEqual([row["id"] for row in rows], ["event-1", "event-2", "event-3"])
        self.assertEqual([call["offset"] for call in client.calls], [0, 2])
        self.assertEqual([call["limit"] for call in client.calls], [2, 2])

    def test_fetch_image_events_respects_limit_across_page_boundaries(self) -> None:
        client = _RecordingSelectClient(
            pages=[
                [
                    {"id": "event-1"},
                    {"id": "event-2"},
                ],
                [
                    {"id": "event-3"},
                    {"id": "event-4"},
                ],
            ]
        )

        rows = fetch_image_events(
            client,
            schema="public",
            limit=3,
            window_start=None,
            window_end=None,
            page_size=2,
        )

        self.assertEqual([row["id"] for row in rows], ["event-1", "event-2", "event-3"])
        self.assertEqual([call["offset"] for call in client.calls], [0, 2])
        self.assertEqual([call["limit"] for call in client.calls], [2, 1])

    def test_fetch_sessions_for_events_batches_large_session_id_sets(self) -> None:
        client = _RecordingSelectClient(
            pages=[
                [
                    {"session_id": "session-1", "id": "row-1"},
                    {"session_id": "session-2", "id": "row-2"},
                ],
                [
                    {"session_id": "session-3", "id": "row-3"},
                ],
            ]
        )

        rows = fetch_sessions_for_events(
            client,
            schema="public",
            events=[
                {"session_id": "session-1"},
                {"session_id": "session-2"},
                {"session_id": "session-3"},
            ],
            page_size=2,
        )

        self.assertEqual(sorted(rows), ["session-1", "session-2", "session-3"])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.calls[0]["filters"], {"session_id": "in.(session-1,session-2)"})
        self.assertEqual(client.calls[1]["filters"], {"session_id": "in.(session-3)"})


class _RecordingSelectClient:
    def __init__(self, pages: list[list[dict[str, object]]] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.pages = pages or []

    def select(
        self,
        *,
        schema: str,
        table: str,
        columns: str,
        filters: list[tuple[str, str]] | dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "schema": schema,
                "table": table,
                "columns": columns,
                "filters": filters,
                "order": order,
                "limit": limit,
                "offset": offset,
            }
        )
        if not self.pages:
            return []
        page = self.pages.pop(0)
        return page if limit is None else page[:limit]


class LoadSupportTests(unittest.TestCase):
    def test_dedupe_graph_rows_removes_duplicate_nodes_and_aggregates_edge_recurrence(self) -> None:
        nodes = [
            {"node_type": "trade", "node_key": "trade:safety", "display_label": "Safety"},
            {"node_type": "trade", "node_key": "trade:safety", "display_label": "Safety"},
        ]
        edges = [
            {
                "src_node_key": "hazard:exposed wiring",
                "edge_type": "hazard_near_location_hint",
                "dst_node_key": "location_hint:near panel a",
                "evidence_image_id": "image-1",
                "evidence_session_id": "session-1",
                "attributes": {"event_timestamp": "2026-03-18T22:02:05.602+00:00"},
            },
            {
                "src_node_key": "hazard:exposed wiring",
                "edge_type": "hazard_near_location_hint",
                "dst_node_key": "location_hint:near panel a",
                "evidence_image_id": "image-2",
                "evidence_session_id": "session-2",
                "attributes": {"event_timestamp": "2026-03-19T22:02:05.602+00:00"},
            },
        ]

        deduped = dedupe_graph_rows(nodes=nodes, edges=edges)

        self.assertEqual(len(deduped["nodes"]), 1)
        self.assertEqual(len(deduped["edges"]), 1)
        self.assertEqual(
            deduped["edges"][0]["attributes"],
            {
                "evidence_image_count": 2,
                "evidence_session_count": 2,
                "first_seen_at": "2026-03-18T22:02:05.602+00:00",
                "last_seen_at": "2026-03-19T22:02:05.602+00:00",
            },
        )

    def test_dedupe_graph_rows_preserves_zero_image_evidence_count(self) -> None:
        deduped = dedupe_graph_rows(
            nodes=[],
            edges=[
                {
                    "src_node_key": "session:session-1",
                    "edge_type": "belongs_to_company",
                    "dst_node_key": "company:company-1",
                    "evidence_image_id": None,
                    "evidence_session_id": "derived-session-1",
                    "attributes": {"event_timestamp": "2026-03-18T22:02:05.602+00:00"},
                }
            ],
        )

        self.assertEqual(
            deduped["edges"][0]["attributes"],
            {
                "evidence_image_count": 0,
                "evidence_session_count": 1,
                "first_seen_at": "2026-03-18T22:02:05.602+00:00",
                "last_seen_at": "2026-03-18T22:02:05.602+00:00",
            },
        )

    def test_materialize_graph_edges_replaces_node_keys_with_node_ids(self) -> None:
        edges = [
            {
                "pipeline_run_id": "run-1",
                "src_node_key": "image:event-1",
                "edge_type": "occurred_in_session",
                "dst_node_key": "session:session-1",
                "evidence_image_id": "image-1",
                "evidence_session_id": "session-row-1",
                "source_kind": "derived_rule",
                "attributes": None,
            }
        ]
        node_map = {
            ("image", "image:event-1"): {"id": "node-image-1"},
            ("session", "session:session-1"): {"id": "node-session-1"},
        }

        materialized = materialize_graph_edges(
            edges=edges,
            node_map=node_map,
            edge_node_types={"occurred_in_session": ("image", "session")},
        )

        self.assertEqual(
            materialized,
            [
                {
                    "pipeline_run_id": "run-1",
                    "src_node_id": "node-image-1",
                    "edge_type": "occurred_in_session",
                    "dst_node_id": "node-session-1",
                    "evidence_image_id": "image-1",
                    "evidence_session_id": "session-row-1",
                    "source_kind": "derived_rule",
                    "attributes": None,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
