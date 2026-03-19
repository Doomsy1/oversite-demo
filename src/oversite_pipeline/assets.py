from __future__ import annotations

import hashlib
import json
import struct
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_png_dimensions(payload: bytes) -> tuple[int | None, int | None]:
    if len(payload) < 24 or payload[:8] != b"\x89PNG\r\n\x1a\n":
        return None, None
    width = int.from_bytes(payload[16:20], "big")
    height = int.from_bytes(payload[20:24], "big")
    return width, height


def _parse_gif_dimensions(payload: bytes) -> tuple[int | None, int | None]:
    if len(payload) < 10 or payload[:6] not in {b"GIF87a", b"GIF89a"}:
        return None, None
    width = int.from_bytes(payload[6:8], "little")
    height = int.from_bytes(payload[8:10], "little")
    return width, height


def _parse_jpeg_dimensions(payload: bytes) -> tuple[int | None, int | None]:
    if len(payload) < 4 or payload[:2] != b"\xff\xd8":
        return None, None
    offset = 2
    while offset + 9 < len(payload):
        if payload[offset] != 0xFF:
            offset += 1
            continue
        marker = payload[offset + 1]
        offset += 2
        if marker in {0xD8, 0xD9}:
            continue
        if offset + 2 > len(payload):
            break
        segment_length = int.from_bytes(payload[offset : offset + 2], "big")
        if segment_length < 2 or offset + segment_length > len(payload):
            break
        if marker in {
            0xC0,
            0xC1,
            0xC2,
            0xC3,
            0xC5,
            0xC6,
            0xC7,
            0xC9,
            0xCA,
            0xCB,
            0xCD,
            0xCE,
            0xCF,
        } and segment_length >= 7:
            height = int.from_bytes(payload[offset + 3 : offset + 5], "big")
            width = int.from_bytes(payload[offset + 5 : offset + 7], "big")
            return width, height
        offset += segment_length
    return None, None


def _parse_webp_dimensions(payload: bytes) -> tuple[int | None, int | None]:
    if len(payload) < 30 or payload[:4] != b"RIFF" or payload[8:12] != b"WEBP":
        return None, None
    chunk = payload[12:16]
    if chunk == b"VP8 " and len(payload) >= 30:
        width, height = struct.unpack("<HH", payload[26:30])
        return width & 0x3FFF, height & 0x3FFF
    if chunk == b"VP8L" and len(payload) >= 25:
        bits = int.from_bytes(payload[21:25], "little")
        width = (bits & 0x3FFF) + 1
        height = ((bits >> 14) & 0x3FFF) + 1
        return width, height
    if chunk == b"VP8X" and len(payload) >= 30:
        width = int.from_bytes(payload[24:27], "little") + 1
        height = int.from_bytes(payload[27:30], "little") + 1
        return width, height
    return None, None


def _extract_dimensions(payload: bytes) -> tuple[int | None, int | None]:
    for parser in (
        _parse_png_dimensions,
        _parse_jpeg_dimensions,
        _parse_gif_dimensions,
        _parse_webp_dimensions,
    ):
        width, height = parser(payload)
        if width is not None and height is not None:
            return width, height
    return None, None


def inspect_image_bytes(payload: bytes, *, content_type: str | None) -> dict[str, Any]:
    width, height = _extract_dimensions(payload)
    return {
        "image_sha256": hashlib.sha256(payload).hexdigest(),
        "image_mime_type": content_type,
        "image_width": width,
        "image_height": height,
        "image_bytes": len(payload),
    }


def fetch_image_asset(*, blob_url: str, timeout_seconds: int) -> dict[str, Any]:
    request = Request(blob_url, method="GET")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
            content_type = response.headers.get("Content-Type")
            inspection = inspect_image_bytes(payload, content_type=content_type)
            return {
                "fetch_status": "fetched",
                "fetch_http_status": response.status,
                "fetched_at": _utc_now_iso(),
                "raw_headers": dict(response.headers.items()),
                "raw_error": None,
                **inspection,
            }
    except Exception as exc:  # pragma: no cover - exercised via pipeline integration
        return {
            "fetch_status": "failed",
            "fetch_http_status": getattr(exc, "code", None),
            "fetched_at": _utc_now_iso(),
            "raw_headers": None,
            "raw_error": {"type": type(exc).__name__, "message": str(exc)},
            "image_sha256": None,
            "image_mime_type": None,
            "image_width": None,
            "image_height": None,
            "image_bytes": None,
        }


def build_image_asset_record(
    *,
    pipeline_run_id: str,
    image_record: dict[str, Any],
    download: dict[str, Any],
) -> dict[str, Any]:
    return {
        "last_materialized_run_id": pipeline_run_id,
        "derived_image_id": image_record["id"],
        "source_bucket": image_record["storage_bucket"],
        "source_path": image_record["storage_path"],
        "source_url": image_record["blob_url"],
        "fetch_status": download["fetch_status"],
        "fetch_http_status": download.get("fetch_http_status"),
        "fetched_at": download.get("fetched_at"),
        "byte_size": download.get("image_bytes"),
        "sha256": download.get("image_sha256"),
        "mime_type": download.get("image_mime_type"),
        "width": download.get("image_width"),
        "height": download.get("image_height"),
        "curated_bucket": None,
        "curated_path": None,
        "raw_headers": download.get("raw_headers"),
        "raw_error": download.get("raw_error"),
    }


def update_image_from_download(
    image_record: dict[str, Any],
    download: dict[str, Any],
) -> dict[str, Any]:
    return {
        **image_record,
        "image_fetch_status": download["fetch_status"],
        "image_sha256": download.get("image_sha256"),
        "image_mime_type": download.get("image_mime_type"),
        "image_width": download.get("image_width"),
        "image_height": download.get("image_height"),
        "image_bytes": download.get("image_bytes"),
    }
