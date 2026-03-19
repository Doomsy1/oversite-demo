from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def build_select_url(
    *,
    base_url: str,
    schema: str,
    table: str,
    columns: str,
    filters: dict[str, str] | Sequence[tuple[str, str]] | None = None,
    order: str | None = None,
    limit: int | None = None,
) -> str:
    del schema
    query: list[tuple[str, str]] = [("select", columns)]
    if filters:
        query.extend(filters.items() if isinstance(filters, dict) else filters)
    if order:
        query.append(("order", order))
    if limit is not None:
        query.append(("limit", str(limit)))
    return f"{base_url.rstrip('/')}/rest/v1/{table}?{urlencode(query)}"


def build_headers(
    *,
    secret_key: str,
    schema: str,
    method: str,
    prefer: str | None = None,
) -> dict[str, str]:
    headers = {
        "apikey": secret_key,
        "Authorization": f"Bearer {secret_key}",
        "User-Agent": "oversite-derived-pipeline",
    }
    if method.upper() == "GET":
        headers["Accept-Profile"] = schema
    else:
        headers["Content-Profile"] = schema
        headers["Content-Type"] = "application/json"
    if prefer:
        headers["Prefer"] = prefer
    return headers


class SupabaseRestClient:
    def __init__(self, *, base_url: str, secret_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.secret_key = secret_key

    def select(
        self,
        *,
        schema: str,
        table: str,
        columns: str,
        filters: dict[str, str] | Sequence[tuple[str, str]] | None = None,
        order: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        url = build_select_url(
            base_url=self.base_url,
            schema=schema,
            table=table,
            columns=columns,
            filters=filters,
            order=order,
            limit=limit,
        )
        headers = build_headers(
            secret_key=self.secret_key,
            schema=schema,
            method="GET",
        )
        request = Request(url, method="GET", headers=headers)
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def upsert(
        self,
        *,
        schema: str,
        table: str,
        rows: list[dict[str, Any]],
        on_conflict: str,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        url = (
            f"{self.base_url}/rest/v1/{table}?"
            f"{urlencode([('on_conflict', on_conflict)])}"
        )
        headers = build_headers(
            secret_key=self.secret_key,
            schema=schema,
            method="POST",
            prefer="resolution=merge-duplicates,return=representation",
        )
        request = Request(
            url,
            data=json.dumps(rows).encode("utf-8"),
            method="POST",
            headers=headers,
        )
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def insert(
        self,
        *,
        schema: str,
        table: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        url = f"{self.base_url}/rest/v1/{table}"
        headers = build_headers(
            secret_key=self.secret_key,
            schema=schema,
            method="POST",
            prefer="return=representation",
        )
        request = Request(
            url,
            data=json.dumps(rows).encode("utf-8"),
            method="POST",
            headers=headers,
        )
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
