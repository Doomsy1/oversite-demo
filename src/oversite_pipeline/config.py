from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


@dataclass(frozen=True)
class PipelineConfig:
    supabase_url: str
    supabase_secret_key: str
    source_schema: str
    derived_schema: str
    source_bucket: str
    pipeline_page_size: int
    fetch_timeout_seconds: int
    export_path: str

    @property
    def storage_bucket(self) -> str:
        return self.source_bucket

    @classmethod
    def from_mapping(cls, mapping: dict[str, str]) -> "PipelineConfig":
        return cls(
            supabase_url=mapping["SUPABASE_URL"].strip(),
            supabase_secret_key=mapping["SUPABASE_SECRET_KEY"].strip(),
            source_schema=mapping.get("SUPABASE_SCHEMA", "public").strip() or "public",
            derived_schema=mapping.get("DERIVED_SCHEMA", "derived").strip() or "derived",
            source_bucket=(
                mapping.get("SOURCE_BUCKET")
                or mapping.get("SUPABASE_STORAGE_BUCKET")
                or "engagement-blobs"
            ).strip()
            or "engagement-blobs",
            pipeline_page_size=int(mapping.get("PIPELINE_PAGE_SIZE", "200")),
            fetch_timeout_seconds=int(mapping.get("PIPELINE_FETCH_TIMEOUT_SECONDS", "30")),
            export_path=mapping.get("PIPELINE_EXPORT_PATH", "local/ml_manifest.jsonl").strip()
            or "local/ml_manifest.jsonl",
        )

    @classmethod
    def from_env_file(cls, env_path: str = ".env") -> "PipelineConfig":
        return cls.from_mapping(_read_env_file(Path(env_path)))
