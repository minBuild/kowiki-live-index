from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KOWIKI_", env_file=".env", extra="ignore")

    wiki: str = "kowiki"
    wiki_domain: str = "ko.wikipedia.org"
    user_agent: str = "kowiki-live-index/0.1 (https://github.com/minBuild/kowiki-live-index)"

    kafka_bootstrap: str = "localhost:29092"
    topic_prefix: str = ""
    topic_partitions: int = 6
    topic_replicas: int = 1
    group_prefix: str = "kowiki"
    batch_size: int = 16
    max_poll_interval_ms: int = 600_000
    claim_check_bytes: int = 512_000

    blob_url: str = "file:///tmp/kowiki-blobs"
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"

    qdrant_url: str = "http://localhost:6333"
    qdrant_api_key: str | None = None
    collection: str = "kowiki"
    collection_version: str = "v1"

    embed_backend: Literal["openai", "hash"] = "openai"
    embed_base_url: str = "http://localhost:11434/v1"
    embed_model: str = "bge-m3"
    embed_api_key: str | None = None
    embed_dim: int | None = None
    embed_batch: int = 32
    embed_query_prefix: str = ""
    embed_doc_prefix: str = ""
    reuse_vectors: bool = True

    chunk_strategy: Literal["section", "fixed"] = "section"
    chunk_max_tokens: int = 400
    chunk_overlap_tokens: int = 40
    tokenizer: str | None = None

    metrics_port: int = 9100
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
