import gzip
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from kowiki.errors import TransientError
from kowiki.settings import Settings


class BlobStore(Protocol):
    def put(self, key: str, data: bytes) -> str: ...

    def get(self, uri: str) -> bytes: ...

    def read(self, key: str) -> bytes | None: ...

    def ensure(self) -> None: ...


class FileBlobStore:
    def __init__(self, root: Path):
        self.root = root

    def put(self, key: str, data: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(path)
        return path.as_uri()

    def get(self, uri: str) -> bytes:
        return Path(urlparse(uri).path).read_bytes()

    def read(self, key: str) -> bytes | None:
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def ensure(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)


class S3BlobStore:
    def __init__(self, client, bucket: str, prefix: str = ""):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix

    def put(self, key: str, data: bytes) -> str:
        name = self.prefix + key
        self._call(self.client.put_object, Bucket=self.bucket, Key=name, Body=data)
        return f"s3://{self.bucket}/{name}"

    def get(self, uri: str) -> bytes:
        url = urlparse(uri)
        return self._call(self.client.get_object, Bucket=url.netloc, Key=url.path.lstrip("/"))["Body"].read()

    def read(self, key: str) -> bytes | None:
        try:
            return self.get(f"s3://{self.bucket}/{self.prefix}{key}")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def ensure(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def _call(self, fn, **kwargs):
        try:
            return fn(**kwargs)
        except (BotoConnectionError, HTTPClientError) as e:
            raise TransientError(f"s3: {e}") from e
        except ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if status == 429 or status >= 500:
                raise TransientError(f"s3 {status}: {e}") from e
            raise


def open_blobstore(settings: Settings) -> BlobStore:
    url = urlparse(settings.blob_url)
    if url.scheme == "file":
        return FileBlobStore(Path(url.path))
    if url.scheme == "s3":
        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint or None,
            aws_access_key_id=settings.s3_access_key or None,
            aws_secret_access_key=settings.s3_secret_key or None,
            region_name=settings.s3_region,
            config=Config(
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        prefix = url.path.strip("/")
        return S3BlobStore(client, url.netloc, f"{prefix}/" if prefix else "")
    raise ValueError(f"unsupported blob url: {settings.blob_url}")


def put_text(store: BlobStore, key: str, text: str) -> str:
    return store.put(key, gzip.compress(text.encode()))


def get_text(store: BlobStore, uri: str) -> str:
    data = store.get(uri)
    return (gzip.decompress(data) if uri.endswith(".gz") else data).decode()
