#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Iterable

from minio import Minio
from minio.error import S3Error


# =========================
# MinIO client
# =========================
def make_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes", "y")

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


# =========================
# Ignore rules (repo-friendly)
# =========================
IGNORE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "venv",
    ".venv",
}

IGNORE_FILES = {
    ".DS_Store",
}


# =========================
# Walk folder recursively
# =========================
def iter_all_files(root_dir: str) -> Iterable[str]:
    root_dir = os.path.abspath(root_dir)

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # filter ignored dirs IN-PLACE (important)
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]

        for name in filenames:
            if name in IGNORE_FILES:
                continue
            yield os.path.join(dirpath, name)


# =========================
# Helpers
# =========================
def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        raise RuntimeError(
            f"Bucket '{bucket}' does not exist. Create it first (e.g. via mc)."
        )


def normalize_object_key(rel_path: str, prefix: str) -> str:
    rel_path = rel_path.replace("\\", "/").lstrip("/")
    prefix = (prefix or "").strip().strip("/")
    return f"{prefix}/{rel_path}" if prefix else rel_path


def object_exists(client: Minio, bucket: str, key: str) -> bool:
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject", "NotFound"):
            return False
        if getattr(e, "response", None) and getattr(e.response, "status", None) == 404:
            return False
        raise


# =========================
# Upload logic
# =========================
def upload_folder(
    client: Minio,
    bucket: str,
    local_folder: str,
    prefix: str,
    dry_run: bool = False,
    skip_existing: bool = False,
) -> int:
    local_folder = os.path.abspath(local_folder)

    if not os.path.isdir(local_folder):
        raise RuntimeError(f"Folder does not exist: {local_folder}")

    files = sorted(iter_all_files(local_folder))
    if not files:
        print(f"[INFO] Folder is empty (after ignore rules): {local_folder}")
        return 0

    total = len(files)
    ok = skipped = failed = 0

    print(f"[INFO] Uploading folder: {local_folder}")
    print(f"[INFO] Files count: {total}")
    print(f"[INFO] Target: s3://{bucket}/{prefix or ''}")
    print(f"[INFO] Options: dry_run={dry_run}, skip_existing={skip_existing}")

    for i, local_path in enumerate(files, 1):
        rel_path = os.path.relpath(local_path, local_folder)
        object_key = normalize_object_key(rel_path, prefix)

        tag = f"[{i}/{total}]"

        if skip_existing and not dry_run:
            try:
                if object_exists(client, bucket, object_key):
                    print(f"{tag} SKIP exists: {object_key}")
                    skipped += 1
                    continue
            except Exception as e:
                print(f"{tag} WARN stat failed for {object_key}: {e}")

        if dry_run:
            print(f"{tag} DRY-RUN upload: {local_path} -> s3://{bucket}/{object_key}")
            continue

        try:
            client.fput_object(
                bucket_name=bucket,
                object_name=object_key,
                file_path=local_path,
            )
            size = os.path.getsize(local_path)
            print(f"{tag} UPLOADED {object_key} ({size} bytes)")
            ok += 1
        except Exception as e:
            print(f"{tag} FAILED {object_key}: {e}", file=sys.stderr)
            failed += 1

    print(f"[DONE] ok={ok} skipped={skipped} failed={failed} total={total}")
    return 0 if failed == 0 else 1


# =========================
# CLI
# =========================
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Upload a whole local folder (recursively) to MinIO, preserving structure."
    )
    ap.add_argument(
        "folder",
        help="Local folder to upload (e.g. repos_code/12345/snapshot_xxx)",
    )
    ap.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "repos"),
        help="MinIO bucket name (default: env MINIO_BUCKET or 'repos')",
    )
    ap.add_argument(
        "--prefix",
        default="",
        help="Object key prefix inside bucket (e.g. github_repo_id/123/snapshot)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Show actions without uploading.")
    ap.add_argument("--skip-existing", action="store_true", help="Skip objects that already exist.")

    args = ap.parse_args()

    client = make_minio_client()
    try:
        ensure_bucket(client, args.bucket)
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 2

    try:
        return upload_folder(
            client=client,
            bucket=args.bucket,
            local_folder=args.folder,
            prefix=args.prefix,
            dry_run=args.dry_run,
            skip_existing=args.skip_existing,
        )
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
