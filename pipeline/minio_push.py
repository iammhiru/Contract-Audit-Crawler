#!/usr/bin/env python3
import argparse
import fnmatch
import os
import sys
from typing import Iterable, List, Tuple

from minio import Minio
from minio.error import S3Error


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


def iter_files(root_dir: str, patterns: List[str]) -> Iterable[str]:
    root_dir = os.path.abspath(root_dir)
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            if any(fnmatch.fnmatch(name, pat) for pat in patterns):
                yield os.path.join(dirpath, name)


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        raise RuntimeError(
            f"Bucket '{bucket}' does not exist. Create it first (e.g. via mc), or change --bucket."
        )


def normalize_object_key(rel_path: str, prefix: str) -> str:
    rel_path = rel_path.replace("\\", "/").lstrip("/")
    prefix = (prefix or "").strip().strip("/")
    if prefix:
        return f"{prefix}/{rel_path}"
    return rel_path


def object_exists(client: Minio, bucket: str, key: str) -> bool:
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject", "NotFound"):
            return False
        if getattr(e, "response", None) is not None and getattr(e.response, "status", None) == 404:
            return False
        raise


def upload_one(
    client: Minio,
    bucket: str,
    local_path: str,
    object_key: str,
    content_type: str,
) -> Tuple[bool, str]:
    try:
        stat = client.fput_object(
            bucket_name=bucket,
            object_name=object_key,
            file_path=local_path,
            content_type=content_type,
        )
        size = os.path.getsize(local_path)
        return True, f"OK etag={stat.etag} size={size}"
    except Exception as e:
        return False, f"ERROR {e}"


def guess_content_type(filename: str) -> str:
    if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
        return "application/gzip"
    if filename.endswith(".zip"):
        return "application/zip"
    return "application/octet-stream"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Upload repo code artifacts (e.g. repos_code/<repo_id>/...) to MinIO bucket."
    )
    ap.add_argument("--root", default="repos_code", help="Local root folder to scan (default: repos_code)")
    ap.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "repos"), help="MinIO bucket (default: env MINIO_BUCKET or 'repos')")
    ap.add_argument("--prefix", default="", help="Optional object key prefix inside bucket (e.g. 'repos_code'). Default empty.")
    ap.add_argument(
        "--pattern",
        action="append",
        default=["*.tar.gz"],
        help="Filename glob to include. Repeatable. Default: *.tar.gz",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print actions without uploading.")
    ap.add_argument("--skip-existing", action="store_true", help="Skip upload if object key already exists.")
    ap.add_argument("--delete-local", action="store_true", help="Delete local file after successful upload.")
    args = ap.parse_args()

    root_dir = os.path.abspath(args.root)
    if not os.path.isdir(root_dir):
        print(f"[FATAL] root folder does not exist: {root_dir}", file=sys.stderr)
        return 2

    client = make_minio_client()
    try:
        ensure_bucket(client, args.bucket)
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 2

    files = sorted(iter_files(root_dir, args.pattern))
    if not files:
        print(f"[INFO] No files matched under {root_dir} with patterns={args.pattern}")
        return 0

    total = len(files)
    ok = 0
    skipped = 0
    failed = 0

    print(f"[INFO] Found {total} file(s) under {root_dir}")
    print(f"[INFO] Target: bucket='{args.bucket}', prefix='{args.prefix or ''}'")
    print(f"[INFO] Options: dry_run={args.dry_run}, skip_existing={args.skip_existing}, delete_local={args.delete_local}")

    for i, local_path in enumerate(files, 1):
        rel_path = os.path.relpath(local_path, root_dir)
        object_key = normalize_object_key(rel_path, args.prefix)
        content_type = guess_content_type(local_path)

        action_prefix = f"[{i}/{total}]"
        if args.skip_existing and not args.dry_run:
            try:
                if object_exists(client, args.bucket, object_key):
                    print(f"{action_prefix} SKIP exists: {object_key}")
                    skipped += 1
                    continue
            except Exception as e:
                print(f"{action_prefix} WARN stat failed for {object_key}: {e} (will try upload)")

        if args.dry_run:
            print(f"{action_prefix} DRY-RUN upload: {local_path} -> s3://{args.bucket}/{object_key}")
            continue

        ok_flag, msg = upload_one(client, args.bucket, local_path, object_key, content_type)
        if ok_flag:
            print(f"{action_prefix} UPLOADED s3://{args.bucket}/{object_key} | {msg}")
            ok += 1
            if args.delete_local:
                try:
                    os.remove(local_path)
                    print(f"{action_prefix} DELETED local: {local_path}")
                except OSError as e:
                    print(f"{action_prefix} WARN cannot delete local file: {e}")
        else:
            print(f"{action_prefix} FAILED s3://{args.bucket}/{object_key} | {msg}", file=sys.stderr)
            failed += 1

    print(f"[DONE] ok={ok} skipped={skipped} failed={failed} total={total}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
