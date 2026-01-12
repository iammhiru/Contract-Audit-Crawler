#!/usr/bin/env python3
import argparse
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests


# -----------------------------
# Helpers
# -----------------------------

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


def parse_time_to_iso(s: str) -> Optional[str]:
    """
    Input example: "Dec 17, 2025" -> "2025-12-17T00:00:00Z"
    Optional - nếu parse fail thì trả None.
    """
    if not s:
        return None
    s = s.strip()
    m = re.match(r"^(?P<mon>[A-Za-z]{3})\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})$", s)
    if not m:
        return None
    mon = MONTHS.get(m.group("mon").lower())
    if not mon:
        return None
    day = int(m.group("day"))
    year = int(m.group("year"))
    try:
        dt = datetime(year, mon, day, 0, 0, 0)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def iter_json_files(root: str, pattern: str = "**/*.json") -> List[str]:
    root = os.path.abspath(root)
    return sorted(glob.glob(os.path.join(root, pattern), recursive=True))


def load_docs_from_file(path: str) -> List[Dict[str, Any]]:
    """
    - Nếu file là list[dict] => coi là mảng issues (full_report output)
    - Nếu file là dict => coi là 1 issue
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def normalize_issue(doc: Dict[str, Any], source: str, source_file: str) -> Dict[str, Any]:
    """
    Không ép schema/mapping — chỉ enrich tối thiểu:
    - source (nếu chưa có)
    - _source_file (để trace)
    - time_iso (nếu có field 'time' kiểu "Dec 17, 2025")
    """
    out = dict(doc)

    if "source" not in out:
        out["source"] = source

    out["_source_file"] = source_file

    # optional enrich
    if "time_iso" not in out and isinstance(out.get("time"), str):
        iso = parse_time_to_iso(out["time"])
        if iso:
            out["time_iso"] = iso

    # Convert numeric-looking fields safely (optional)
    for k in ("quality_votes", "rarity_votes"):
        if k in out and isinstance(out[k], str) and out[k].isdigit():
            out[k] = int(out[k])

    for k in ("quality_rate", "rarity_rate"):
        if k in out and isinstance(out[k], str):
            try:
                out[k] = float(out[k])
            except Exception:
                pass

    return out


def stable_doc_id(doc: Dict[str, Any], source_file: str) -> str:
    """
    _id ổn định:
    1) doc["id"] (vd "#64037") nếu có
    2) doc["issue_code"] + doc["project"] (+title) nếu có
    3) hash toàn bộ doc
    """
    if isinstance(doc.get("id"), str) and doc["id"].strip():
        return doc["id"].strip()

    issue_code = str(doc.get("issue_code") or "").strip()
    project = str(doc.get("project") or "").strip()
    title = str(doc.get("title") or "").strip()

    if issue_code and project:
        base = f"{project}::{issue_code}::{title}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]

    normalized = json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256((source_file + "::" + normalized).encode("utf-8")).hexdigest()[:32]


# -----------------------------
# Elasticsearch
# -----------------------------

def es_request(
    method: str,
    url: str,
    *,
    auth: Optional[Tuple[str, str]] = None,
    json_body: Optional[dict] = None,
    data: Optional[Union[str, bytes]] = None,
    timeout: int = 30,
) -> requests.Response:
    headers: Dict[str, str] = {}
    if data is not None:
        headers["Content-Type"] = "application/x-ndjson"
    return requests.request(
        method,
        url,
        auth=auth,
        json=json_body,
        data=data,
        headers=headers,
        timeout=timeout,
    )


def ensure_index(es: str, index: str, auth: Optional[Tuple[str, str]]) -> None:
    """
    Không mapping: chỉ tạo index empty (dynamic mapping)
    """
    r = es_request("HEAD", f"{es}/{index}", auth=auth, timeout=10)
    if r.status_code == 200:
        return
    if r.status_code != 404:
        raise RuntimeError(f"HEAD index failed: {r.status_code} {r.text}")

    r = es_request("PUT", f"{es}/{index}", auth=auth, json_body={}, timeout=20)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Create index failed: {r.status_code} {r.text}")


def bulk_index(
    es: str,
    index: str,
    docs: List[Tuple[str, Dict[str, Any]]],
    auth: Optional[Tuple[str, str]],
    batch_size: int = 500,
) -> Tuple[int, int]:
    """
    Bulk INDEX (overwrite by _id) => chạy lại không dup.
    Returns: (ok, fail)
    """
    ok = 0
    fail = 0

    for i in range(0, len(docs), batch_size):
        chunk = docs[i:i + batch_size]

        lines: List[str] = []
        for doc_id, doc in chunk:
            # IMPORTANT: _id chỉ nằm trong action line, KHÔNG nằm trong doc body
            lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}, ensure_ascii=False))
            lines.append(json.dumps(doc, ensure_ascii=False))

        payload = "\n".join(lines)

        # loại NULL byte (ES hay choke)
        payload = payload.replace("\x00", "")

        # đảm bảo kết thúc bằng newline
        if not payload.endswith("\n"):
            payload += "\n"

        r = es_request(
            "POST",
            f"{es}/_bulk",
            auth=auth,
            data=payload.encode("utf-8"),
            timeout=120,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Bulk failed: {r.status_code} {r.text}")

        resp = r.json()
        items = resp.get("items", [])
        if resp.get("errors"):
            for it in items:
                res = it.get("index") or {}
                status = res.get("status", 0)
                if 200 <= status < 300:
                    ok += 1
                else:
                    fail += 1
                    _id = res.get("_id")
                    err = res.get("error")
                    print(f"[FAIL] id={_id} status={status} error={err}", file=sys.stderr)
        else:
            ok += len(chunk)

    return ok, fail


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Push full_report issues JSON (each file can be array of issues) into Elasticsearch index."
    )
    ap.add_argument("--root", required=True, help="Root folder chứa JSON issues (recursive).")
    ap.add_argument("--pattern", default="**/*.json", help="Glob under root (default **/*.json).")
    ap.add_argument("--es", default=os.getenv("ES_URL", "http://localhost:9200"), help="Elasticsearch base URL.")
    ap.add_argument("--index", default=os.getenv("ES_INDEX", "issue_full_report"), help="Index name.")
    ap.add_argument("--source", default=os.getenv("ISSUE_SOURCE", "full_report"),
                    help="Value for field 'source' if missing.")
    ap.add_argument("--user", default=os.getenv("ES_USER"), help="Basic auth user (optional).")
    ap.add_argument("--password", default=os.getenv("ES_PASSWORD"), help="Basic auth password (optional).")
    ap.add_argument("--batch-size", type=int, default=500, help="Bulk batch size.")
    ap.add_argument("--dry-run", action="store_true", help="Scan/count only, no push.")
    ap.add_argument("--add-file-meta", action="store_true",
                    help="Add _local_relpath for traceability.")
    ap.add_argument(
        "--id-field",
        default="doc_id",
        help="Nếu muốn lưu id vào document để search/debug, lưu dưới field này (mặc định doc_id). "
             "Để tắt hoàn toàn: --id-field ''"
    )
    args = ap.parse_args()

    es = args.es.rstrip("/")
    auth = (args.user, args.password) if args.user and args.password else None

    ensure_index(es, args.index, auth)

    files = iter_json_files(args.root, pattern=args.pattern)
    if not files:
        print(f"[INFO] No JSON files found under {args.root} with pattern={args.pattern}")
        return 0

    all_docs: List[Tuple[str, Dict[str, Any]]] = []
    skipped = 0

    for fp in files:
        try:
            docs = load_docs_from_file(fp)
            for d in docs:
                if not isinstance(d, dict):
                    continue
                doc = normalize_issue(d, source=args.source, source_file=fp)

                if args.add_file_meta:
                    doc["_local_relpath"] = os.path.relpath(fp, os.path.abspath(args.root)).replace("\\", "/")

                doc_id = stable_doc_id(doc, fp)

                # IMPORTANT: KHÔNG ĐƯỢC set doc["_id"] (ES metadata field)
                if args.id_field:
                    doc[args.id_field] = doc_id

                all_docs.append((doc_id, doc))
        except Exception as e:
            skipped += 1
            print(f"[WARN] Skip file {fp}: {e}", file=sys.stderr)

    print(f"[SCAN] files={len(files)} docs={len(all_docs)} skipped_files={skipped} root={args.root}")

    if args.dry_run:
        print("[DRY-RUN] exit without pushing.")
        return 0

    ok, fail = bulk_index(es, args.index, all_docs, auth, batch_size=args.batch_size)
    print(f"[DONE] index={args.index} ok={ok} fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
