#!/usr/bin/env python3
import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests


MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


def parse_time_to_iso(s: str) -> str | None:
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


def ensure_index_exists(es: str, index: str) -> None:
    r = requests.head(f"{es}/{index}", timeout=10)
    if r.status_code == 200:
        return
    if r.status_code != 404:
        raise RuntimeError(f"HEAD index failed: {r.status_code} {r.text}")

    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic": True,
            "properties": {
                # identifiers
                "source": {"type": "keyword"},
                "issue_id": {"type": "keyword"},
                "project": {"type": "keyword"},
                "full_report_url": {"type": "keyword"},

                # searchable text
                "title": {"type": "text"},
                "description": {"type": "text"},

                # facets
                "impact": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "categories": {"type": "keyword"},
                "tags": {"type": "keyword"},

                # stats
                "quality_rate": {"type": "float"},
                "quality_votes": {"type": "integer"},
                "rarity_rate": {"type": "float"},
                "rarity_votes": {"type": "integer"},

                # time
                "time": {"type": "keyword"},
                "time_iso": {"type": "date"},
            }
        }
    }

    cr = requests.put(f"{es}/{index}", json=mapping, timeout=20)
    if cr.status_code not in (200, 201):
        raise RuntimeError(f"Create index failed: {cr.status_code} {cr.text}")


def recreate_index(es: str, index: str) -> None:
    requests.delete(f"{es}/{index}", timeout=20)
    ensure_index_exists(es, index)


def load_issue_file(fp: str) -> Dict[str, Any]:
    with open(fp, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Issue json must be an object/dict")
    return data


def normalize_issue(doc: Dict[str, Any], source: str | None) -> Dict[str, Any]:

    out = dict(doc)

    if "source" not in out:
        out["source"] = source or "crawl_local"

    if "issue_id" not in out:
        out["issue_id"] = out.get("id")

    if "severity" not in out and out.get("impact") is not None:
        out["severity"] = out.get("impact")

    if "time_iso" not in out and isinstance(out.get("time"), str):
        iso = parse_time_to_iso(out["time"])
        if iso:
            out["time_iso"] = iso

    def _to_int(v):
        try:
            return int(v)
        except Exception:
            return v

    def _to_float(v):
        try:
            return float(v)
        except Exception:
            return v

    if "quality_votes" in out:
        out["quality_votes"] = _to_int(out["quality_votes"])
    if "rarity_votes" in out:
        out["rarity_votes"] = _to_int(out["rarity_votes"])
    if "quality_rate" in out:
        out["quality_rate"] = _to_float(out["quality_rate"])
    if "rarity_rate" in out:
        out["rarity_rate"] = _to_float(out["rarity_rate"])

    return out


def bulk_upsert(
    es: str,
    index: str,
    docs: List[Tuple[str, Dict[str, Any]]],
    chunk_size: int = 500,
    dry_run: bool = False,
) -> Tuple[int, int]:
    ok = 0
    fail = 0

    for i in range(0, len(docs), chunk_size):
        part = docs[i:i + chunk_size]
        if dry_run:
            print(f"[DRY-RUN] bulk upsert {len(part)} docs -> {index}")
            ok += len(part)
            continue

        lines: List[str] = []
        for doc_id, doc in part:
            lines.append(json.dumps({"update": {"_index": index, "_id": doc_id}}, ensure_ascii=False))
            lines.append(json.dumps({"doc": doc, "doc_as_upsert": True}, ensure_ascii=False))
        body = "\n".join(lines) + "\n"

        r = requests.post(
            f"{es}/_bulk",
            data=body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Bulk failed: {r.status_code} {r.text}")

        resp = r.json()
        if resp.get("errors"):
            for item in resp.get("items", []):
                u = item.get("update", {})
                status = u.get("status", 0)
                if 200 <= status < 300:
                    ok += 1
                else:
                    fail += 1
                    _id = u.get("_id")
                    err = u.get("error")
                    print(f"[FAIL] id={_id} status={status} error={err}", file=sys.stderr)
        else:
            ok += len(part)

    return ok, fail


def main() -> int:
    ap = argparse.ArgumentParser(description="Push locally crawled issue JSON files (1 issue per file) to Elasticsearch.")
    ap.add_argument("--root", default="crawl", help="Root folder chứa issues (default: crawl)")
    ap.add_argument("--pattern", default="**/*.json", help="Glob pattern under root (default: **/*.json)")
    ap.add_argument("--es", default=os.getenv("ES_HOST", "http://localhost:9200"), help="Elasticsearch base url")
    ap.add_argument("--index", default=os.getenv("ES_INDEX", "issues"), help="Target index name")
    ap.add_argument("--source", default=os.getenv("ISSUE_SOURCE", "crawl_local"), help="Value to set field 'source'")
    ap.add_argument("--recreate-index", action="store_true", help="Delete & create index before pushing")
    ap.add_argument("--dry-run", action="store_true", help="Parse/count only, do not write to ES")
    ap.add_argument("--chunk-size", type=int, default=500, help="Bulk chunk size (default 500)")
    ap.add_argument("--add-file-meta", action="store_true", help="Add _local_relpath for traceability")
    args = ap.parse_args()

    root_dir = os.path.abspath(args.root)
    if not os.path.isdir(root_dir):
        print(f"[FATAL] root folder does not exist: {root_dir}", file=sys.stderr)
        return 2

    files = sorted(glob.glob(os.path.join(root_dir, args.pattern), recursive=True))
    if not files:
        print(f"[INFO] No files matched under {root_dir} with pattern={args.pattern}")
        return 0

    es = args.es.rstrip("/")
    index = args.index

    if args.recreate_index and not args.dry_run:
        print(f"[INFO] Recreating index: {index}")
        recreate_index(es, index)
    elif not args.dry_run:
        ensure_index_exists(es, index)

    docs: List[Tuple[str, Dict[str, Any]]] = []
    skipped = 0

    for fp in files:
        try:
            raw = load_issue_file(fp)
            doc = normalize_issue(raw, source=args.source)

            if args.add_file_meta:
                doc["_local_relpath"] = os.path.relpath(fp, root_dir).replace("\\", "/")

            # ID: dùng field id (ví dụ "#64037") để upsert ổn định
            doc_id = str(doc.get("id") or doc.get("issue_id") or "").strip()
            if not doc_id:
                raise ValueError("Missing 'id' in issue json (cannot build ES _id).")

            docs.append((doc_id, doc))

        except Exception as e:
            skipped += 1
            print(f"[WARN] Skip file {fp}: {e}", file=sys.stderr)

    print(f"[INFO] Loaded {len(docs)} docs from {len(files)} files (skipped={skipped})")

    if not docs:
        return 0

    ok, fail = bulk_upsert(es, index, docs, chunk_size=args.chunk_size, dry_run=args.dry_run)
    print(f"[DONE] index={index} ok={ok} fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
