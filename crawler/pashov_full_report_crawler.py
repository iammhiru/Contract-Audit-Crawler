from collections import defaultdict
import os
import re
import json
import requests
import subprocess
import hashlib
from typing import Dict, List, Optional, Any

def _run(cmd: List[str]) -> None:
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\nSTDERR:\n{r.stderr}")

def _parse_owner_repo(repo_url: str) -> Optional[tuple]:
    m = re.match(r"^https?://github\.com/([^/]+)/([^/#?]+)", repo_url.strip())
    if not m:
        return None
    owner = m.group(1)
    repo = m.group(2).replace(".git", "")
    return owner, repo

def clone_repo_at_ref(repo_url: str, ref: str | None, out_dir: str) -> str:
    parsed = _parse_owner_repo(repo_url)
    if not parsed:
        raise ValueError(f"Invalid repo_url: {repo_url}")

    owner, repo = parsed
    os.makedirs(out_dir, exist_ok=True)

    dest = os.path.join(out_dir, f"{owner}__{repo}")

    if not os.path.exists(dest):
        _run(["git", "clone", "--no-tags", repo_url, dest])

    if ref:
        _run(["git", "-C", dest, "fetch", "--no-tags", "origin", ref])
        _run(["git", "-C", dest, "checkout", "--force", ref])

    return dest


def extract_repo_ref_hints(md_text: str) -> Dict[str, Dict[str, Optional[str]]]:
    repo_map: Dict[str, Dict[str, Optional[str]]] = {}

    repo_root_pat = re.compile(r'https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)')
    for owner, repo in repo_root_pat.findall(md_text):
        repo = repo.replace(".git", "")
        root = f"https://github.com/{owner}/{repo}"
        repo_map.setdefault(root, {"commit": None, "ref": None})

    commit_url_pat = re.compile(
        r'https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/commit/([0-9a-fA-F]{7,40})'
    )
    for owner, repo, sha in commit_url_pat.findall(md_text):
        repo = repo.replace(".git", "")
        root = f"https://github.com/{owner}/{repo}"
        repo_map.setdefault(root, {"commit": None, "ref": None})
        cur = repo_map[root]["commit"]
        if cur is None or len(sha) > len(cur):
            repo_map[root]["commit"] = sha

    ref_url_pat = re.compile(
        r'https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/(?:blob|tree)/([^/\s#?]+)'
    )
    for owner, repo, ref in ref_url_pat.findall(md_text):
        repo = repo.replace(".git", "")
        root = f"https://github.com/{owner}/{repo}"
        repo_map.setdefault(root, {"commit": None, "ref": None})
        if repo_map[root]["ref"] is None:
            repo_map[root]["ref"] = ref

    text_commit_pat = re.compile(r'(?i)\bcommit\b[^0-9a-fA-F]*([0-9a-fA-F]{7,40})')
    text_shas = text_commit_pat.findall(md_text)
    if text_shas and len(repo_map) == 1:
        root = next(iter(repo_map.keys()))
        best = max(text_shas, key=len)
        cur = repo_map[root]["commit"]
        if cur is None or len(best) > len(cur):
            repo_map[root]["commit"] = best

    return repo_map


def download_github_tarball(repo_url: str, ref: str, out_dir: str) -> str:
    parsed = _parse_owner_repo(repo_url)
    if not parsed:
        raise ValueError(f"Invalid repo_url: {repo_url}")

    owner, repo = parsed
    os.makedirs(out_dir, exist_ok=True)

    tar_url = f"https://codeload.github.com/{owner}/{repo}/tar.gz/{ref}"

    safe_ref = re.sub(r'[^A-Za-z0-9_.-]+', '_', ref)[:80]
    filename = f"{owner}__{repo}__{safe_ref}.tar.gz"
    path = os.path.join(out_dir, filename)

    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(tar_url, headers=headers, stream=True, timeout=60) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Tarball download failed ({r.status_code}): {tar_url}")
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return path


def shallow_clone_repo(repo_url: str, out_dir: str) -> str:
    parsed = _parse_owner_repo(repo_url)
    if not parsed:
        raise ValueError(f"Invalid repo_url: {repo_url}")

    owner, repo = parsed
    os.makedirs(out_dir, exist_ok=True)

    dest = os.path.join(out_dir, f"{owner}__{repo}")
    if os.path.exists(dest) and os.path.isdir(dest) and os.path.exists(os.path.join(dest, ".git")):
        _run(["git", "-C", dest, "fetch", "--depth", "1", "--no-tags", "origin"])
        _run(["git", "-C", dest, "reset", "--hard", "FETCH_HEAD"])
        return dest

    _run(["git", "clone", "--depth", "1", "--no-tags", repo_url, dest])
    return dest


def resolve_github_repo(repo_url: str, token: str | None = None) -> dict:
    parsed = _parse_owner_repo(repo_url)
    if not parsed:
        raise ValueError(f"Invalid repo_url: {repo_url}")

    owner, repo = parsed
    api_url = f"https://api.github.com/repos/{owner}/{repo}"

    headers = {"User-Agent": "audit-crawler"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    r = requests.get(api_url, headers=headers, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API failed ({r.status_code}): {api_url}")

    data = r.json()
    return {
        "repo_id": data["id"],
        "node_id": data.get("node_id"),
        "full_name": data["full_name"],
        "default_branch": data["default_branch"],
        "archived": data["archived"],
        "fork": data["fork"],
        "html_url": data["html_url"],
    }


def crawl_source_code_from_report(
    md_text: str,
    code_out_dir: str = "repos_code",
    github_token: str | None = None
) -> List[dict]:
    repo_hints = extract_repo_ref_hints(md_text)
    results: List[dict] = []

    for repo_url, hint in repo_hints.items():
        commit = hint.get("commit")
        ref = hint.get("ref")

        try:
            repo_meta = resolve_github_repo(repo_url, token=github_token)
            repo_id = repo_meta["repo_id"]
            repo_key = f"github_repo_id:{repo_id}"

            if commit:
                ref_used = commit
                method = "git_commit"
                artifact = clone_repo_at_ref(repo_url, commit, code_out_dir)
            elif ref:
                ref_used = ref
                method = "git_ref"
                artifact = clone_repo_at_ref(repo_url, ref, code_out_dir)
            else:
                ref_used = "default_branch"
                method = "git_default"
                artifact = clone_repo_at_ref(repo_url, None, code_out_dir)

            snapshot_id = hashlib.sha256(
                f"{repo_key}:{ref_used}".encode("utf-8")
            ).hexdigest()[:24]

            results.append({
                "repo_url": repo_url,
                "repo_id": repo_id,
                "repo_key": repo_key,
                "repo_full_name": repo_meta["full_name"],
                "snapshot_id": snapshot_id,
                "ref": ref_used,
                "method": method,
                "artifact_path": artifact,
                "status": "OK",
            })

            print(
                f"[CODE OK] {method} "
                f"{repo_meta['full_name']}@{ref_used} "
                f"snapshot_id={snapshot_id}"
            )

        except Exception as e:
            results.append({
                "repo_url": repo_url,
                "repo_key": None,
                "snapshot_id": None,
                "ref": commit or ref,
                "method": "auto",
                "artifact_path": None,
                "status": "ERROR",
                "error": str(e),
            })
            print(f"[CODE ERROR] {repo_url} -> {e}")

    return results



def convert_github_to_raw(url: str) -> str:
    if "github.com" in url and "/blob/" in url:
        return url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    return url


def download_markdown(raw_url: str) -> str | None:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(raw_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.text
        print(f"Request failed ({resp.status_code}): {raw_url}")
        return None
    except Exception as e:
        print(f"Error while downloading {raw_url}: {e}")
        return None


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name)


def save_markdown(md_text: str, filename: str, folder="full_report"):
    os.makedirs(folder, exist_ok=True)
    filename = sanitize_filename(filename) + ".md"
    filepath = os.path.join(folder, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(md_text)
    print(f"Saved markdown: {filepath}")
    return filepath


def save_parsed_result(data, filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    ext = filepath.lower().split(".")[-1]

    if ext == "json":
        with open(filepath, "w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif ext == "txt":
        with open(filepath, "w", encoding="utf8") as f:
            f.write(str(data))
    else:
        raise ValueError("Unsupported file extension. Use json or txt.")

    print(f"Saved parsed output to {filepath}")

PASHOV_ISSUE_H1_RE = re.compile(
    r"^#\s+\[(?P<code>[A-Za-z]+-\d+)\]\s+(?P<title>.+?)\s*$",
    re.MULTILINE,
)

H2_SECTION_RE = re.compile(r"^##\s+(?P<name>.+?)\s*$", re.MULTILINE)

IMPACT_RE = re.compile(r"(?im)^\*\*Impact:\*\*\s*(?P<impact>.+?)\s*$")
LIKELIHOOD_RE = re.compile(r"(?im)^\*\*Likelihood:\*\*\s*(?P<likelihood>.+?)\s*$")

LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
CODE_FENCE_RE = re.compile(r"```[\s\S]*?```", re.MULTILINE)


def _normalize_key(s: str) -> str:
    return re.sub(r"\W+", "_", (s or "").strip().lower()).strip("_")


def _extract_links(text: str) -> List[Dict[str, str]]:
    return [{"text": m.group(1).strip(), "url": m.group(2).strip()} for m in LINK_RE.finditer(text or "")]


def _extract_code_blocks(text: str) -> List[str]:
    return [m.group(0) for m in CODE_FENCE_RE.finditer(text or "")]


def _split_h2_sections(block: str) -> Dict[str, str]:
    block = (block or "").strip()
    matches = list(H2_SECTION_RE.finditer(block))
    if not matches:
        return {"body": block}

    sections: Dict[str, str] = {}
    for i, m in enumerate(matches):
        name = _normalize_key(m.group("name"))
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
        sections[name] = block[start:end].strip()
    return sections


def parse_pashov_report_metadata(md_text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "review_commit": None,
        "fixes_review_commit": None,
        "scope_contracts": [],
        "repo_full_name_hint": None,
    }

    m = re.search(r"(?im)^_review commit hash_\s*-\s*\[([0-9a-f]{7,40})", md_text)
    if m:
        out["review_commit"] = m.group(1)

    m = re.search(r"(?im)^_fixes review commit hash_\s*-\s*\[([0-9a-f]{7,40})", md_text)
    if m:
        out["fixes_review_commit"] = m.group(1)

    scope_match = re.search(r"(?is)###\s+Scope\s*(?P<body>.*?)(?:\n#\s+Findings|\Z)", md_text)
    if scope_match:
        scope_body = scope_match.group("body")
        out["scope_contracts"] = re.findall(r"(?m)^\-\s+`([^`]+)`\s*$", scope_body)

    m = re.search(r"(?i)\*\*(?P<full>[^*\s]+/[^*\s]+)\*\*\s+repository", md_text)
    if m:
        out["repo_full_name_hint"] = m.group("full").strip()

    return out

PASHOV_REVIEW_COMMIT_RE = re.compile(
    r"(?im)^_review commit hash_\s*-\s*\[(?P<sha>[0-9a-f]{7,40})\]\((?P<url>https?://github\.com/[^)]+)\)\s*$"
)
PASHOV_FIXES_COMMIT_RE = re.compile(
    r"(?im)^_fixes review commit hash_\s*-\s*\[(?P<sha>[0-9a-f]{7,40})\]\((?P<url>https?://github\.com/[^)]+)\)\s*$"
)

def parse_pashov_repo_and_commits(md_text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"repo_url": None, "review_commit": None, "fixes_review_commit": None}

    m = re.search(
        r"(?is)^\s*#{1,6}\s*Security Assessment Summary\s*$\s*(?P<body>.*?)(?:^\s*#{1,6}\s+|\Z)",
        md_text,
        flags=re.MULTILINE,
    )
    summary = m.group("body") if m else md_text

    summary_norm = summary
    summary_norm = re.sub(r"(?i)<br\s*/?>", "\n", summary_norm)
    summary_norm = summary_norm.replace("&nbsp;", " ")
    summary_norm = re.sub(r"[ \t]+\n", "\n", summary_norm)
    summary_norm = re.sub(r"\n{3,}", "\n\n", summary_norm).strip()

    def _find_repo_hint(text: str) -> Optional[str]:
        mh = re.search(r"\(\s*([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\s*\)", text)
        return mh.group(1) if mh else None

    def _find_sha_and_url(window: str) -> Optional[Dict[str, Optional[str]]]:
        ms = re.search(r"(?i)\b([0-9a-f]{7,40})\b", window)
        if not ms:
            return None
        sha = ms.group(1)

        mu = re.search(rf"(?i)(https?://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/tree/{sha})", window)
        if mu:
            return {"sha": sha, "url": mu.group(1)}

        mu2 = re.search(r"(?i)(https?://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+[^\s)\]]*)", window)
        if mu2:
            return {"sha": sha, "url": mu2.group(1)}

        return {"sha": sha, "url": None}

    def _extract_kind(kind: str) -> Optional[Dict[str, Any]]:
        if kind == "review":
            key_pat = r"(review\s+commit\s+hash)"
        else:
            key_pat = r"(fixes\s+review\s+commit\s+hash)"

        mk = re.search(rf"(?is){key_pat}\s*[:：]?", summary_norm)
        if not mk:
            return None

        start = mk.end()
        window = summary_norm[start:start + 600]
        data = _find_sha_and_url(window)
        if not data:
            return None

        repo_hint = _find_repo_hint(window)
        return {"sha": data["sha"], "url": data.get("url"), "repo_full_name_hint": repo_hint}

    out["review_commit"] = _extract_kind("review")
    out["fixes_review_commit"] = _extract_kind("fixes")

    def _repo_url_from_github_url(u: str) -> Optional[str]:
        mm = re.match(r"^(https?://github\.com/[^/]+/[^/]+)", (u or "").strip())
        return mm.group(1).replace(".git", "") if mm else None

    repo_url = None
    for obj in (out["review_commit"], out["fixes_review_commit"]):
        if obj and obj.get("url"):
            repo_url = _repo_url_from_github_url(obj["url"])
            if repo_url:
                break

    if not repo_url:
        hint = None
        for obj in (out["review_commit"], out["fixes_review_commit"]):
            if obj and obj.get("repo_full_name_hint"):
                hint = obj["repo_full_name_hint"]
                break
        if hint:
            repo_url = f"https://github.com/{hint}"

    out["repo_url"] = repo_url
    return out

def crawl_pashov_review_code_snapshots(
    md_text: str,
    code_out_dir: str = "repos_code",
    github_token: str | None = None
) -> List[dict]:
    info = parse_pashov_repo_and_commits(md_text)
    repo_url = info.get("repo_url")

    results: List[dict] = []

    if not repo_url:
        results.append({
            "repo_url": None,
            "repo_id": None,
            "repo_key": None,
            "repo_full_name": None,
            "snapshot_id": None,
            "ref": None,
            "method": "pashov_review_snapshot",
            "artifact_path": None,
            "status": "ERROR",
            "error": "Cannot determine audited repo_url from Pashov report (missing tree links).",
            "pashov_snapshot_kind": None,
            "pashov_info": info,
        })
        return results

    try:
        repo_meta = resolve_github_repo(repo_url, token=github_token)
        repo_id = repo_meta["repo_id"]
        repo_key = f"github_repo_id:{repo_id}"
        repo_full_name = repo_meta["full_name"]
    except Exception as e:
        results.append({
            "repo_url": repo_url,
            "repo_id": None,
            "repo_key": None,
            "repo_full_name": None,
            "snapshot_id": None,
            "ref": None,
            "method": "resolve_repo",
            "artifact_path": None,
            "status": "ERROR",
            "error": str(e),
            "pashov_snapshot_kind": None,
            "pashov_info": info,
        })
        return results

    def _download_one(kind: str, sha: str, url_from_report: str | None):
        try:
            repo_out_dir = os.path.join(code_out_dir, str(repo_id))
            artifact = clone_repo_at_ref(repo_url, sha, repo_out_dir)

            snapshot_id = hashlib.sha256(
                f"{repo_key}:{sha}".encode("utf-8")
            ).hexdigest()[:24]

            results.append({
                "repo_url": repo_url,
                "repo_id": repo_id,
                "repo_key": repo_key,
                "repo_full_name": repo_full_name,

                "snapshot_id": snapshot_id,
                "ref": sha,
                "method": f"tarball_{kind}",
                "artifact_path": artifact,
                "status": "OK",

                "pashov_snapshot_kind": kind,
                "pashov_commit_url": url_from_report,
            })
        except Exception as e:
            results.append({
                "repo_url": repo_url,
                "repo_id": repo_id,
                "repo_key": repo_key,
                "repo_full_name": repo_full_name,

                "snapshot_id": None,
                "ref": sha,
                "method": f"tarball_{kind}",
                "artifact_path": None,
                "status": "ERROR",
                "error": str(e),

                "pashov_snapshot_kind": kind,
                "pashov_commit_url": url_from_report,
            })

    if info.get("review_commit") and info["review_commit"].get("sha"):
        _download_one("review", info["review_commit"]["sha"], info["review_commit"].get("url"))

    if info.get("fixes_review_commit") and info["fixes_review_commit"].get("sha"):
        _download_one("fixes", info["fixes_review_commit"]["sha"], info["fixes_review_commit"].get("url"))

    if not (info.get("review_commit") or info.get("fixes_review_commit")):
        results.append({
            "repo_url": repo_url,
            "repo_id": repo_id,
            "repo_key": repo_key,
            "repo_full_name": repo_full_name,
            "snapshot_id": None,
            "ref": None,
            "method": "pashov_review_snapshot",
            "artifact_path": None,
            "status": "ERROR",
            "error": "No review_commit / fixes_review_commit found in Pashov report.",
            "pashov_snapshot_kind": None,
            "pashov_info": info,
        })

    return results


def parse_pashov_issues_from_markdown(md_text: str) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []

    headers = list(PASHOV_ISSUE_H1_RE.finditer(md_text))
    for i, h in enumerate(headers):
        code = h.group("code").strip()
        title = h.group("title").strip()

        start = h.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(md_text)
        body = md_text[start:end].strip()

        sections = _split_h2_sections(body)

        impact = None
        likelihood = None
        sev_text = sections.get("severity")
        if sev_text:
            m1 = IMPACT_RE.search(sev_text)
            m2 = LIKELIHOOD_RE.search(sev_text)
            impact = m1.group("impact").strip() if m1 else None
            likelihood = m2.group("likelihood").strip() if m2 else None

        issues.append({
            "issue_code": code,
            "issue_prefix": code.split("-")[0].upper(),
            "title": title,

            "impact": impact,
            "likelihood": likelihood,

            "sections": sections,

            "description": sections.get("description"),
            "recommendations": sections.get("recommendations"),

            "links": _extract_links(body),
            "code_blocks": _extract_code_blocks(body),

            "raw": body,
        })

    return issues


def process_pashov_markdown_file(filepath: str) -> Dict[str, Any]:
    with open(filepath, "r", encoding="utf8") as f:
        md_text = f.read()

    report_meta = parse_pashov_report_metadata(md_text)
    issues = parse_pashov_issues_from_markdown(md_text)

    project = os.path.basename(filepath).replace(".md", "")

    return {
        "project": project,
        "report_meta": report_meta,
        "issues": issues,
    }

def crawl_full_report_pashov(
    full_report_url: str,
    output_filepath: str,
    repo_filepath: str,
    github_token: str | None = None,
    reports_folder: str = "full_report",
    repos_code_folder: str = "repos_code",
):
    raw_url = convert_github_to_raw(full_report_url)
    print("➡Raw URL:", raw_url)

    md_text = download_markdown(raw_url)
    if not md_text:
        print("Failed to download md file.")
        return None

    print("Markdown downloaded successfully.")

    fname = full_report_url.split("/")[-1].replace(".md", "")
    filepath = save_markdown(md_text, fname, folder=reports_folder)

    code_results = crawl_pashov_review_code_snapshots(
        md_text,
        code_out_dir=repos_code_folder,
        github_token=github_token
    )
    ok_artifacts = [r for r in code_results if r.get("status") == "OK"]

    parsed = process_pashov_markdown_file(filepath)
    final_issues = parsed["issues"]

    for iss in final_issues:
        iss["repo_code_artifacts"] = ok_artifacts
        iss["project"] = parsed["project"]
        iss["report_meta"] = parsed["report_meta"]
        iss["source"] = "pashov"
        iss["full_report_url"] = full_report_url

    save_parsed_result(final_issues, f"{output_filepath}/{fname}.json")
    save_parsed_result(code_results, f"{repo_filepath}/{fname}.code.json")

    return final_issues
