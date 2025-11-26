import requests
import os
import re
import json
import yaml
from collections import defaultdict

def convert_github_to_raw(url: str) -> str:
    if "github.com" in url and "/blob/" in url:
        return url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")

    return url

def download_markdown(raw_url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(raw_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.text
        else:
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

def crawl_full_report(full_report_url: str):

    raw_url = convert_github_to_raw(full_report_url)
    print("➡Raw URL:", raw_url)

    md_text = download_markdown(raw_url)
    if not md_text:
        print("Failed to download md file.")
        return None

    print("Markdown downloaded successfully.")

    fname = full_report_url.split("/")[-1].replace(".md", "")
    save_markdown(md_text, fname)

    return md_text

def nested_dict():
    return defaultdict(nested_dict)


def parse_markdown(md_text: str):
    lines = md_text.split("\n")

    root = nested_dict()
    stack = []

    heading_pattern = re.compile(r"^(#{1,6})\s+(.*)")
    bold_key_pattern = re.compile(r"^\*\*(.+?)\*\*[:：]?\s*(.*)$")

    buffer_text = []
    in_code_block = False
    current_field = None

    def flush_buffer():
        nonlocal buffer_text, current_field

        if not buffer_text or not stack:
            buffer_text = []
            return

        parent = stack[-1][1]
        text_to_add = "\n".join(buffer_text).strip()
        buffer_text = []
        if not text_to_add:
            return

        if current_field:
            parent.setdefault(current_field, "")
            if parent[current_field]:
                parent[current_field] += "\n" + text_to_add
            else:
                parent[current_field] = text_to_add

        else:
            parent.setdefault("text", "")
            if parent["text"]:
                parent["text"] += "\n" + text_to_add
            else:
                parent["text"] = text_to_add

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("```"):
            if not in_code_block:
                in_code_block = True
            else:
                in_code_block = False
            buffer_text.append(line)
            continue

        if in_code_block:
            buffer_text.append(line)
            continue

        h = heading_pattern.match(line)
        if h:
            flush_buffer()
            current_field = None

            level = len(h.group(1))
            title = h.group(2).strip()

            while stack and stack[-1][0] >= level:
                stack.pop()

            parent = stack[-1][1] if stack else root
            parent[title] = {}
            stack.append((level, parent[title]))
            continue

        m = bold_key_pattern.match(line)
        if m and stack:
            flush_buffer()

            key, val = m.group(1).strip(), m.group(2).strip()
            parent = stack[-1][1]

            parent.setdefault(key, "")
            if val:
                if parent[key]:
                    parent[key] += "\n" + val
                else:
                    parent[key] = val

            current_field = key
            continue

        if stripped:
            buffer_text.append(line)

    flush_buffer()

    return root


def parse_auditors(md_text: str):
    lead = []
    assist = []

    # Lấy phần ở đầu trước khi gặp "---" hoặc "#"
    intro = md_text.split("\n---")[0].split("\n#")[0]

    current = None

    link_pattern = re.compile(r"\[(.+?)\]\((.+?)\)")

    for line in intro.split("\n"):
        line = line.strip()

        if not line:
            continue

        # Detect section
        if line.lower().startswith("**lead auditors**".lower()):
            current = "lead"
            continue

        if line.lower().startswith("**assisting auditors**".lower()):
            current = "assist"
            continue

        # Detect [Name](URL)
        m = link_pattern.match(line)
        if m:
            name = m.group(1).strip()
            url = m.group(2).strip()

            entry = {"name": name, "url": url}

            if current == "lead":
                lead.append(entry)
            elif current == "assist":
                assist.append(entry)

    return {
        "lead_auditors": lead,
        "assisting_auditors": assist
    }

def flatten_issue(issue: dict):
    flat = {
        "severity": issue["severity"],
        "title": issue["title"]
    }

    for key, val in issue["fields"].items():
        k = key.rstrip(":：").strip().lower().replace(" ", "_")
        flat[k] = val

    return flat

def extract_project_from_filename(filepath: str):
    filename = os.path.basename(filepath)

    m = re.match(r"^\d{4}-\d{2}-\d{2}-(.*)\.md$", filename)
    if m:
        return m.group(1)

    # fallback: bỏ .md
    return filename.replace(".md", "")

def unwrap_findings(parsed, wrapper_key="Findings"):
    if wrapper_key not in parsed or not isinstance(parsed[wrapper_key], dict):
        return parsed  # không có Findings thì giữ nguyên

    new_root = {}

    # 1) thêm tất cả children bên trong Findings
    for k, v in parsed[wrapper_key].items():
        new_root[k] = v

    # 2) thêm các key top-level khác (trừ Findings)
    for k, v in parsed.items():
        if k == wrapper_key:
            continue
        # nếu trùng key thì giữ cái từ Findings là chính
        if k not in new_root:
            new_root[k] = v

    return new_root

def extract_issues(parsed_root):
    issues = []

    def walk(node, parent_key=None):
        if not isinstance(node, dict):
            return

        for key, value in node.items():
            if not isinstance(value, dict):
                continue

            has_non_dict_child = any(
                not isinstance(child_val, dict) for child_val in value.values()
            )

            if has_non_dict_child and parent_key is not None:
                issues.append({
                    "severity": parent_key,
                    "title": key,
                    "fields": value
                })

            walk(value, key)

    walk(parsed_root)
    return issues

def process_markdown_file(filepath: str):
    with open(filepath, "r", encoding="utf8") as f:
        md_text = f.read()

    parsed = parse_markdown(md_text)
    normalized = unwrap_findings(parsed)
    issues = extract_issues(normalized)
    auditors = parse_auditors(md_text)
    project = extract_project_from_filename(filepath)

    final_issues = []
    for iss in issues:
        flat = flatten_issue(iss)
        flat["project"] = project
        flat["lead_auditors"] = auditors["lead_auditors"]
        flat["assisting_auditors"] = auditors["assisting_auditors"]
        final_issues.append(flat)

    return final_issues

def save_parsed_result(data, filepath: str):
    ext = filepath.lower().split(".")[-1]

    if ext == "json":
        with open(filepath, "w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    elif ext in ("yaml", "yml"):
        with open(filepath, "w", encoding="utf8") as f:
            yaml.dump(data, f, allow_unicode=True)

    elif ext == "txt":
        with open(filepath, "w", encoding="utf8") as f:
            f.write(str(data))

    else:
        raise ValueError("Unsupported file extension. Use json, yaml, yml, or txt.")

    print(f"Saved parsed output to {filepath}")