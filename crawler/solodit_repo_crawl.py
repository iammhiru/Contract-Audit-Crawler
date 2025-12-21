import os, hashlib, re
from solodit_full_report_crawler import process_markdown_file, save_parsed_result
from typing import Iterator, Tuple

REPORTS_ROOT = "/home/hieu/PycharmProjects/Project3/solodit_content/reports"
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
ISSUES_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "issues_output")

def compute_report_id(firm: str, rel_path: str) -> str:
    key = f"{firm}:{rel_path}".encode("utf-8")
    return hashlib.sha256(key).hexdigest()[:24]

def sanitize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r'[\\/*?:"<>|]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name

def iter_md_files(reports_root: str) -> Iterator[Tuple[str, str, str]]:
    reports_root = os.path.abspath(reports_root)

    for entry in os.scandir(reports_root):
        if not entry.is_dir():
            continue

        firm = entry.name
        if firm.startswith("."):
            continue

        firm_dir = entry.path
        for dirpath, _, filenames in os.walk(firm_dir):
            for fn in filenames:
                if not fn.lower().endswith(".md"):
                    continue
                if fn.lower() == "readme.md":
                    continue

                md_path = os.path.join(dirpath, fn)
                rel_path = os.path.relpath(md_path, reports_root)
                yield md_path, firm, rel_path


def parse_report_markdown(md_text: str) -> dict:
    import re
    gh_urls = re.findall(r'https?://github\.com/[^\s)>\"]+', md_text)
    return {
        "github_urls": list(dict.fromkeys(gh_urls)),
        "length": len(md_text),
    }

def main():
    for md_path, firm, rel_path in iter_md_files(REPORTS_ROOT):
        final_issue = process_markdown_file(md_path)

        report_filename = os.path.splitext(os.path.basename(md_path))[0]
        report_filename = sanitize_filename(report_filename)

        output_path = os.path.join(
            ISSUES_OUTPUT_DIR,
            f"{report_filename}.json"
        )

        save_parsed_result(final_issue, filepath=output_path)


if __name__ == "__main__":
    main()
