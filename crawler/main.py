import argparse
import time
import json, os, re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from audit_crawler import crawl_issue_list, crawl_issue_detail
from crawler.pashov_full_report_crawler import crawl_full_report_pashov
from solodit_full_report_crawler import crawl_full_report_solodit
from utils import save_to_json


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", required=True, help="Run timestamp")
    return parser.parse_args()


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '', name)


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver


def crawl_multiple_pages(driver, start_page=1, total_pages=3, max_issue_per_page=15):
    results = []
    for page in range(start_page, total_pages + start_page):
        data = crawl_issue_list(driver, page, max_issue_per_page)
        if not data:
            break
        results.extend(data)
    return results


if __name__ == "__main__":
    args = parse_args()
    timestamp = args.timestamp

    crawl_folder = f"crawl/{timestamp}"
    output_folder = f"issues_output/{timestamp}"
    repo_file = f"repos/{timestamp}.json"

    os.makedirs(crawl_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs("repos", exist_ok=True)

    driver = create_driver()

    try:
        issues = crawl_multiple_pages(driver, start_page=1, total_pages=50, max_issue_per_page=10)

        full_report_url_list = set()

        for item in issues:
            item["title"] = sanitize_filename(item["title"])
            print("Crawling issue:", item["title"])

            result = crawl_issue_detail(driver, item["link"])
            full_report_url_list.add(result["full_report_url"])

            save_to_json(
                result,
                folder=crawl_folder,
                title=f"{item['title']}.json"
            )

        for url in full_report_url_list:
            if "solodit" in url.lower():
                crawl_full_report_solodit(url, output_folder, repo_file)
            elif "pashov" in url.lower():
                crawl_full_report_pashov(url, output_folder, repo_file)

    except Exception as e:
        print("Crawl failed:", str(e))
    finally:
        driver.quit()
