from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from audit_crawler import crawl_issue_list, crawl_issue_detail
from solodit_full_report_crawler import crawl_full_report
import json, os, re

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '', name)

def save_json(data: dict, title: str, folder="crawl"):
    os.makedirs(folder, exist_ok=True)

    title_clean = sanitize_filename(title)

    filename = f"{title_clean}.json"
    filepath = os.path.join(folder, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("Saved result to:", filepath)


def create_driver():
    options = webdriver.ChromeOptions()

    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        driver.maximize_window()
    except:
        driver.set_window_size(1920, 1080)

    return driver


def crawl_multiple_pages(driver, start_page=1, total_pages=3, max_issue_per_page=15):
    results = []
    for page in range(start_page, total_pages + start_page):
        data = crawl_issue_list(driver, page, max_issue_per_page)
        if not data:
            break
        results.extend(data)
    return results


def process_all_crawl_json(crawl_folder="crawl", output_folder="issues_output", repo_folder="repos"):
    os.makedirs(output_folder, exist_ok=True)

    all_url = set()
    for filename in os.listdir(crawl_folder):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(crawl_folder, filename)

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            url = data.get("full_report_url", "")

            if "solodit" not in url.lower():
                continue
            all_url.add(url)

    for url in all_url:
        print("Crawling full report for URL:", url)
        crawl_full_report(url, f"{output_folder}", f"{repo_folder}")


if __name__ == "__main__":
    # process_all_crawl_json()
    driver = create_driver()

    try:
        issues = crawl_multiple_pages(driver, start_page=200, total_pages=100, max_issue_per_page=10)

        for item in issues:
            print("==============================")
            print("Crawling detail for issue:", item["title"])
            result = crawl_issue_detail(driver, item["link"])
            save_json(result, item["title"])
    except Exception as e:
        print("Đã có lỗi xảy ra trong quá trình crawl. Chi tiết:", str(e))
