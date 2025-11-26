from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from audit_crawler import crawl_issue_list, crawl_issue_detail
import json, os, re
from datetime import datetime

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '', name)

def save_json(data: dict, title: str, folder="crawl"):
    os.makedirs(folder, exist_ok=True)

    title_clean = sanitize_filename(title)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    filename = f"{title_clean}_{timestamp}.json"
    filepath = os.path.join(folder, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("Saved result to:", filepath)


def create_driver():
    options = webdriver.ChromeOptions()

    options.add_argument("--headless")
    # options.add_argument("--user-data-dir=/home/hieu/.config/google-chrome")
    # options.add_argument("--profile-directory=SoloditProfile")
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


def crawl_multiple_pages(driver, total_pages=3, max_issue_per_page=15):
    results = []
    for page in range(1, total_pages + 1):
        data = crawl_issue_list(driver, page, max_issue_per_page)
        if not data:
            break
        results.extend(data)
    return results


if __name__ == "__main__":
    driver = create_driver()

    try:
        issues = crawl_multiple_pages(driver, total_pages=50, max_issue_per_page=10)

        for item in issues:
            print("==============================")
            print("Crawling detail for issue:", item["title"])
            result = crawl_issue_detail(driver, item["link"])
            save_json(result, item["title"])
    except Exception as e:
        print("Đã có lỗi xảy ra trong quá trình crawl. Chi tiết:", str(e))
