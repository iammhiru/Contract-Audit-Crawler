from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from audit_crawler import crawl_issue_list, crawl_issue_detail


def create_driver():
    options = webdriver.ChromeOptions()

    # options.add_argument("--headless")

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
        issues = crawl_multiple_pages(driver, total_pages=2, max_issue_per_page=10)

        print(f"\n✨ Crawl xong {len(issues)} issues.")
        for item in issues:
            print(item)

    except:
        print("Đã có lỗi xảy ra trong quá trình crawl.")
