from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv

def crawl_solodit_list(url, max_items=100):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    # optional: options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get(url)
    # chờ phần danh sách lỗi load (ví dụ: chờ một phần tử có class “finding-row” xuất hiện)
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^='/issues/']"))  # link tới từng issue
    )

    time.sleep(2)  # có thể tăng nếu dữ liệu load chậm

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Tìm tất cả các dòng lỗi. Ví dụ selector: mỗi lỗi là thẻ <a href="/issues/..."> nằm trong <div class="finding-row">
    items = soup.select("a[href^='/issues/']")

    data = []
    count = 0
    for a in items:
        if count >= max_items:
            break
        link = a.get("href")
        full_link = "https://solodit.cyfrin.io" + link
        title = a.get_text(strip=True)

        # Lấy thêm thông tin severity hoặc impact nếu có
        parent = a.find_parent("div", class_="finding-row")
        if parent:
            severity_el = parent.select_one(".impact")  # giả sử class “impact”
            severity = severity_el.get_text(strip=True) if severity_el else None
        else:
            severity = None

        data.append({
            "title": title,
            "severity": severity,
            "report_link": full_link
        })
        count += 1

    driver.quit()
    return data

if __name__ == "__main__":
    url = "https://solodit.cyfrin.io/?i=HIGH%2CMEDIUM%2CLOW%2CGAS&maxf=&minf=&rf=alltime&sd=Desc&sf=Recency"
    result = crawl_solodit_list(url, max_items=50)

    # lưu CSV
    with open("solodit_findings.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title","severity","report_link"])
        writer.writeheader()
        for r in result:
            writer.writerow(r)

    print(f"Crawled {len(result)} findings")
