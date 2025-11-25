from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

BASE_URL = "https://solodit.cyfrin.io/?b=false&f=&ff=&i=HIGH%2CMEDIUM%2CLOW%2CGAS&l=&maxf=&minf=&p={page}&pc=&pn=&qs=1&r=true&rf=alltime&rs=1&s=&sd=Desc&sf=Recency&t=&u=&ur=true"


def close_popup_if_exists(driver):
    try:
        close_button = driver.find_elements(By.XPATH, "//button[@title='Close']")
        print(len(close_button))
        if close_button:
            close_button[-1].click()
        time.sleep(2)
    except Exception:
        pass


def crawl_issue_list(driver, page_number, max_issue=10):

    url = BASE_URL.format(page=page_number)
    print(f"🔎 Crawling page {page_number}: {url}")

    driver.get(url)

    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^='/issues/']"))
        )
    except:
        print(f"⚠️ Page {page_number} không có dữ liệu!")
        return []

    time.sleep(1.0)
    close_popup_if_exists(driver)
    time.sleep(1.0)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    items = soup.select("a[href^='/issues/']")
    data = []
    count = 0

    for a in items:
        if count >= max_issue:
            break

        title = a.get_text(strip=True)
        link = "https://solodit.cyfrin.io" + a.get("href")

        data.append({
            "page": page_number,
            "title": title,
            "link": link
        })

        count += 1

    return data


def crawl_issue_detail(driver, url):
    return None
