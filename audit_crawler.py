import uuid
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import html2text

BASE_URL = "https://solodit.cyfrin.io/?b=false&f=&ff=&i=HIGH%2CMEDIUM%2CLOW%2CGAS&l=&maxf=&minf=&p={page}&pc=&pn=&qs=1&r=true&rf=alltime&rs=1&s=&sd=Desc&sf=Recency&t=&u=&ur=true"


def close_popup_if_exists(driver):
    try:
        close_button = driver.find_elements(By.XPATH, "//button[@title='Close']")
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
        print(f"Page {page_number} không có dữ liệu!")
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
    driver.get(url)
    time.sleep(3.5)

    data = {}

    issue_id = driver.find_elements(By.XPATH, ".//li/span[contains(text(),'#')]")
    if issue_id:
        data["id"] = issue_id[0].text.strip()

    header = driver.find_element(By.XPATH, "//h1/ancestor::div[2]")
    if header:
        title = header.find_element(By.XPATH, "//h1")
        if title:
            data["title"] = title.text.strip()

        name_and_time = header.find_element(By.XPATH, ".//span")
        if name_and_time:
            nt = name_and_time.find_elements(By.XPATH, ".//span")
            if len(nt) >= 3:
                data["name"] = nt[0].text.strip()
                data["time"] = nt[-1].text.strip()

    overview = driver.find_elements(By.XPATH, "//span[contains(text(),'Overview')]")
    if overview:
        overview = overview[-1]
        desc = overview.find_element(By.XPATH, "../../div[2]")

        impact = desc.find_element(By.XPATH, ".//div[contains(text(),'Impact')]/..")
        if impact:
            impact_value = impact.find_element(By.XPATH, ".//p")
            data["impact"] = impact_value.text.strip()

        quality = desc.find_element(By.XPATH, ".//div[contains(text(),'Quality')]/..")
        if quality:
            quality_value = quality.find_element(By.XPATH, ".//span").text.strip().split()
            data["quality_rate"] = quality_value[0]
            data["quality_votes"] = quality_value[-1].strip("()")

        rarity = desc.find_element(By.XPATH, ".//div[contains(text(),'Rarity')]/..")
        if rarity:
            rarity_value = rarity.find_element(By.XPATH, ".//span").text.strip().split()
            data["rarity_rate"] = rarity_value[0]
            data["rarity_votes"] = rarity_value[-1].strip("()")

        full_report = desc.find_element(By.XPATH, ".//div[contains(text(),'Full report')]/..")
        if full_report:
            full_report_url = full_report.find_element(By.XPATH, ".//a")
            data["full_report_url"] = full_report_url.get_attribute("href").strip()

        categories = desc.find_element(By.XPATH, ".//div[contains(text(),'Categories')]/..")
        if categories:
            data["categories"] = categories.text.strip()

        tags = desc.find_element(By.XPATH, ".//div[contains(text(),'Tags')]/..")
        if tags:
            data["tags"] = tags.text.strip()

        authors = desc.find_element(By.XPATH, ".//div[contains(text(),'Author(s)')]/..")
        if authors:
            author_list = authors.find_element(By.XPATH, ".//span")
            data["authors"] = author_list.text.strip().split()

    wait = WebDriverWait(driver, 5)
    markdown_block = wait.until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'markdown')]//p"))
    )

    description = driver.find_element(By.XPATH, "//div[contains(@class, 'markdown')]")
    if description:
        html_content = description.get_attribute("innerHTML")

        soup = BeautifulSoup(html_content, "html.parser")
        code_blocks = {}
        placeholder_prefix = "CODEBLOCKPLACEHOLDER_"

        for container in soup.find_all("div", class_="ql-code-block-container"):
            language = container.get("data-language", "").strip()
            code_lines = [
                line.get_text() for line in container.find_all("div", class_="ql-code-block")
            ]

            markdown_code = "```" + language + "\n" + "\n".join(code_lines) + "\n```\n"

            token = placeholder_prefix + str(uuid.uuid4()).replace("-", "")
            code_blocks[token] = markdown_code
            container.replace_with(token)

        h = html2text.HTML2Text()
        h.body_width = 0
        h.single_line_break = True
        h.ignore_links = False
        h.ignore_images = True

        md = h.handle(str(soup))

        md = md.replace("\n**", "\n\n**")

        for token, code in code_blocks.items():
            md = md.replace(token, code)

        md = md.replace("\\clearpage", "").strip()

        data["description"] = md

    return data

