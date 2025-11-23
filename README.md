# Project 3: Smart Contract Vulnerability Crawler

## Overview

This project is a Python-based crawler designed to automatically detect
and collect information about smart contract vulnerabilities from
blockchain explorers, security databases, and other related sources. The
crawler uses **Selenium** to automate browser interactions, allowing it
to scrape dynamic content and navigate through complex interfaces.

## Features

-   Automated crawling of smart contract sources
-   Extraction of vulnerability-related metadata
-   Selenium-based browser automation
-   Configurable crawling rules and target URLs
-   Data output in structured formats (JSON, CSV, etc.)

## Technologies Used

-   **Python 3**
-   **Selenium WebDriver**
-   **ChromeDriver / GeckoDriver**
-   **BeautifulSoup** (optional for parsing)
-   **Pandas** (optional for data output formatting)

## Installation

### 1. Clone the repository

``` bash
git clone https://github.com/yourusername/project3-smartcontract-crawler.git
cd project3-smartcontract-crawler
```

### 2. Install dependencies

``` bash
pip install -r requirements.txt
```

### 3. Install WebDriver

Download the appropriate driver for your browser: - ChromeDriver:
https://chromedriver.chromium.org/ - GeckoDriver:
https://github.com/mozilla/geckodriver/releases

Ensure it is added to your system PATH.

## Usage

### Run the crawler

``` bash
python crawler.py
```

### Configuration

Edit the `config.json` file to set: - Target URLs - Crawling depth -
Output format - Browser options

Example:

``` json
{
  "targets": ["https://example.com/contracts"],
  "headless": true,
  "output": "results.json"
}
```

## Project Structure

    ├── crawler.py
    ├── config.json
    ├── README.md
    ├── requirements.txt
    └── utils/
        ├── parser.py
        └── helpers.py

## Notes

-   Some websites may implement rate limiting or anti-bot measures.
    Consider using random delays or proxy rotation.
-   Ensure compliance with website terms of service when crawling.

## License

This project is licensed under the MIT License.