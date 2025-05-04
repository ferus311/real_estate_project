from playwright.sync_api import sync_playwright
import random

# Danh sách user-agent
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

url = "https://batdongsan.com.vn/nha-dat-ban"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)

    context = browser.new_context(
        user_agent=random.choice(user_agents),
        java_script_enabled=True,
    )

    # Tắt tải ảnh và css (tăng tốc)
    context.route(
        "**/*",
        lambda route, request: (
            route.abort()
            if request.resource_type in ["image", "stylesheet", "font"]
            else route.continue_()
        ),
    )

    page = context.new_page()
    page.goto(url, timeout=60000)  # timeout = 60s

    page.wait_for_load_state("networkidle")  # đảm bảo load xong

    def get_js_card_divs(page):
        divs = page.query_selector_all("div.js__card")
        for div in divs:
            print(div.outer_html())
            links = div.query_selector_all("a.js_product-link-for-product-id")
            for link in links:
                href = link.get_attribute("href")
                print(href)

    get_js_card_divs(page)

    html = page.content()
    print(html)

    browser.close()
