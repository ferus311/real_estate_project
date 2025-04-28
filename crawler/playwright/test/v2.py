from playwright.sync_api import sync_playwright
import json
import pandas as pd
import time
import random


def scrape_batdongsan(page):
    items = page.locator(".js__card")
    houses = []

    count = items.count()
    for i in range(count):
        item = items.nth(i)
        house = {
            "title": (
                item.locator(".re__card-title .js__card-title").inner_text(timeout=1000)
                if item.locator(".js__card-title").count() > 0
                else None
            ),
            "price": (
                item.locator(".re__card-config-price").inner_text(timeout=1000)
                if item.locator(".re__card-config-price").count() > 0
                else None
            ),
            "area": (
                item.locator(".re__card-config-area").inner_text(timeout=1000)
                if item.locator(".re__card-config-area").count() > 0
                else None
            ),
            "price_per_m2": (
                item.locator(".re__card-config-price_per_m2").inner_text(timeout=1000)
                if item.locator(".re__card-config-price_per_m2").count() > 0
                else None
            ),
            "location": (
                item.locator(".re__card-location span >> nth=1").inner_text(
                    timeout=1000
                )
                if item.locator(".re__card-location span").count() >= 2
                else None
            ),
            "description": (
                item.locator(".re__card-description").inner_text(timeout=1000)
                if item.locator(".re__card-description").count() > 0
                else None
            ),
            "bedroom": (
                item.locator(".re__card-config-bedroom span").inner_text(timeout=1000)
                if item.locator(".re__card-config-bedroom span").count() > 0
                else None
            ),
            "bathroom": (
                item.locator(".re__card-config-toilet span").inner_text(timeout=1000)
                if item.locator(".re__card-config-toilet span").count() > 0
                else None
            ),
        }
        houses.append(house)
    return houses


def scroll_page(page):
    for i in range(5):
        page.mouse.wheel(0, 1000)
        time.sleep(random.uniform(0.3, 0.8))


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=random.choice(
                [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
                ]
            )
        )

        # Block images, fonts, css
        context.route(
            "**/*",
            lambda route, req: (
                route.abort()
                if req.resource_type in ["image", "stylesheet", "font"]
                else route.continue_()
            ),
        )

        page = context.new_page()
        url = "https://batdongsan.com.vn/nha-dat-ban/p1"
        page.goto(url, timeout=60000)
        page.wait_for_load_state("networkidle")

        # scroll_page(page)  # cần nếu có lazy load

        data = scrape_batdongsan(page)

        with open("batdongsan.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        # df = pd.DataFrame(data)
        # df.to_csv("batdongsan.csv", index=False)

        browser.close()


if __name__ == "__main__":
    main()
