import scrapy
from scrapy_playwright.page import PageMethod

class BatdongsanSpider(scrapy.Spider):
    name = "batdongsan_spider"
    allowed_domains = ["batdongsan.com.vn"]
    start_urls = [
        "https://batdongsan.com.vn/nha-dat-ban/p1"
    ]

    custom_settings = {
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", ".js__card"),
                    ]
                },
                callback=self.parse,
                errback=self.errback
            )

    def parse(self, response):
        page = response.meta.get("playwright_page")
        if page:
            self.crawler.engine.playwright_context_manager.loop.create_task(page.close())
        with open("debug.html", "w", encoding="utf-8") as f:
            f.write(response.text)

        self.logger.info("💡 Saved debug.html to inspect the HTML content")

        for card in response.css(".js__card"):
            yield {
                "title": card.css(".js__card-title::text").get(),
                "price": card.css(".re__card-config-price::text").get(),
                "area": card.css(".re__card-config-area::text").get(),
                "location": card.css(".re__card-location span::text").getall()[-1]
                if card.css(".re__card-location span::text") else None,
            }

        # Crawl trang tiếp theo
        next_page = response.css("a[rel=next]::attr(href)").get()
        if next_page:
            next_url = response.urljoin(next_page)
            yield scrapy.Request(
                next_url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", ".js__card"),
                    ]
                },
                callback=self.parse,
                errback=self.errback
            )

    async def errback(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
