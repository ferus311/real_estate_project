import scrapy
from scrapy_playwright.page import PageMethod

class TestSpider(scrapy.Spider):
    name = "test"
    def start_requests(self):
        yield scrapy.Request(

            url="https://shoppable-campaign-demo.netlify.app/#/",
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", '.card-body'),
                ],
            },
        )

    def parse(self, response):

        products = response.xpath('//*[@class="card-body"]')
        for product in products:
            yield {
            'title':product.xpath('.//*[@class="card-title"]/text()').get()

            }
