import json
import pandas as pd
import logging
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

class House:
    def __init__(
        self, id, title, price, images, area, price_per_m2, location, description
    ):
        self.id = id
        self.title = title
        self.price = price
        self.images = images
        self.area = area
        self.price_per_m2 = price_per_m2
        self.location = location
        self.description = description

def create_house_object(data):
    return House(
        id=None,
        title=data.get("title"),
        price=data.get("price"),
        images=data.get("images", []),
        area=data.get("area"),
        price_per_m2=data.get("price_per_m2"),
        location=data.get("location"),
        description=data.get("description")
    )

# Extract functions
async def extract_title(element):
    return await extract_text(element, "h3.re__card-title span.pr-title")

async def extract_price(element):
    return await extract_text(element, "span.re__card-config-price")

async def extract_area(element):
    return await extract_text(element, "span.re__card-config-area")

async def extract_price_per_m2(element):
    return await extract_text(element, "span.re__card-config-price_per_m2")

async def extract_location(element):
    return await extract_text(element, "div.re__card-location span:nth-child(2)")

async def extract_bedroom(element):
    return await extract_text(element, "span.re__card-config-bedroom span")

async def extract_bathroom(element):
    return await extract_text(element, "span.re__card-config-toilet span")

async def extract_description(element):
    return await extract_text(element, "div.re__card-description")

async def extract_text(element, selector):
    try:
        loc = element.locator(selector)
        if await loc.count() > 0:
            text = await loc.text_content()
            return text.strip() if text else None
        else:
            return None
    except:
        return None

# Main function to get house data
async def getJsonHouses(page):
    # logging.debug("Getting JSON houses from page content...")
    houses = []
    elements = await page.locator(".js__card").all()

    for element in elements:
        tasks = [
            extract_title(element),
            extract_price(element),
            extract_area(element),
            extract_price_per_m2(element),
            extract_location(element),
            extract_bedroom(element),
            extract_bathroom(element),
            extract_description(element),
        ]

        (
            title,
            price,
            area,
            price_per_m2,
            location,
            bedroom,
            bathroom,
            description,
        ) = await asyncio.gather(*tasks)

        house = {
            "title": title,
            "price": price,
            "area": area,
            "price_per_m2": price_per_m2,
            "location": location,
            "bedroom": bedroom,
            "bathroom": bathroom,
            "description": description,
        }

        if location and area:
            houses.append(house)

    return houses

async def navigateToWeb(url, page):
    logging.debug(f"Navigating to URL: {url}")
    await page.goto(url, timeout=60000)

async def writeToFile(file_path, element_list):
    logging.debug(f"Writing data to file: {file_path}")
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
    except:
        data = []
    data.extend(element_list)
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)
