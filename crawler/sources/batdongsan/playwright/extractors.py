from crawler.common.models.house_list import HouseListItem
from crawler.common.models.house_detail import HouseDetailItem
import re


# ===============================
# Utility extractor
# ===============================
async def extract_text(element_or_page, selector):
    try:
        loc = element_or_page.locator(selector)
        if await loc.count() > 0:
            text = await loc.text_content()
            return text.strip() if text else None
        return None
    except:
        return None


# ===============================
# List page extractors
# ===============================


async def extract_link(element):
    try:
        loc = element.locator("a.js__product-link-for-product-id")
        if await loc.count() > 0:
            href = await loc.get_attribute("href")
            return "https://batdongsan.com.vn" + href if href.startswith("/") else href
        return None
    except:
        return None


async def extract_list_item(element):
    title = await extract_text(element, "h3.re__card-title span.pr-title")
    price = await extract_text(element, "span.re__card-config-price")
    area = await extract_text(element, "span.re__card-config-area")
    location = await extract_text(element, "div.re__card-location span:nth-child(2)")
    link = await extract_link(element)

    return HouseListItem(
        link=link,
        title=title,
        price=price,
        area=area,
        location=location,
    )


async def extract_list_items(page):
    elements = await page.locator(".js__card").all()
    return [await extract_list_item(el) for el in elements]


# ===============================
# Detail page extractors
# ===============================


def normalize_spec_key(title):
    mapping = {
        "Mức giá": "price",
        "Diện tích": "area",
        "Số phòng ngủ": "bedroom",
        "Số phòng tắm, vệ sinh": "bathroom",
        "Hướng nhà": "house_direction",
        "Hướng ban công": "balcony_direction",
        "Pháp lý": "legal_status",
        "Nội thất": "interior",
        "Mặt tiền": "facade_width",
        "Đường vào": "road_width",
        "Số tầng": "floor_count",
    }
    return mapping.get(title.strip(), title.strip().lower().replace(" ", "_"))


async def extract_specifications(page):
    specs = {}
    items = await page.locator(".re__pr-specs-content-item").all()

    for item in items:
        try:
            title = await extract_text(item, ".re__pr-specs-content-item-title")
            value = await extract_text(item, ".re__pr-specs-content-item-value")

            if title and value:
                normalized_key = normalize_spec_key(title)
                specs[normalized_key] = value
        except:
            continue

    return specs


async def extract_price_per_m2_alt(page):
    try:
        loc = page.locator("div.re__pr-short-info-item span.ext")
        if await loc.count() > 0:
            text = await loc.text_content()
            return text.strip() if text else None
        return None
    except:
        return None


async def extract_coordinates(page):
    try:
        # Look for the map data in the page's script tags
        script_loc = page.locator("script:contains('LatLng(')")
        if await script_loc.count() > 0:
            script_content = await script_loc.text_content()
            if script_content:
                # Extract coordinates using regex patterns
                import re

                coords_match = re.search(
                    r"LatLng\(([0-9.-]+),\s*([0-9.-]+)\)", script_content
                )
                if coords_match:
                    lat, lng = coords_match.groups()
                    return f"{lat},{lng}"
        return None
    except:
        return None


async def extract_detail_info(page):
    title = await extract_text(page, "h1.re__pr-title.pr-title.js__pr-title")
    short_description = await extract_text(
        page, "span.re__pr-short-description.js__pr-address"
    )
    price_per_m2 = await extract_price_per_m2_alt(page)
    coordinates = await extract_coordinates(page)
    description = await extract_text(page, "div.re__section-body.re__detail-content")
    # Bổ sung thông tin chi tiết dạng flexible
    specs = await extract_specifications(page)  # dùng hàm bạn đã có

    return HouseDetailItem(
        title=title,
        short_description=short_description,
        price=specs.get("price"),
        area=specs.get("area"),
        price_per_m2=price_per_m2,
        coordinates=coordinates,
        bedroom=specs.get("bedroom"),
        bathroom=specs.get("bathroom"),
        house_direction=specs.get("house_direction"),
        balcony_direction=specs.get("balcony_direction"),
        legal_status=specs.get("legal_status"),
        interior=specs.get("interior"),
        facade_width=specs.get("facade_width"),
        road_width=specs.get("road_width"),
        floor_count=specs.get("floor_count"),
        description=description,
    )
