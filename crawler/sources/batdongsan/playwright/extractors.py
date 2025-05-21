import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from common.models.house_list import HouseListItem
from common.models.house_detail import HouseDetailItem

# Setup logger
logger = logging.getLogger(__name__)

# ===============================
# Utility functions
# ===============================


def extract_text(soup, selector: str):
    try:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else None
    except Exception as e:
        logger.error(f"Error extracting text with selector {selector}: {e}")
        return None


def normalize_spec_key(title: str):
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


# ===============================
# List page extractors
# ===============================


def extract_link(card_soup):
    try:
        a_tag = card_soup.select_one("a.js__product-link-for-product-id")
        if a_tag and a_tag.get("href"):
            href = a_tag["href"]
            return "https://batdongsan.com.vn" + href if href.startswith("/") else href
    except Exception as e:
        logger.error(f"Error extracting link: {e}")
    return None


def extract_list_item(card_soup):
    try:
        return HouseListItem(
            link=extract_link(card_soup),
            title=extract_text(card_soup, "h3.re__card-title span.pr-title"),
            price=extract_text(card_soup, "span.re__card-config-price"),
            area=extract_text(card_soup, "span.re__card-config-area"),
            location=extract_text(card_soup, "div.re__card-location span:nth-child(2)"),
        )
    except Exception as e:
        logger.error(f"Error extracting list item: {e}")
        return None


def extract_list_items(html_content: str):
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        cards = soup.select(".js__card")
        items = [extract_list_item(card) for card in cards]
        return [item for item in items if item is not None]  # Filter out None values
    except Exception as e:
        logger.error(f"Error extracting list items: {e}")
        return []


# ===============================
# Detail page extractors
# ===============================


def extract_specifications(soup):
    specs = {}
    try:
        items = soup.select(".re__pr-specs-content-item")
        for item in items:
            title = extract_text(item, ".re__pr-specs-content-item-title")
            value = extract_text(item, ".re__pr-specs-content-item-value")
            if title and value:
                key = normalize_spec_key(title)
                specs[key] = value
    except Exception as e:
        logger.error(f"Error extracting specifications: {e}")
    return specs


def extract_price_per_m2_alt(soup):
    try:
        return extract_text(soup, "div.re__pr-short-info-item span.ext")
    except Exception as e:
        logger.error(f"Error extracting price per m2: {e}")
        return None


def extract_coordinates_from_iframe(soup):
    try:
        iframe = soup.select_one("iframe.lazyload[data-src]")
        if iframe:
            data_src = iframe.get("data-src", "")
            match = re.search(r"q=([0-9.-]+),([0-9.-]+)", data_src)
            if match:
                lat, lng = match.groups()
                return str(lat), str(lng)
    except Exception as e:
        logger.error(f"Error extracting coordinates: {e}")
    return None, None


def extract_posted_date(soup):
    """
    Extract the posted date from the detail page and convert it to epoch seconds.
    Looks for elements like: <div class="re__pr-short-info-item js__pr-config-item"><span class="title">Ngày đăng</span><span class="value">20/05/2025</span></div>

    Returns:
        int or None: The posted date as epoch seconds, or None if not found/invalid
    """
    try:
        # Using a more specific selector for the date div with both classes
        date_items = soup.select("div.re__pr-short-info-item.js__pr-config-item")
        for item in date_items:
            title_span = item.select_one("span.title")
            if title_span and "Ngày đăng" in title_span.get_text(strip=True):
                value_span = item.select_one("span.value")
                if value_span:
                    date_str = value_span.get_text(strip=True)
                    try:
                        # Parse date in dd/mm/yyyy format
                        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                        # Convert to epoch seconds
                        return int(date_obj.timestamp())
                    except ValueError:
                        # If date format is incorrect, return None
                        logger.warning(f"Invalid date format: {date_str}")
                        pass
    except Exception as e:
        logger.error(f"Error extracting posted date: {e}")
    return None


def extract_detail_info(html_content: str):
    if not html_content or not html_content.strip():
        logger.error("Empty HTML content received")
        return None

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # Check if we got a proper page or an error page
        error_msg = soup.select_one(".re__server-error")
        if error_msg:
            logger.warning(
                f"Server error page detected: {error_msg.get_text(strip=True)}"
            )
            return None

        # Check for captcha or anti-bot mechanisms
        if (
            soup.select_one("captcha")
            or soup.select_one(".captcha")
            or "captcha" in html_content.lower()
        ):
            logger.warning("Captcha detected on the page")
            return None

        title = extract_text(soup, "h1.re__pr-title.pr-title.js__pr-title")
        if not title:
            logger.warning("Could not extract title - possibly invalid page structure")
            return None

        location_text = extract_text(
            soup, "span.re__pr-short-description.js__pr-address"
        )
        price_per_m2 = extract_price_per_m2_alt(soup)
        latitude, longitude = extract_coordinates_from_iframe(soup)
        description = extract_text(soup, "div.re__section-body.re__detail-content")
        specs = extract_specifications(soup)
        posted_date_epoch = extract_posted_date(soup)

        detail_item = HouseDetailItem(
            title=title,
            location=location_text,
            price=specs.get("price"),
            area=specs.get("area"),
            price_per_m2=price_per_m2,
            latitude=latitude,
            longitude=longitude,
            bedroom=specs.get("bedroom"),
            bathroom=specs.get("bathroom"),
            house_direction=specs.get("house_direction"),
            # balcony_direction=specs.get("balcony_direction"),
            legal_status=specs.get("legal_status"),
            interior=specs.get("interior"),
            facade_width=specs.get("facade_width"),
            road_width=specs.get("road_width"),
            floor_count=specs.get("floor_count"),
            description=description,
            source="batdongsan",
            posted_date=str(posted_date_epoch) if posted_date_epoch else None,
        )

        # Log successful extraction
        logger.info(f"Successfully extracted detail: {title[:30]}...")
        return detail_item

    except Exception as e:
        logger.error(f"Error extracting detail info: {e}")
        return None
