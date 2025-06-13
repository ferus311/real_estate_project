"""
API Examples for Real Estate Website
Sử dụng Gold Schema Data
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass

# ===============================
# DATA MODELS từ Gold Schema
# ===============================


@dataclass
class PropertyListItem:
    """Model cho danh sách properties"""

    property_id: str
    title: str
    price: Optional[float]
    area: Optional[float]
    price_per_m2: Optional[float]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    province: str
    district: str
    ward: Optional[str]
    address: str
    latitude: Optional[float]
    longitude: Optional[float]
    house_type: Optional[str]
    data_type: str  # 'sell' or 'rent'
    posted_date: datetime
    source: str


@dataclass
class PropertyDetail:
    """Model chi tiết property"""

    # Basic info (từ PropertyListItem)
    property_id: str
    title: str
    description: Optional[str]
    price: Optional[float]
    area: Optional[float]
    price_per_m2: Optional[float]

    # Property details
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    floor_count: Optional[int]

    # Location
    province: str
    district: str
    ward: Optional[str]
    street: Optional[str]
    address: str
    latitude: Optional[float]
    longitude: Optional[float]

    # Advanced features
    house_type: Optional[str]
    house_direction: Optional[str]
    legal_status: Optional[str]
    interior: Optional[str]

    # Dimensions
    width: Optional[float]
    length: Optional[float]
    living_size: Optional[float]
    facade_width: Optional[float]
    road_width: Optional[float]

    # Metadata
    data_type: str
    source: str
    url: Optional[str]
    posted_date: datetime
    data_quality_score: Optional[float]


@dataclass
class SearchFilters:
    """Model cho search filters"""

    # Location filters
    province: Optional[str] = None
    district: Optional[str] = None
    ward: Optional[str] = None

    # Price & Area filters
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    area_min: Optional[float] = None
    area_max: Optional[float] = None
    price_per_m2_min: Optional[float] = None
    price_per_m2_max: Optional[float] = None

    # Property filters
    bedrooms_min: Optional[int] = None
    bathrooms_min: Optional[int] = None
    data_type: Optional[str] = None  # 'sell', 'rent', or None for both

    # Advanced filters
    house_type: Optional[str] = None
    house_direction: Optional[str] = None
    legal_status: Optional[str] = None
    interior: Optional[str] = None

    # Dimension filters
    width_min: Optional[float] = None
    length_min: Optional[float] = None
    facade_width_min: Optional[float] = None

    # Geographic filters
    lat_min: Optional[float] = None
    lat_max: Optional[float] = None
    lng_min: Optional[float] = None
    lng_max: Optional[float] = None

    # Sorting & Pagination
    sort_by: str = "posted_date"  # 'price', 'area', 'price_per_m2', 'posted_date'
    sort_order: str = "desc"  # 'asc' or 'desc'
    page: int = 1
    limit: int = 20


# ===============================
# SQL QUERIES cho các chức năng web
# ===============================


class RealEstateQueries:
    """Collection of SQL queries for real estate website"""

    @staticmethod
    def search_properties(filters: SearchFilters) -> str:
        """
        Tìm kiếm properties với filters phức tạp
        Sử dụng tất cả fields từ Gold schema
        """
        where_clauses = ["1=1"]  # Base condition

        # Location filters
        if filters.province:
            where_clauses.append(f"province = '{filters.province}'")
        if filters.district:
            where_clauses.append(f"district = '{filters.district}'")
        if filters.ward:
            where_clauses.append(f"ward = '{filters.ward}'")

        # Price filters
        if filters.price_min:
            where_clauses.append(f"price >= {filters.price_min}")
        if filters.price_max:
            where_clauses.append(f"price <= {filters.price_max}")

        # Area filters
        if filters.area_min:
            where_clauses.append(f"area >= {filters.area_min}")
        if filters.area_max:
            where_clauses.append(f"area <= {filters.area_max}")

        # Price per m2 filters
        if filters.price_per_m2_min:
            where_clauses.append(f"price_per_m2 >= {filters.price_per_m2_min}")
        if filters.price_per_m2_max:
            where_clauses.append(f"price_per_m2 <= {filters.price_per_m2_max}")

        # Property detail filters
        if filters.bedrooms_min:
            where_clauses.append(f"bedroom >= {filters.bedrooms_min}")
        if filters.bathrooms_min:
            where_clauses.append(f"bathroom >= {filters.bathrooms_min}")
        if filters.data_type:
            where_clauses.append(f"data_type = '{filters.data_type}'")

        # Advanced filters
        if filters.house_type:
            where_clauses.append(f"house_type = '{filters.house_type}'")
        if filters.house_direction:
            where_clauses.append(f"house_direction = '{filters.house_direction}'")
        if filters.legal_status:
            where_clauses.append(f"legal_status = '{filters.legal_status}'")
        if filters.interior:
            where_clauses.append(f"interior = '{filters.interior}'")

        # Dimension filters
        if filters.width_min:
            where_clauses.append(f"width >= {filters.width_min}")
        if filters.length_min:
            where_clauses.append(f"length >= {filters.length_min}")
        if filters.facade_width_min:
            where_clauses.append(f"facade_width >= {filters.facade_width_min}")

        # Geographic bounding box
        if all([filters.lat_min, filters.lat_max, filters.lng_min, filters.lng_max]):
            where_clauses.append(
                f"""
                latitude BETWEEN {filters.lat_min} AND {filters.lat_max}
                AND longitude BETWEEN {filters.lng_min} AND {filters.lng_max}
            """
            )

        # Build the complete query
        where_clause = " AND ".join(where_clauses)
        offset = (filters.page - 1) * filters.limit

        return f"""
        SELECT
            id as property_id,
            title,
            price,
            area,
            price_per_m2,
            bedroom as bedrooms,
            bathroom as bathrooms,
            province,
            district,
            ward,
            location as address,
            latitude,
            longitude,
            house_type,
            data_type,
            posted_date,
            source,
            data_quality_score
        FROM properties
        WHERE {where_clause}
            AND title IS NOT NULL
            AND data_quality_score > 0.3  -- Filter low quality data
        ORDER BY {filters.sort_by} {filters.sort_order}
        LIMIT {filters.limit} OFFSET {offset}
        """

    @staticmethod
    def get_property_detail(property_id: str) -> str:
        """Lấy chi tiết đầy đủ của 1 property"""
        return f"""
        SELECT
            id as property_id,
            title,
            description,
            price,
            area,
            price_per_m2,
            bedroom as bedrooms,
            bathroom as bathrooms,
            floor_count,
            province,
            district,
            ward,
            street,
            location as address,
            latitude,
            longitude,
            house_type,
            house_direction,
            legal_status,
            interior,
            width,
            length,
            living_size,
            facade_width,
            road_width,
            data_type,
            source,
            url,
            posted_date,
            data_quality_score
        FROM properties
        WHERE id = '{property_id}'
        """

    @staticmethod
    def get_similar_properties(property_id: str, limit: int = 6) -> str:
        """Tìm properties tương tự (cùng khu vực, giá gần nhau)"""
        return f"""
        WITH target_property AS (
            SELECT province, district, price, area, house_type
            FROM properties
            WHERE id = '{property_id}'
        )
        SELECT
            p.id as property_id,
            p.title,
            p.price,
            p.area,
            p.price_per_m2,
            p.bedroom as bedrooms,
            p.bathroom as bathrooms,
            p.province,
            p.district,
            p.location as address,
            p.latitude,
            p.longitude,
            p.posted_date
        FROM properties p, target_property t
        WHERE p.id != '{property_id}'
            AND p.province = t.province
            AND p.district = t.district
            AND p.title IS NOT NULL
            AND (
                p.house_type = t.house_type OR
                ABS(p.price - t.price) / t.price < 0.3 OR  -- Price within 30%
                ABS(p.area - t.area) / t.area < 0.2        -- Area within 20%
            )
        ORDER BY
            CASE WHEN p.house_type = t.house_type THEN 0 ELSE 1 END,
            ABS(p.price - t.price) / t.price,
            ABS(p.area - t.area) / t.area
        LIMIT {limit}
        """

    @staticmethod
    def get_map_properties(filters: SearchFilters) -> str:
        """Lấy properties cho hiển thị trên map (chỉ cần tọa độ + info cơ bản)"""
        where_clauses = ["latitude IS NOT NULL", "longitude IS NOT NULL"]

        # Apply location filters
        if filters.province:
            where_clauses.append(f"province = '{filters.province}'")
        if filters.district:
            where_clauses.append(f"district = '{filters.district}'")

        # Apply price filters
        if filters.price_min:
            where_clauses.append(f"price >= {filters.price_min}")
        if filters.price_max:
            where_clauses.append(f"price <= {filters.price_max}")

        # Geographic bounding box (important for map)
        if all([filters.lat_min, filters.lat_max, filters.lng_min, filters.lng_max]):
            where_clauses.append(
                f"""
                latitude BETWEEN {filters.lat_min} AND {filters.lat_max}
                AND longitude BETWEEN {filters.lng_min} AND {filters.lng_max}
            """
            )

        where_clause = " AND ".join(where_clauses)

        return f"""
        SELECT
            id as property_id,
            title,
            price,
            area,
            latitude,
            longitude,
            province,
            district,
            house_type,
            data_type
        FROM properties
        WHERE {where_clause}
            AND data_quality_score > 0.5  -- Higher quality for map
        LIMIT 1000  -- Limit for performance
        """

    @staticmethod
    def get_market_analytics(province: Optional[str] = None) -> str:
        """Thống kê thị trường"""
        where_clause = "price IS NOT NULL AND area IS NOT NULL"
        if province:
            where_clause += f" AND province = '{province}'"

        return f"""
        SELECT
            province,
            district,
            COUNT(*) as total_properties,
            AVG(price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(area) as avg_area,
            AVG(price_per_m2) as avg_price_per_m2,
            COUNT(CASE WHEN data_type = 'sell' THEN 1 END) as for_sale_count,
            COUNT(CASE WHEN data_type = 'rent' THEN 1 END) as for_rent_count
        FROM properties
        WHERE {where_clause}
        GROUP BY province, district
        ORDER BY total_properties DESC
        """

    @staticmethod
    def get_price_trends(province: str, months: int = 12) -> str:
        """Xu hướng giá theo thời gian"""
        return f"""
        SELECT
            DATE_TRUNC('month', posted_date) as month,
            COUNT(*) as listings_count,
            AVG(price) as avg_price,
            AVG(price_per_m2) as avg_price_per_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
        FROM properties
        WHERE province = '{province}'
            AND posted_date >= NOW() - INTERVAL '{months} months'
            AND price IS NOT NULL
        GROUP BY DATE_TRUNC('month', posted_date)
        ORDER BY month DESC
        """

    @staticmethod
    def get_filter_options() -> Dict[str, str]:
        """Lấy các options cho filters"""
        return {
            "provinces": """
                SELECT DISTINCT province, COUNT(*) as count
                FROM properties
                WHERE province IS NOT NULL
                GROUP BY province
                ORDER BY count DESC
            """,
            "house_types": """
                SELECT DISTINCT house_type, COUNT(*) as count
                FROM properties
                WHERE house_type IS NOT NULL
                GROUP BY house_type
                ORDER BY count DESC
            """,
            "house_directions": """
                SELECT DISTINCT house_direction, COUNT(*) as count
                FROM properties
                WHERE house_direction IS NOT NULL
                GROUP BY house_direction
                ORDER BY count DESC
            """,
            "legal_statuses": """
                SELECT DISTINCT legal_status, COUNT(*) as count
                FROM properties
                WHERE legal_status IS NOT NULL
                GROUP BY legal_status
                ORDER BY count DESC
            """,
            "interior_types": """
                SELECT DISTINCT interior, COUNT(*) as count
                FROM properties
                WHERE interior IS NOT NULL
                GROUP BY interior
                ORDER BY count DESC
            """,
        }


# ===============================
# API ROUTE EXAMPLES (FastAPI)
# ===============================

"""
from fastapi import FastAPI, Query
from typing import Optional

app = FastAPI()

@app.get("/api/properties")
async def search_properties(
    province: Optional[str] = None,
    district: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    bedrooms_min: Optional[int] = None,
    house_type: Optional[str] = None,
    data_type: Optional[str] = None,
    sort_by: str = "posted_date",
    sort_order: str = "desc",
    page: int = 1,
    limit: int = 20
):
    filters = SearchFilters(
        province=province,
        district=district,
        price_min=price_min,
        price_max=price_max,
        area_min=area_min,
        area_max=area_max,
        bedrooms_min=bedrooms_min,
        house_type=house_type,
        data_type=data_type,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        limit=limit
    )

    query = RealEstateQueries.search_properties(filters)
    # Execute query and return results

@app.get("/api/properties/{property_id}")
async def get_property_detail(property_id: str):
    query = RealEstateQueries.get_property_detail(property_id)
    # Execute query and return results

@app.get("/api/properties/{property_id}/similar")
async def get_similar_properties(property_id: str, limit: int = 6):
    query = RealEstateQueries.get_similar_properties(property_id, limit)
    # Execute query and return results

@app.get("/api/properties/map")
async def get_map_properties(
    province: Optional[str] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
    lng_min: Optional[float] = None,
    lng_max: Optional[float] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None
):
    filters = SearchFilters(
        province=province,
        lat_min=lat_min,
        lat_max=lat_max,
        lng_min=lng_min,
        lng_max=lng_max,
        price_min=price_min,
        price_max=price_max
    )

    query = RealEstateQueries.get_map_properties(filters)
    # Execute query and return results

@app.get("/api/analytics/market")
async def get_market_analytics(province: Optional[str] = None):
    query = RealEstateQueries.get_market_analytics(province)
    # Execute query and return results

@app.get("/api/analytics/price-trends")
async def get_price_trends(province: str, months: int = 12):
    query = RealEstateQueries.get_price_trends(province, months)
    # Execute query and return results

@app.get("/api/filters/options")
async def get_filter_options():
    queries = RealEstateQueries.get_filter_options()
    # Execute all queries and return combined results
"""
