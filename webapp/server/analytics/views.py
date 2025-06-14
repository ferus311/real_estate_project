from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
from django.http import JsonResponse
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@api_view(["GET"])
def market_overview(request):
    """
    Get market overview statistics
    GET /api/analytics/market-overview/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")

        # Base query for market overview
        base_query = """
        SELECT
            COUNT(*) as total_properties,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(area) as avg_area,
            AVG(price_per_m2) as avg_price_per_m2,
            COUNT(DISTINCT province) as provinces_count,
            COUNT(DISTINCT district) as districts_count
        FROM properties
        WHERE price > 0 AND area > 0
        """

        params = []

        if province:
            base_query += " AND province ILIKE %s"
            params.append(f"%{province}%")

        if district:
            base_query += " AND district ILIKE %s"
            params.append(f"%{district}%")

        with connection.cursor() as cursor:
            cursor.execute(base_query, params)
            result = cursor.fetchone()

            overview = {
                "total_properties": result[0] or 0,
                "avg_price": float(result[1]) if result[1] else 0,
                "min_price": float(result[2]) if result[2] else 0,
                "max_price": float(result[3]) if result[3] else 0,
                "avg_area": float(result[4]) if result[4] else 0,
                "avg_price_per_m2": float(result[5]) if result[5] else 0,
                "provinces_count": result[6] or 0,
                "districts_count": result[7] or 0,
            }

            # Format prices
            overview["avg_price_formatted"] = f"{overview['avg_price']:,.0f} VND"
            overview["min_price_formatted"] = f"{overview['min_price']:,.0f} VND"
            overview["max_price_formatted"] = f"{overview['max_price']:,.0f} VND"
            overview["avg_price_per_m2_formatted"] = (
                f"{overview['avg_price_per_m2']:,.0f} VND/m²"
            )

        return Response(
            {
                "success": True,
                "market_overview": overview,
                "filters": {"province": province, "district": district},
            }
        )

    except Exception as e:
        logger.error(f"Error in market overview: {e}")
        return Response(
            {"error": f"Failed to get market overview: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def price_distribution(request):
    """
    Get price distribution by ranges
    GET /api/analytics/price-distribution/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")

        # Price ranges in VND
        price_ranges = [
            (0, 1_000_000_000, "< 1 tỷ"),
            (1_000_000_000, 3_000_000_000, "1-3 tỷ"),
            (3_000_000_000, 5_000_000_000, "3-5 tỷ"),
            (5_000_000_000, 10_000_000_000, "5-10 tỷ"),
            (10_000_000_000, 20_000_000_000, "10-20 tỷ"),
            (20_000_000_000, float("inf"), "> 20 tỷ"),
        ]

        distribution = []

        for min_price, max_price, label in price_ranges:
            query = """
            SELECT COUNT(*) as count, AVG(price) as avg_price
            FROM properties
            WHERE price >= %s AND price < %s
            """
            params = [min_price, max_price]

            if province:
                query += " AND province ILIKE %s"
                params.append(f"%{province}%")

            if district:
                query += " AND district ILIKE %s"
                params.append(f"%{district}%")

            with connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()

                distribution.append(
                    {
                        "range": label,
                        "min_price": min_price,
                        "max_price": max_price if max_price != float("inf") else None,
                        "count": result[0] or 0,
                        "avg_price": float(result[1]) if result[1] else 0,
                        "avg_price_formatted": (
                            f"{float(result[1]):,.0f} VND" if result[1] else "0 VND"
                        ),
                    }
                )

        return Response(
            {
                "success": True,
                "price_distribution": distribution,
                "filters": {"province": province, "district": district},
            }
        )

    except Exception as e:
        logger.error(f"Error in price distribution: {e}")
        return Response(
            {"error": f"Failed to get price distribution: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def property_type_stats(request):
    """
    Get statistics by property type
    GET /api/analytics/property-type-stats/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")

        query = """
        SELECT
            house_type,
            COUNT(*) as count,
            AVG(price) as avg_price,
            AVG(area) as avg_area,
            AVG(price_per_m2) as avg_price_per_m2,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM properties
        WHERE house_type IS NOT NULL AND price > 0 AND area > 0
        """

        params = []

        if province:
            query += " AND province ILIKE %s"
            params.append(f"%{province}%")

        if district:
            query += " AND district ILIKE %s"
            params.append(f"%{district}%")

        query += " GROUP BY house_type ORDER BY count DESC"

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

            stats = []
            for row in results:
                stats.append(
                    {
                        "house_type": row[0],
                        "count": row[1],
                        "avg_price": float(row[2]) if row[2] else 0,
                        "avg_area": float(row[3]) if row[3] else 0,
                        "avg_price_per_m2": float(row[4]) if row[4] else 0,
                        "min_price": float(row[5]) if row[5] else 0,
                        "max_price": float(row[6]) if row[6] else 0,
                        "avg_price_formatted": (
                            f"{float(row[2]):,.0f} VND" if row[2] else "0 VND"
                        ),
                        "avg_price_per_m2_formatted": (
                            f"{float(row[4]):,.0f} VND/m²" if row[4] else "0 VND/m²"
                        ),
                    }
                )

        return Response(
            {
                "success": True,
                "property_type_stats": stats,
                "filters": {"province": province, "district": district},
            }
        )

    except Exception as e:
        logger.error(f"Error in property type stats: {e}")
        return Response(
            {"error": f"Failed to get property type stats: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def location_stats(request):
    """
    Get statistics by location (top provinces/districts)
    GET /api/analytics/location-stats/
    """
    try:
        level = request.GET.get("level", "province")  # province or district
        province = request.GET.get("province")
        limit = min(int(request.GET.get("limit", 10)), 50)

        if level == "province":
            query = """
            SELECT
                province,
                COUNT(*) as count,
                AVG(price) as avg_price,
                AVG(area) as avg_area,
                AVG(price_per_m2) as avg_price_per_m2
            FROM properties
            WHERE province IS NOT NULL AND price > 0 AND area > 0
            GROUP BY province
            ORDER BY count DESC
            LIMIT %s
            """
            params = [limit]

        else:  # district level
            query = """
            SELECT
                district,
                province,
                COUNT(*) as count,
                AVG(price) as avg_price,
                AVG(area) as avg_area,
                AVG(price_per_m2) as avg_price_per_m2
            FROM properties
            WHERE district IS NOT NULL AND price > 0 AND area > 0
            """
            params = []

            if province:
                query += " AND province ILIKE %s"
                params.append(f"%{province}%")

            query += " GROUP BY district, province ORDER BY count DESC LIMIT %s"
            params.append(limit)

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

            stats = []
            for row in results:
                if level == "province":
                    stats.append(
                        {
                            "province": row[0],
                            "count": row[1],
                            "avg_price": float(row[2]) if row[2] else 0,
                            "avg_area": float(row[3]) if row[3] else 0,
                            "avg_price_per_m2": float(row[4]) if row[4] else 0,
                            "avg_price_formatted": (
                                f"{float(row[2]):,.0f} VND" if row[2] else "0 VND"
                            ),
                            "avg_price_per_m2_formatted": (
                                f"{float(row[4]):,.0f} VND/m²" if row[4] else "0 VND/m²"
                            ),
                        }
                    )
                else:
                    stats.append(
                        {
                            "district": row[0],
                            "province": row[1],
                            "count": row[2],
                            "avg_price": float(row[3]) if row[3] else 0,
                            "avg_area": float(row[4]) if row[4] else 0,
                            "avg_price_per_m2": float(row[5]) if row[5] else 0,
                            "avg_price_formatted": (
                                f"{float(row[3]):,.0f} VND" if row[3] else "0 VND"
                            ),
                            "avg_price_per_m2_formatted": (
                                f"{float(row[5]):,.0f} VND/m²" if row[5] else "0 VND/m²"
                            ),
                        }
                    )

        return Response(
            {
                "success": True,
                "location_stats": stats,
                "level": level,
                "filters": {"province": province, "limit": limit},
            }
        )

    except Exception as e:
        logger.error(f"Error in location stats: {e}")
        return Response(
            {"error": f"Failed to get location stats: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def price_trends(request):
    """
    Get price trends over time
    GET /api/analytics/price-trends/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")
        months = int(request.GET.get("months", 6))  # Default 6 months

        # Get monthly price trends
        query = """
        SELECT
            DATE_TRUNC('month', posted_date) as month,
            COUNT(*) as count,
            AVG(price) as avg_price,
            AVG(area) as avg_area,
            AVG(price_per_m2) as avg_price_per_m2
        FROM properties
        WHERE posted_date >= %s AND price > 0 AND area > 0
        """

        # Calculate start date
        start_date = datetime.now() - timedelta(days=months * 30)
        params = [start_date]

        if province:
            query += " AND province ILIKE %s"
            params.append(f"%{province}%")

        if district:
            query += " AND district ILIKE %s"
            params.append(f"%{district}%")

        query += " GROUP BY DATE_TRUNC('month', posted_date) ORDER BY month"

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

            trends = []
            for row in results:
                trends.append(
                    {
                        "month": row[0].strftime("%Y-%m") if row[0] else None,
                        "count": row[1],
                        "avg_price": float(row[2]) if row[2] else 0,
                        "avg_area": float(row[3]) if row[3] else 0,
                        "avg_price_per_m2": float(row[4]) if row[4] else 0,
                        "avg_price_formatted": (
                            f"{float(row[2]):,.0f} VND" if row[2] else "0 VND"
                        ),
                        "avg_price_per_m2_formatted": (
                            f"{float(row[4]):,.0f} VND/m²" if row[4] else "0 VND/m²"
                        ),
                    }
                )

        return Response(
            {
                "success": True,
                "price_trends": trends,
                "filters": {
                    "province": province,
                    "district": district,
                    "months": months,
                },
            }
        )

    except Exception as e:
        logger.error(f"Error in price trends: {e}")
        return Response(
            {"error": f"Failed to get price trends: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def area_distribution(request):
    """
    Get area distribution statistics
    GET /api/analytics/area-distribution/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")

        # Area ranges in m²
        area_ranges = [
            (0, 50, "< 50m²"),
            (50, 100, "50-100m²"),
            (100, 200, "100-200m²"),
            (200, 500, "200-500m²"),
            (500, float("inf"), "> 500m²"),
        ]

        distribution = []

        for min_area, max_area, label in area_ranges:
            query = """
            SELECT COUNT(*) as count, AVG(price) as avg_price, AVG(price_per_m2) as avg_price_per_m2
            FROM properties
            WHERE area >= %s AND area < %s
            """
            params = [min_area, max_area]

            if province:
                query += " AND province ILIKE %s"
                params.append(f"%{province}%")

            if district:
                query += " AND district ILIKE %s"
                params.append(f"%{district}%")

            with connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()

                distribution.append(
                    {
                        "range": label,
                        "min_area": min_area,
                        "max_area": max_area if max_area != float("inf") else None,
                        "count": result[0] or 0,
                        "avg_price": float(result[1]) if result[1] else 0,
                        "avg_price_per_m2": float(result[2]) if result[2] else 0,
                        "avg_price_formatted": (
                            f"{float(result[1]):,.0f} VND" if result[1] else "0 VND"
                        ),
                        "avg_price_per_m2_formatted": (
                            f"{float(result[2]):,.0f} VND/m²"
                            if result[2]
                            else "0 VND/m²"
                        ),
                    }
                )

        return Response(
            {
                "success": True,
                "area_distribution": distribution,
                "filters": {"province": province, "district": district},
            }
        )

    except Exception as e:
        logger.error(f"Error in area distribution: {e}")
        return Response(
            {"error": f"Failed to get area distribution: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def dashboard_summary(request):
    """
    Get dashboard summary with key metrics
    GET /api/analytics/dashboard-summary/
    """
    try:
        # Get overall statistics
        with connection.cursor() as cursor:
            # Total properties and basic stats
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_properties,
                    AVG(price) as avg_price,
                    AVG(area) as avg_area,
                    COUNT(DISTINCT province) as provinces,
                    COUNT(DISTINCT district) as districts
                FROM properties
                WHERE price > 0 AND area > 0
            """
            )
            basic_stats = cursor.fetchone()

            # Recent activity (last 30 days)
            cursor.execute(
                """
                SELECT COUNT(*) as recent_properties
                FROM properties
                WHERE posted_date >= %s
            """,
                [datetime.now() - timedelta(days=30)],
            )
            recent_count = cursor.fetchone()[0]

            # Top 3 provinces by count
            cursor.execute(
                """
                SELECT province, COUNT(*) as count
                FROM properties
                WHERE province IS NOT NULL
                GROUP BY province
                ORDER BY count DESC
                LIMIT 3
            """
            )
            top_provinces = cursor.fetchall()

            # Top 3 property types
            cursor.execute(
                """
                SELECT house_type, COUNT(*) as count
                FROM properties
                WHERE house_type IS NOT NULL
                GROUP BY house_type
                ORDER BY count DESC
                LIMIT 3
            """
            )
            top_types = cursor.fetchall()

        summary = {
            "total_properties": basic_stats[0] or 0,
            "avg_price": float(basic_stats[1]) if basic_stats[1] else 0,
            "avg_area": float(basic_stats[2]) if basic_stats[2] else 0,
            "provinces_count": basic_stats[3] or 0,
            "districts_count": basic_stats[4] or 0,
            "recent_properties": recent_count or 0,
            "avg_price_formatted": (
                f"{float(basic_stats[1]):,.0f} VND" if basic_stats[1] else "0 VND"
            ),
            "top_provinces": [
                {"province": row[0], "count": row[1]} for row in top_provinces
            ],
            "top_property_types": [
                {"type": row[0], "count": row[1]} for row in top_types
            ],
        }

        return Response({"success": True, "dashboard_summary": summary})

    except Exception as e:
        logger.error(f"Error in dashboard summary: {e}")
        return Response(
            {"error": f"Failed to get dashboard summary: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
