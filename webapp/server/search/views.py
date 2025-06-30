from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
from django.http import JsonResponse
import logging

logger = logging.getLogger(__name__)


@api_view(["GET", "POST"])
def advanced_search(request):
    """
    Advanced property search with filters
    GET/POST /api/search/advanced/
    """
    try:
        # Get search parameters from GET or POST
        if request.method == "GET":
            params = request.GET
        else:
            params = request.data

        # Build search query based on parameters
        base_query = """
        SELECT
            id,
            title,
            description,
            price,
            area,
            price_per_m2,
            province,
            district,
            ward,
            street,
            location,
            latitude,
            longitude,
            bedroom,
            bathroom,
            floor_count,
            house_type,
            house_direction,
            legal_status,
            interior,
            posted_date,
            source,
            url
        FROM properties
        WHERE 1=1
        """

        filters = []
        query_params = []

        # Location filters
        if params.get("province"):
            filters.append("province ILIKE %s")
            query_params.append(f"%{params['province']}%")

        if params.get("district"):
            filters.append("district ILIKE %s")
            query_params.append(f"%{params['district']}%")

        if params.get("ward"):
            filters.append("ward ILIKE %s")
            query_params.append(f"%{params['ward']}%")

        if params.get("street"):
            filters.append("street ILIKE %s")
            query_params.append(f"%{params['street']}%")

        # Price filters
        if params.get("price_min"):
            filters.append("price >= %s")
            query_params.append(float(params["price_min"]))

        if params.get("price_max"):
            filters.append("price <= %s")
            query_params.append(float(params["price_max"]))

        # Area filters
        if params.get("area_min"):
            filters.append("area >= %s")
            query_params.append(float(params["area_min"]))

        if params.get("area_max"):
            filters.append("area <= %s")
            query_params.append(float(params["area_max"]))

        # Room filters
        if params.get("bedroom_min"):
            filters.append("bedroom >= %s")
            query_params.append(int(params["bedroom_min"]))

        if params.get("bedroom"):
            filters.append("bedroom = %s")
            query_params.append(int(params["bedroom"]))

        if params.get("bathroom_min"):
            filters.append("bathroom >= %s")
            query_params.append(int(params["bathroom_min"]))

        # Property type filter
        if params.get("house_type"):
            filters.append("house_type ILIKE %s")
            query_params.append(f"%{params['house_type']}%")

        # Legal status filter
        if params.get("legal_status"):
            filters.append("legal_status ILIKE %s")
            query_params.append(f"%{params['legal_status']}%")

        # Full-text search
        if params.get("search_text") or params.get("keyword"):
            search_keyword = params.get("search_text") or params.get("keyword")
            filters.append(
                "(title ILIKE %s OR description ILIKE %s OR location ILIKE %s)"
            )
            search_term = f"%{search_keyword}%"
            query_params.extend([search_term, search_term, search_term])

        # Add filters to query
        if filters:
            base_query += " AND " + " AND ".join(filters)

        # Sorting
        sort_by = params.get("sort_by", "posted_date")
        sort_order = params.get("sort_order", "desc")

        valid_sort_fields = [
            "price",
            "area",
            "price_per_m2",
            "posted_date",
            "bedroom",
            "bathroom",
        ]
        if sort_by in valid_sort_fields:
            base_query += f" ORDER BY {sort_by} {sort_order.upper()}"
        else:
            base_query += " ORDER BY posted_date DESC"

        # Pagination
        page = int(params.get("page", 1))
        page_size = min(int(params.get("page_size", 20)), 100)  # Max 100 items per page
        offset = (page - 1) * page_size

        base_query += f" LIMIT {page_size} OFFSET {offset}"

        # Execute query
        with connection.cursor() as cursor:
            cursor.execute(base_query, query_params)
            results = cursor.fetchall()

            # Get column names
            columns = [col[0] for col in cursor.description]

            # Convert to dict
            properties = []
            for row in results:
                property_dict = dict(zip(columns, row))
                # Format price
                if property_dict.get("price"):
                    property_dict["price_formatted"] = (
                        f"{property_dict['price']:,.0f} VND"
                    )
                properties.append(property_dict)

        # Get total count for pagination
        count_query = "SELECT COUNT(*) FROM properties WHERE 1=1"
        if filters:
            count_query += " AND " + " AND ".join(filters)

        with connection.cursor() as cursor:
            cursor.execute(count_query, query_params)
            total_count = cursor.fetchone()[0]

        total_pages = (total_count + page_size - 1) // page_size

        return Response(
            {
                "success": True,
                "results": properties,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1,
                },
                "search_params": dict(params),
            }
        )

    except Exception as e:
        logger.error(f"Error in advanced search: {e}")
        return Response(
            {"error": f"Search failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def search_by_location(request):
    """
    Search properties by location (province/district/ward)
    GET /api/search/location/
    """
    try:
        province = request.GET.get("province")
        district = request.GET.get("district")
        ward = request.GET.get("ward")

        if not province:
            return Response(
                {"error": "Province parameter is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = """
        SELECT
            id, title, price, area, price_per_m2,
            province, district, ward, street, location,
            bedroom, bathroom, house_type,
            posted_date, url
        FROM properties
        WHERE province ILIKE %s
        """

        params = [f"%{province}%"]

        if district:
            query += " AND district ILIKE %s"
            params.append(f"%{district}%")

        if ward:
            query += " AND ward ILIKE %s"
            params.append(f"%{ward}%")

        query += " ORDER BY posted_date DESC LIMIT 50"

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            properties = []
            for row in results:
                property_dict = dict(zip(columns, row))
                if property_dict.get("price"):
                    property_dict["price_formatted"] = (
                        f"{property_dict['price']:,.0f} VND"
                    )
                properties.append(property_dict)

        return Response(
            {
                "success": True,
                "results": properties,
                "location": {"province": province, "district": district, "ward": ward},
            }
        )

    except Exception as e:
        logger.error(f"Error in location search: {e}")
        return Response(
            {"error": f"Location search failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def search_by_radius(request):
    """
    Search properties within a radius from a point
    GET /api/search/radius/
    """
    try:
        lat = request.GET.get("latitude")
        lng = request.GET.get("longitude")
        radius = request.GET.get("radius", 5000)  # Default 5km

        if not lat or not lng:
            return Response(
                {"error": "Latitude and longitude parameters are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        lat = float(lat)
        lng = float(lng)
        radius = float(radius)

        # Use Haversine formula to find properties within radius
        query = """
        SELECT
            id, title, price, area, price_per_m2,
            province, district, ward, street,
            latitude, longitude,
            bedroom, bathroom, house_type,
            posted_date, url,
            (6371000 * acos(cos(radians(%s)) * cos(radians(latitude)) *
             cos(radians(longitude) - radians(%s)) +
             sin(radians(%s)) * sin(radians(latitude)))) AS distance
        FROM properties
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        HAVING distance <= %s
        ORDER BY distance
        LIMIT 50
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [lat, lng, lat, radius])
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            properties = []
            for row in results:
                property_dict = dict(zip(columns, row))
                if property_dict.get("price"):
                    property_dict["price_formatted"] = (
                        f"{property_dict['price']:,.0f} VND"
                    )
                if property_dict.get("distance"):
                    property_dict["distance_formatted"] = (
                        f"{property_dict['distance']:.0f}m"
                    )
                properties.append(property_dict)

        return Response(
            {
                "success": True,
                "results": properties,
                "search_center": {"latitude": lat, "longitude": lng, "radius": radius},
            }
        )

    except Exception as e:
        logger.error(f"Error in radius search: {e}")
        return Response(
            {"error": f"Radius search failed: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def search_suggestions(request):
    """
    Get search suggestions for autocomplete
    GET /api/search/suggestions/
    """
    try:
        query = request.GET.get("q", "")
        suggestion_type = request.GET.get(
            "type", "all"
        )  # all, province, district, street

        if len(query) < 2:
            return Response({"suggestions": []})

        suggestions = []

        if suggestion_type in ["all", "province"]:
            # Province suggestions
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT province, COUNT(*) as count
                    FROM properties
                    WHERE province ILIKE %s
                    GROUP BY province
                    ORDER BY count DESC
                    LIMIT 5
                """,
                    [f"%{query}%"],
                )

                for row in cursor.fetchall():
                    suggestions.append(
                        {"type": "province", "text": row[0], "count": row[1]}
                    )

        if suggestion_type in ["all", "district"]:
            # District suggestions
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT district, province, COUNT(*) as count
                    FROM properties
                    WHERE district ILIKE %s
                    GROUP BY district, province
                    ORDER BY count DESC
                    LIMIT 5
                """,
                    [f"%{query}%"],
                )

                for row in cursor.fetchall():
                    suggestions.append(
                        {
                            "type": "district",
                            "text": f"{row[0]}, {row[1]}",
                            "count": row[2],
                        }
                    )

        if suggestion_type in ["all", "street"]:
            # Street suggestions
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT street, district, COUNT(*) as count
                    FROM properties
                    WHERE street ILIKE %s AND street IS NOT NULL
                    GROUP BY street, district
                    ORDER BY count DESC
                    LIMIT 5
                """,
                    [f"%{query}%"],
                )

                for row in cursor.fetchall():
                    suggestions.append(
                        {
                            "type": "street",
                            "text": f"{row[0]}, {row[1]}",
                            "count": row[2],
                        }
                    )

        return Response(
            {"suggestions": suggestions[:10]}  # Limit to 10 total suggestions
        )

    except Exception as e:
        logger.error(f"Error getting suggestions: {e}")
        return Response(
            {"error": f"Failed to get suggestions: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def property_detail(request, property_id):
    """
    Get detailed information for a specific property
    GET /api/search/property/{property_id}/
    """
    try:
        query = """
        SELECT * FROM properties WHERE id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [property_id])
            result = cursor.fetchone()

            if not result:
                return Response(
                    {"error": "Property not found"}, status=status.HTTP_404_NOT_FOUND
                )

            columns = [col[0] for col in cursor.description]
            property_data = dict(zip(columns, result))

            # Format price
            if property_data.get("price"):
                property_data["price_formatted"] = f"{property_data['price']:,.0f} VND"

        # Get similar properties
        similar_query = """
        SELECT id, title, price, area, price_per_m2
        FROM properties
        WHERE id != %s
          AND province = %s
          AND district = %s
          AND house_type = %s
          AND price BETWEEN %s AND %s
        ORDER BY ABS(price - %s)
        LIMIT 5
        """

        price = property_data.get("price", 0)
        price_range = price * 0.2  # ±20% price range

        with connection.cursor() as cursor:
            cursor.execute(
                similar_query,
                [
                    property_id,
                    property_data.get("province"),
                    property_data.get("district"),
                    property_data.get("house_type"),
                    price - price_range,
                    price + price_range,
                    price,
                ],
            )

            similar_results = cursor.fetchall()
            similar_columns = [col[0] for col in cursor.description]

            similar_properties = []
            for row in similar_results:
                similar_dict = dict(zip(similar_columns, row))
                if similar_dict.get("price"):
                    similar_dict["price_formatted"] = (
                        f"{similar_dict['price']:,.0f} VND"
                    )
                similar_properties.append(similar_dict)

        return Response(
            {
                "success": True,
                "property": property_data,
                "similar_properties": similar_properties,
            }
        )

    except Exception as e:
        logger.error(f"Error getting property detail: {e}")
        return Response(
            {"error": f"Failed to get property detail: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
