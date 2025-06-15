from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import JsonResponse


@api_view(["GET"])
def api_info(request):
    """
    API information and status
    GET /api/info/
    """
    return Response(
        {
            "success": True,
            "message": "Real Estate API is running",
            "version": "1.0.0",
            "features": {
                "prediction": {
                    "description": "Price prediction using ML models",
                    "endpoints": [
                        "/api/prediction/xgboost/",
                        "/api/prediction/lightgbm/",
                        "/api/prediction/all-models/",
                        "/api/prediction/model-info/",
                        "/api/prediction/feature-info/",
                        "/api/prediction/all-models-info/",
                    ],
                },
                "search": {
                    "description": "Advanced property search",
                    "endpoints": [
                        "/api/search/advanced/",
                        "/api/search/location/",
                        "/api/search/radius/",
                        "/api/search/suggestions/",
                        "/api/search/property/{id}/",
                    ],
                },
                "analytics": {
                    "description": "Market analytics and statistics",
                    "endpoints": [
                        "/api/analytics/market-overview/",
                        "/api/analytics/price-distribution/",
                        "/api/analytics/property-type-stats/",
                        "/api/analytics/location-stats/",
                        "/api/analytics/price-trends/",
                        "/api/analytics/area-distribution/",
                        "/api/analytics/dashboard-summary/",
                    ],
                },
            },
            "documentation": {
                "prediction_example": {
                    "url": "/api/prediction/ensemble/",
                    "method": "POST",
                    "body": {
                        "area": 80.0,
                        "latitude": 10.762622,
                        "longitude": 106.660172,
                        "bedroom": 3,
                        "bathroom": 2,
                        "floor_count": 3,
                        "house_direction_code": 3,
                        "legal_status_code": 1,
                        "interior_code": 2,
                        "province_id": 79,
                        "district_id": 769,
                        "ward_id": 27000,
                    },
                },
                "search_example": {
                    "url": "/api/search/advanced/?province=Hồ Chí Minh&price_min=2000000000&price_max=5000000000&bedroom_min=2",
                    "method": "GET",
                },
                "analytics_example": {
                    "url": "/api/analytics/market-overview/?province=Hà Nội",
                    "method": "GET",
                },
            },
        }
    )


@api_view(["GET"])
def health_check(request):
    """
    Health check endpoint
    GET /api/health/
    """
    try:
        from django.db import connection

        # Test database connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            db_status = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return Response(
        {"status": "healthy", "database": db_status, "timestamp": "2025-06-15"}
    )
