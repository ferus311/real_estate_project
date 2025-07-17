"""
Analytics API for Real Estate Price Analysis
Provides price distribution, trends, and market insights by location
"""

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Q, Avg, Count, Min, Max, StdDev, FloatField
from django.http import JsonResponse
import logging
from datetime import datetime, timedelta
import json

from properties.models import Property
from prediction.simple_ml_service import get_simple_model_loader

logger = logging.getLogger(__name__)


@api_view(["GET"])
def get_price_distribution_by_location(request):
    """
    API endpoint to get price distribution by province/district
    GET /api/analytics/price_distribution/?province_id=79&district_id=769
    """
    try:
        province_id = request.GET.get("province_id")
        district_id = request.GET.get("district_id")

        if not province_id:
            return Response(
                {"error": "province_id is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        # Base filter
        filters = Q(province_id=province_id)

        # Add district filter if provided
        if district_id:
            filters &= Q(district_id=district_id)

        # Get properties with valid prices
        properties = (
            Property.objects.filter(filters, price__gt=0, area__gt=0)
            .exclude(price__isnull=True)
            .exclude(area__isnull=True)
        )

        if not properties.exists():
            return Response(
                {
                    "message": "No properties found for the specified location",
                    "province_id": province_id,
                    "district_id": district_id,
                    "data": {
                        "total_properties": 0,
                        "price_distribution": [],
                        "price_per_m2_distribution": [],
                    },
                },
                status=status.HTTP_200_OK,
            )

        # Calculate basic statistics
        stats = properties.aggregate(
            total_count=Count("id"),
            avg_price=Avg("price"),
            min_price=Min("price"),
            max_price=Max("price"),
            std_price=StdDev("price"),
            avg_area=Avg("area"),
        )

        # Calculate smart price distribution (total price in billions)
        min_price = float(stats["min_price"])
        max_price = float(stats["max_price"])

        # Convert to billions for better readability
        min_price_billions = min_price / 1_000_000_000
        max_price_billions = max_price / 1_000_000_000

        # Smart binning for total price based on typical real estate prices (in billions VND)
        price_distribution = []

        if max_price_billions <= 10:  # Low-end market
            ranges = [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]
        elif max_price_billions <= 20:  # Mid-range market
            ranges = [(0, 3), (3, 6), (6, 10), (10, 15), (15, 20)]
        elif max_price_billions <= 50:  # High-end market
            ranges = [(0, 5), (5, 10), (10, 20), (20, 35), (35, 50)]
        else:  # Luxury market
            ranges = [(0, 10), (10, 25), (25, 50), (50, 100), (100, float("inf"))]

        # Filter ranges to only include those with data
        for range_min, range_max in ranges:
            # Convert back to VND for filtering
            bin_min = range_min * 1_000_000_000
            bin_max = (
                range_max * 1_000_000_000 if range_max != float("inf") else float("inf")
            )

            if bin_max == float("inf"):
                count = properties.filter(price__gte=bin_min).count()
            else:
                count = properties.filter(price__gte=bin_min, price__lt=bin_max).count()

            # Only include ranges that have properties
            if count > 0:
                # Format range display in billions
                if range_max == float("inf"):
                    range_text = f"Trên {range_min:.0f} tỷ VND"
                else:
                    range_text = f"{range_min:.0f} - {range_max:.0f} tỷ VND"

                price_distribution.append(
                    {
                        "range": range_text,
                        "min_price": bin_min,
                        "max_price": bin_max if bin_max != float("inf") else max_price,
                        "count": count,
                        "percentage": (
                            round((count / stats["total_count"]) * 100, 1)
                            if stats["total_count"] > 0
                            else 0
                        ),
                    }
                )

        # Calculate price per m2 distribution with smart binning
        properties_with_calc = []
        for prop in properties:
            if prop.area and prop.area > 0:
                price_per_m2 = prop.price / prop.area
                properties_with_calc.append(price_per_m2)

        if properties_with_calc:
            properties_with_calc.sort()
            min_price_m2 = min(properties_with_calc)
            max_price_m2 = max(properties_with_calc)

            # Convert to millions for better readability
            min_price_m2_millions = min_price_m2 / 1_000_000
            max_price_m2_millions = max_price_m2 / 1_000_000

            # Smart binning - create meaningful price ranges
            price_per_m2_distribution = []

            # Define smart price ranges based on typical real estate prices (in millions VND/m²)
            if max_price_m2_millions <= 50:  # Low-end market
                ranges = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50)]
            elif max_price_m2_millions <= 100:  # Mid-range market
                ranges = [(0, 15), (15, 30), (30, 50), (50, 70), (70, 100)]
            elif max_price_m2_millions <= 200:  # High-end market
                ranges = [(0, 30), (30, 60), (60, 100), (100, 150), (150, 200)]
            else:  # Luxury market
                ranges = [
                    (0, 50),
                    (50, 100),
                    (100, 200),
                    (200, 300),
                    (300, float("inf")),
                ]

            # Filter ranges to only include those with data
            for range_min, range_max in ranges:
                # Convert back to VND for filtering
                bin_min = range_min * 1_000_000
                bin_max = (
                    range_max * 1_000_000 if range_max != float("inf") else float("inf")
                )

                # Get values in this bin
                if bin_max == float("inf"):
                    bin_values = [p for p in properties_with_calc if p >= bin_min]
                else:
                    bin_values = [
                        p for p in properties_with_calc if bin_min <= p < bin_max
                    ]

                count = len(bin_values)

                # Only include ranges that have properties
                if count > 0:
                    avg_price_per_m2 = sum(bin_values) / count

                    # Format range display in millions
                    if range_max == float("inf"):
                        range_text = f"Trên {range_min:.0f} triệu VND/m²"
                    else:
                        range_text = f"{range_min:.0f} - {range_max:.0f} triệu VND/m²"

                    price_per_m2_distribution.append(
                        {
                            "range": range_text,
                            "min_price_per_m2": bin_min,
                            "max_price_per_m2": (
                                bin_max if bin_max != float("inf") else max_price_m2
                            ),
                            "avg_price_per_m2": avg_price_per_m2,
                            "count": count,
                            "percentage": round(
                                (count / len(properties_with_calc)) * 100, 1
                            ),
                        }
                    )
        else:
            price_per_m2_distribution = []

        # Get location info
        location_info = {"province_id": province_id, "district_id": district_id}

        # Try to get location names from first property
        first_prop = properties.first()
        if first_prop:
            location_info.update(
                {
                    "province_name": getattr(first_prop, "province_name", "Unknown"),
                    "district_name": getattr(first_prop, "district_name", "Unknown"),
                }
            )

        return Response(
            {
                "message": "Price distribution data retrieved successfully",
                "location": location_info,
                "data": {
                    "total_properties": stats["total_count"],
                    "statistics": {
                        "avg_price": (
                            float(stats["avg_price"]) if stats["avg_price"] else 0
                        ),
                        "min_price": (
                            float(stats["min_price"]) if stats["min_price"] else 0
                        ),
                        "max_price": (
                            float(stats["max_price"]) if stats["max_price"] else 0
                        ),
                        "std_price": (
                            float(stats["std_price"]) if stats["std_price"] else 0
                        ),
                        "avg_area": (
                            float(stats["avg_area"]) if stats["avg_area"] else 0
                        ),
                        "avg_price_per_m2": (
                            (float(stats["avg_price"]) / float(stats["avg_area"]))
                            if stats["avg_price"] and stats["avg_area"]
                            else 0
                        ),
                    },
                    "price_distribution": price_distribution,
                    "price_per_m2_distribution": price_per_m2_distribution,
                },
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in get_price_distribution_by_location: {e}")
        return Response(
            {"error": f"Failed to get price distribution: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_market_overview(request):
    """
    API endpoint to get overall market overview
    GET /api/analytics/market_overview/
    """
    try:
        # Get all properties with valid data
        properties = Property.objects.filter(price__gt=0, area__gt=0).exclude(
            price__isnull=True, area__isnull=True
        )

        if not properties.exists():
            return Response(
                {
                    "message": "No properties found",
                    "data": {
                        "total_properties": 0,
                        "top_provinces": [],
                        "price_ranges": [],
                    },
                },
                status=status.HTTP_200_OK,
            )

        # Overall statistics
        total_stats = properties.aggregate(
            total_count=Count("id"),
            avg_price=Avg("price"),
            min_price=Min("price"),
            max_price=Max("price"),
            avg_area=Avg("area"),
        )

        # Top provinces by property count
        top_provinces = (
            properties.values("province_id", "province_name")
            .annotate(
                property_count=Count("id"),
                avg_price=Avg("price"),
                avg_area=Avg("area"),
            )
            .order_by("-property_count")[:10]
        )

        # Smart price ranges analysis based on market data
        min_price = float(total_stats["min_price"])
        max_price = float(total_stats["max_price"])
        max_price_billions = max_price / 1_000_000_000

        # Define smart ranges based on actual market conditions
        if max_price_billions <= 15:  # Low-end market
            price_ranges_def = [
                {"range": "Dưới 2 tỷ", "min": 0, "max": 2_000_000_000},
                {"range": "2-4 tỷ", "min": 2_000_000_000, "max": 4_000_000_000},
                {"range": "4-7 tỷ", "min": 4_000_000_000, "max": 7_000_000_000},
                {"range": "7-12 tỷ", "min": 7_000_000_000, "max": 12_000_000_000},
                {"range": "Trên 12 tỷ", "min": 12_000_000_000, "max": float("inf")},
            ]
        elif max_price_billions <= 30:  # Mid-range market
            price_ranges_def = [
                {"range": "Dưới 3 tỷ", "min": 0, "max": 3_000_000_000},
                {"range": "3-6 tỷ", "min": 3_000_000_000, "max": 6_000_000_000},
                {"range": "6-12 tỷ", "min": 6_000_000_000, "max": 12_000_000_000},
                {"range": "12-20 tỷ", "min": 12_000_000_000, "max": 20_000_000_000},
                {"range": "Trên 20 tỷ", "min": 20_000_000_000, "max": float("inf")},
            ]
        else:  # High-end market
            price_ranges_def = [
                {"range": "Dưới 5 tỷ", "min": 0, "max": 5_000_000_000},
                {"range": "5-15 tỷ", "min": 5_000_000_000, "max": 15_000_000_000},
                {"range": "15-30 tỷ", "min": 15_000_000_000, "max": 30_000_000_000},
                {"range": "30-50 tỷ", "min": 30_000_000_000, "max": 50_000_000_000},
                {"range": "Trên 50 tỷ", "min": 50_000_000_000, "max": float("inf")},
            ]

        # Only include ranges that have data
        price_ranges = []
        for price_range in price_ranges_def:
            if price_range["max"] == float("inf"):
                count = properties.filter(price__gte=price_range["min"]).count()
            else:
                count = properties.filter(
                    price__gte=price_range["min"], price__lt=price_range["max"]
                ).count()

            # Only include ranges with properties
            if count > 0:
                price_ranges.append(
                    {
                        "range": price_range["range"],
                        "min": price_range["min"],
                        "max": price_range["max"],
                        "count": count,
                        "percentage": (
                            round((count / total_stats["total_count"]) * 100, 1)
                            if total_stats["total_count"] > 0
                            else 0
                        ),
                    }
                )

        return Response(
            {
                "message": "Market overview retrieved successfully",
                "data": {
                    "total_properties": total_stats["total_count"],
                    "overall_statistics": {
                        "avg_price": (
                            float(total_stats["avg_price"])
                            if total_stats["avg_price"]
                            else 0
                        ),
                        "min_price": (
                            float(total_stats["min_price"])
                            if total_stats["min_price"]
                            else 0
                        ),
                        "max_price": (
                            float(total_stats["max_price"])
                            if total_stats["max_price"]
                            else 0
                        ),
                        "avg_area": (
                            float(total_stats["avg_area"])
                            if total_stats["avg_area"]
                            else 0
                        ),
                        "avg_price_per_m2": (
                            (
                                float(total_stats["avg_price"])
                                / float(total_stats["avg_area"])
                            )
                            if total_stats["avg_price"] and total_stats["avg_area"]
                            else 0
                        ),
                    },
                    "top_provinces": [
                        {
                            "province_id": item["province_id"],
                            "province_name": item["province_name"]
                            or f"Province {item['province_id']}",
                            "property_count": item["property_count"],
                            "avg_price": (
                                float(item["avg_price"]) if item["avg_price"] else 0
                            ),
                            "avg_price_per_m2": (
                                (float(item["avg_price"]) / float(item["avg_area"]))
                                if item["avg_price"]
                                and item["avg_area"]
                                and item["avg_area"] > 0
                                else 0
                            ),
                        }
                        for item in top_provinces
                    ],
                    "price_ranges": price_ranges,
                },
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in get_market_overview: {e}")
        return Response(
            {"error": f"Failed to get market overview: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["GET"])
def get_district_comparison(request):
    """
    API endpoint to compare districts within a province
    GET /api/analytics/district_comparison/?province_id=79
    """
    try:
        province_id = request.GET.get("province_id")

        if not province_id:
            return Response(
                {"error": "province_id is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        # Get districts in the province
        districts = (
            Property.objects.filter(province_id=province_id, price__gt=0, area__gt=0)
            .exclude(price__isnull=True, area__isnull=True)
            .values("district_id", "district_name")
            .annotate(
                property_count=Count("id"),
                avg_price=Avg("price"),
                min_price=Min("price"),
                max_price=Max("price"),
                avg_area=Avg("area"),
            )
            .order_by("-avg_price")
        )

        if not districts.exists():
            return Response(
                {
                    "message": f"No districts found for province {province_id}",
                    "province_id": province_id,
                    "data": [],
                },
                status=status.HTTP_200_OK,
            )

        # Format the data
        district_comparison = []
        for district in districts:
            district_comparison.append(
                {
                    "district_id": district["district_id"],
                    "district_name": district["district_name"]
                    or f"District {district['district_id']}",
                    "property_count": district["property_count"],
                    "avg_price": (
                        float(district["avg_price"]) if district["avg_price"] else 0
                    ),
                    "min_price": (
                        float(district["min_price"]) if district["min_price"] else 0
                    ),
                    "max_price": (
                        float(district["max_price"]) if district["max_price"] else 0
                    ),
                    "avg_area": (
                        float(district["avg_area"]) if district["avg_area"] else 0
                    ),
                    "avg_price_per_m2": (
                        (float(district["avg_price"]) / float(district["avg_area"]))
                        if district["avg_price"]
                        and district["avg_area"]
                        and district["avg_area"] > 0
                        else 0
                    ),
                }
            )

        return Response(
            {
                "message": "District comparison retrieved successfully",
                "province_id": province_id,
                "data": district_comparison,
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in get_district_comparison: {e}")
        return Response(
            {"error": f"Failed to get district comparison: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


@api_view(["POST"])
def refresh_model_status(request):
    """
    API endpoint to refresh ML model status and reload if needed
    POST /api/analytics/refresh_model/
    """
    try:
        model_loader = get_simple_model_loader()

        # Get current model info
        current_info = model_loader.get_current_model_info()

        # Try to reload models
        reload_result = model_loader.reload_models(force_reload=True)

        return Response(
            {
                "message": "Model refresh completed",
                "data": {
                    "before_refresh": {
                        "loaded_models": list(
                            current_info.get("loaded_models", {}).keys()
                        ),
                        "current_path": current_info.get("current_model_path"),
                        "hdfs_available": current_info.get("hdfs_available", False),
                    },
                    "refresh_result": reload_result,
                    "after_refresh": model_loader.get_current_model_info(),
                    "refresh_time": datetime.now().isoformat(),
                },
            },
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.error(f"❌ Error in refresh_model_status: {e}")
        return Response(
            {"error": f"Failed to refresh model: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
