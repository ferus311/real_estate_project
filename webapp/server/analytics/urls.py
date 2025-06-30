from django.urls import path
from . import views
from . import analytics_views

urlpatterns = [
    # Market analytics
    path("market-overview/", views.market_overview, name="market_overview"),
    path("price-distribution/", views.price_distribution, name="price_distribution"),
    path("property-type-stats/", views.property_type_stats, name="property_type_stats"),
    # Location analytics
    path("location-stats/", views.location_stats, name="location_stats"),
    # Time-based analytics
    path("price-trends/", views.price_trends, name="price_trends"),
    # Area analytics
    path("area-distribution/", views.area_distribution, name="area_distribution"),
    # Dashboard
    path("dashboard-summary/", views.dashboard_summary, name="dashboard_summary"),
    # New enhanced analytics APIs
    path(
        "price_distribution_by_location/",
        analytics_views.get_price_distribution_by_location,
        name="price_distribution_by_location",
    ),
    path(
        "market_overview_enhanced/",
        analytics_views.get_market_overview,
        name="market_overview_enhanced",
    ),
    path(
        "district_comparison/",
        analytics_views.get_district_comparison,
        name="district_comparison",
    ),
    path(
        "refresh_model/",
        analytics_views.refresh_model_status,
        name="refresh_model_status",
    ),
]
