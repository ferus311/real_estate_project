from django.urls import path
from . import views

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
]
