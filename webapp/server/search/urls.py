from django.urls import path
from . import views

urlpatterns = [
    # Advanced search
    path("advanced/", views.advanced_search, name="advanced_search"),
    # Location-based search
    path("location/", views.search_by_location, name="search_by_location"),
    # Radius search
    path("radius/", views.search_by_radius, name="search_by_radius"),
    # Search suggestions/autocomplete
    path("suggestions/", views.search_suggestions, name="search_suggestions"),
    # Property detail
    path("property/<str:property_id>/", views.property_detail, name="property_detail"),
]
