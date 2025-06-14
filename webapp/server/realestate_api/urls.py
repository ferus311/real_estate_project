"""
URL configuration for realestate_api project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path, include
from . import views

urlpatterns = [
    path("admin/", admin.site.urls),
    # API info and health check
    path("api/info/", views.api_info, name="api_info"),
    path("api/health/", views.health_check, name="health_check"),
    # Core properties API
    path("api/properties/", include("properties.urls")),
    # Prediction APIs - 4 models as requested
    path("api/prediction/", include("prediction.urls")),
    # Search APIs
    path("api/search/", include("search.urls")),
    # Analytics APIs
    path("api/analytics/", include("analytics.urls")),
    # Legacy compatibility
    path("", include("properties.urls")),
]
