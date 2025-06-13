from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r"properties", views.PropertyViewSet)
router.register(r"provinces", views.ProvinceViewSet)
router.register(r"districts", views.DistrictViewSet)
router.register(r"wards", views.WardViewSet)
router.register(r"streets", views.StreetViewSet)

urlpatterns = [
    path("api/", include(router.urls)),
]
