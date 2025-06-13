from rest_framework import viewsets, filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django.db.models import Q, Avg
import django_filters

from .models import Property, Province, District, Ward, Street
from .serializers import (
    PropertyListSerializer,
    PropertyDetailSerializer,
    PropertySearchSerializer,
    ProvinceSerializer,
    DistrictSerializer,
    WardSerializer,
    StreetSerializer,
)


class PropertyFilter(django_filters.FilterSet):
    """Advanced filtering cho Property"""

    # Price range
    min_price = django_filters.NumberFilter(field_name="price", lookup_expr="gte")
    max_price = django_filters.NumberFilter(field_name="price", lookup_expr="lte")

    # Area range
    min_area = django_filters.NumberFilter(field_name="area", lookup_expr="gte")
    max_area = django_filters.NumberFilter(field_name="area", lookup_expr="lte")

    # Room count range
    min_bedroom = django_filters.NumberFilter(field_name="bedroom", lookup_expr="gte")
    max_bedroom = django_filters.NumberFilter(field_name="bedroom", lookup_expr="lte")
    min_bathroom = django_filters.NumberFilter(field_name="bathroom", lookup_expr="gte")
    max_bathroom = django_filters.NumberFilter(field_name="bathroom", lookup_expr="lte")

    # Location filters
    province_id = django_filters.NumberFilter(field_name="province_id")
    district_id = django_filters.NumberFilter(field_name="district_id")
    ward_id = django_filters.NumberFilter(field_name="ward_id")
    street_id = django_filters.NumberFilter(field_name="street_id")

    # Property type filters
    house_type_code = django_filters.NumberFilter(field_name="house_type_code")
    legal_status_code = django_filters.NumberFilter(field_name="legal_status_code")
    house_direction_code = django_filters.NumberFilter(
        field_name="house_direction_code"
    )
    interior_code = django_filters.NumberFilter(field_name="interior_code")

    # Text search
    search = django_filters.CharFilter(method="filter_search")

    class Meta:
        model = Property
        fields = [
            "source",
            "data_type",
            "province_id",
            "district_id",
            "ward_id",
            "street_id",
            "house_type_code",
            "legal_status_code",
            "house_direction_code",
            "interior_code",
            "min_price",
            "max_price",
            "min_area",
            "max_area",
            "min_bedroom",
            "max_bedroom",
            "min_bathroom",
            "max_bathroom",
        ]

    def filter_search(self, queryset, name, value):
        """Full-text search across title, description, and location"""
        if not value:
            return queryset

        return queryset.filter(
            Q(title__icontains=value)
            | Q(description__icontains=value)
            | Q(location__icontains=value)
            | Q(province__icontains=value)
            | Q(district__icontains=value)
            | Q(ward__icontains=value)
            | Q(street__icontains=value)
        )


class PropertyViewSet(viewsets.ReadOnlyModelViewSet):
    """API ViewSet cho Property - chỉ đọc, không write"""

    queryset = Property.objects.all()
    filter_backends = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]
    filterset_class = PropertyFilter
    search_fields = [
        "title",
        "description",
        "location",
        "province",
        "district",
        "ward",
        "street",
    ]
    ordering_fields = ["price", "area", "created_at", "processing_timestamp"]
    ordering = ["-created_at"]  # Default ordering

    def get_serializer_class(self):
        """Dùng serializer khác nhau cho list vs detail"""
        if self.action == "list":
            return PropertyListSerializer
        return PropertyDetailSerializer

    def get_queryset(self):
        """Tối ưu queryset cho performance"""
        queryset = Property.objects.all()

        # Chỉ lấy properties có thông tin cơ bản
        queryset = queryset.filter(title__isnull=False, source__isnull=False).exclude(
            title=""
        )

        return queryset

    @action(detail=False, methods=["get"])
    def stats(self, request):
        """Thống kê tổng quan"""
        queryset = self.get_queryset()

        stats = {
            "total_properties": queryset.count(),
            "with_price": queryset.filter(price__isnull=False).count(),
            "with_area": queryset.filter(area__isnull=False).count(),
            "sources": list(queryset.values_list("source", flat=True).distinct()),
            "avg_price": queryset.filter(price__isnull=False).aggregate(
                avg_price=Avg("price")
            )["avg_price"],
            "avg_area": queryset.filter(area__isnull=False).aggregate(
                avg_area=Avg("area")
            )["avg_area"],
        }

        return Response(stats)


class ProvinceViewSet(viewsets.ReadOnlyModelViewSet):
    """API cho Provinces"""

    queryset = Province.objects.all()
    serializer_class = ProvinceSerializer


class DistrictViewSet(viewsets.ReadOnlyModelViewSet):
    """API cho Districts"""

    queryset = District.objects.all()
    serializer_class = DistrictSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["province"]


class WardViewSet(viewsets.ReadOnlyModelViewSet):
    """API cho Wards"""

    queryset = Ward.objects.all()
    serializer_class = WardSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["district"]


class StreetViewSet(viewsets.ReadOnlyModelViewSet):
    """API cho Streets"""

    queryset = Street.objects.all()
    serializer_class = StreetSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["ward", "district", "province"]
