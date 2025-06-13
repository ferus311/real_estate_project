from rest_framework import serializers
from .models import Property, Province, District, Ward, Street


class ProvinceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Province
        fields = ["id", "name", "code"]


class DistrictSerializer(serializers.ModelSerializer):
    province = ProvinceSerializer(read_only=True)

    class Meta:
        model = District
        fields = ["id", "name", "code", "province"]


class WardSerializer(serializers.ModelSerializer):
    district = DistrictSerializer(read_only=True)

    class Meta:
        model = Ward
        fields = ["id", "name", "code", "district"]


class StreetSerializer(serializers.ModelSerializer):
    ward = WardSerializer(read_only=True)
    district = DistrictSerializer(read_only=True)
    province = ProvinceSerializer(read_only=True)

    class Meta:
        model = Street
        fields = ["id", "name", "ward", "district", "province"]


class PropertyListSerializer(serializers.ModelSerializer):
    """Serializer cho danh sách properties (ít thông tin)"""

    full_address = serializers.ReadOnlyField()
    price_formatted = serializers.ReadOnlyField()

    class Meta:
        model = Property
        fields = [
            "id",
            "title",
            "source",
            "price",
            "price_formatted",
            "area",
            "province",
            "district",
            "ward",
            "street",
            "full_address",
            "house_type",
            "bedroom",
            "bathroom",
            "floor_count",
            "created_at",
        ]


class PropertyDetailSerializer(serializers.ModelSerializer):
    """Serializer cho chi tiết property (đầy đủ thông tin)"""

    full_address = serializers.ReadOnlyField()
    price_formatted = serializers.ReadOnlyField()

    class Meta:
        model = Property
        fields = "__all__"  # Tất cả fields cho detail view


class PropertySearchSerializer(serializers.Serializer):
    """Serializer cho search parameters"""

    # Location filters
    province_id = serializers.IntegerField(required=False)
    district_id = serializers.IntegerField(required=False)
    ward_id = serializers.IntegerField(required=False)
    street_id = serializers.IntegerField(required=False)

    # Property filters
    house_type_code = serializers.IntegerField(required=False)
    legal_status_code = serializers.IntegerField(required=False)
    house_direction_code = serializers.IntegerField(required=False)
    interior_code = serializers.IntegerField(required=False)

    # Price filters
    min_price = serializers.IntegerField(required=False)
    max_price = serializers.IntegerField(required=False)

    # Area filters
    min_area = serializers.FloatField(required=False)
    max_area = serializers.FloatField(required=False)

    # Room filters
    min_bedroom = serializers.FloatField(required=False)
    max_bedroom = serializers.FloatField(required=False)
    min_bathroom = serializers.FloatField(required=False)
    max_bathroom = serializers.FloatField(required=False)

    # Text search
    search = serializers.CharField(required=False, max_length=200)

    # Sorting
    ordering = serializers.ChoiceField(
        choices=[
            "price",
            "-price",
            "area",
            "-area",
            "created_at",
            "-created_at",
            "title",
        ],
        required=False,
        default="-created_at",
    )
