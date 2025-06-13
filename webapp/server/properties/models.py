from django.db import models


class Province(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    code = models.CharField(max_length=10, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False  # Don't let Django manage this table
        db_table = "provinces"  # Use exact table name from schema


class District(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    province = models.ForeignKey(Province, on_delete=models.CASCADE)
    code = models.CharField(max_length=10, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "districts"


class Ward(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    code = models.CharField(max_length=10, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "wards"


class Street(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=200)
    ward = models.ForeignKey(Ward, on_delete=models.CASCADE, null=True, blank=True)
    district = models.ForeignKey(
        District, on_delete=models.CASCADE, null=True, blank=True
    )
    province = models.ForeignKey(
        Province, on_delete=models.CASCADE, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "streets"


class Property(models.Model):
    # Primary Key - match với Gold schema
    id = models.CharField(max_length=255, primary_key=True)

    # Source Info
    source = models.CharField(max_length=50)
    url = models.TextField(null=True, blank=True)

    # Basic Info
    title = models.TextField()
    description = models.TextField(null=True, blank=True)
    location = models.TextField(null=True, blank=True)
    data_type = models.CharField(max_length=50, null=True, blank=True)

    # Location References
    province_id = models.IntegerField(null=True, blank=True)
    district_id = models.IntegerField(null=True, blank=True)
    ward_id = models.IntegerField(null=True, blank=True)
    street_id = models.IntegerField(null=True, blank=True)

    # Location Text Fields
    province = models.CharField(max_length=100, null=True, blank=True)
    district = models.CharField(max_length=100, null=True, blank=True)
    ward = models.CharField(max_length=100, null=True, blank=True)
    street = models.CharField(max_length=200, null=True, blank=True)

    # Geographic
    latitude = models.DecimalField(
        max_digits=10, decimal_places=8, null=True, blank=True
    )
    longitude = models.DecimalField(
        max_digits=11, decimal_places=8, null=True, blank=True
    )

    # Property Details - match exactly với Django schema
    area = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    living_size = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    width = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    length = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    facade_width = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    road_width = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    floor_count = models.IntegerField(null=True, blank=True)
    bedroom = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    bathroom = models.DecimalField(
        max_digits=8, decimal_places=2, null=True, blank=True
    )

    # Pricing
    price = models.BigIntegerField(null=True, blank=True)
    price_per_m2 = models.BigIntegerField(null=True, blank=True)

    # Property Characteristics
    legal_status = models.CharField(max_length=100, null=True, blank=True)
    legal_status_code = models.IntegerField(null=True, blank=True)
    house_direction = models.CharField(max_length=50, null=True, blank=True)
    house_direction_code = models.IntegerField(null=True, blank=True)
    interior = models.CharField(max_length=100, null=True, blank=True)
    interior_code = models.IntegerField(null=True, blank=True)
    house_type = models.CharField(max_length=100, null=True, blank=True)
    house_type_code = models.IntegerField(null=True, blank=True)

    # Timestamps
    posted_date = models.DateTimeField(null=True, blank=True)
    crawl_timestamp = models.DateTimeField(null=True, blank=True)
    processing_timestamp = models.DateTimeField(null=True, blank=True)

    # Processing metadata
    data_quality_score = models.DecimalField(
        max_digits=3, decimal_places=2, null=True, blank=True
    )
    processing_id = models.CharField(max_length=255, null=True, blank=True)

    # Serving layer metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False  # Quan trọng: Không để Django manage table này
        db_table = "properties"  # Exact table name from schema

    def __str__(self):
        return f"{self.title} - {self.source}"

    @property
    def full_address(self):
        """Helper property to get full address"""
        parts = [self.street, self.ward, self.district, self.province]
        return ", ".join([p for p in parts if p])

    @property
    def price_formatted(self):
        """Helper property to format price"""
        if self.price:
            if self.price >= 1_000_000_000:  # >= 1 tỷ
                return f"{self.price / 1_000_000_000:.1f} tỷ"
            elif self.price >= 1_000_000:  # >= 1 triệu
                return f"{self.price / 1_000_000:.0f} triệu"
            else:
                return f"{self.price:,} VND"
        return None
