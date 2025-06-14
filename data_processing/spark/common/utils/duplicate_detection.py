"""
Duplicate Detection Module for Real Estate Data
Hỗ trợ cả UNIFY stage và LOAD TO SERVING stage
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    concat,
    md5,
    lower,
    trim,
    abs as spark_abs,
    max as spark_max,
    min as spark_min,
    row_number,
    desc,
    asc,
    count,
    collect_list,
    struct,
    explode,
    udf,
    broadcast,
    concat_ws,
    regexp_replace,
    round as spark_round,
    greatest,
    least,
    array_join,
    sort_array,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    ArrayType,
    StructType,
    StructField,
)
import hashlib
import math
from typing import Dict, List, Tuple, Optional

# ============================================================================
# CORE UTILITY FUNCTIONS (Shared across both stages)
# ============================================================================


def normalize_text(text: str) -> str:
    """Chuẩn hóa text cho comparison"""
    if not text:
        return ""
    return text.lower().strip().replace(" ", "").replace("-", "").replace("_", "")


def get_price_bucket(price: float, tolerance: float = 0.05) -> str:
    """Tạo price bucket với tolerance"""
    if not price or price <= 0:
        return "unknown"

    # Tạo range với tolerance
    lower_bound = price * (1 - tolerance)
    upper_bound = price * (1 + tolerance)

    # Convert to tỷ VND
    lower_ty = lower_bound / 1_000_000_000
    upper_ty = upper_bound / 1_000_000_000

    return f"{lower_ty:.1f}-{upper_ty:.1f}ty"


def get_area_bucket(area: float, tolerance: float = 10.0) -> str:
    """Tạo area bucket với tolerance (m²)"""
    if not area or area <= 0:
        return "unknown"

    lower_bound = max(0, area - tolerance)
    upper_bound = area + tolerance

    return f"{int(lower_bound)}-{int(upper_bound)}m2"


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Tính khoảng cách giữa 2 coordinates (meters) - Haversine formula"""
    if not all([lat1, lon1, lat2, lon2]):
        return float("inf")

    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    # Earth radius in meters
    return c * 6371000


# UDF Functions
normalize_text_udf = udf(normalize_text, StringType())
get_price_bucket_udf = udf(get_price_bucket, StringType())
get_area_bucket_udf = udf(get_area_bucket, StringType())
calculate_distance_udf = udf(calculate_distance, DoubleType())


# ============================================================================
# FINGERPRINT GENERATION (Shared logic)
# ============================================================================


def create_property_fingerprint_udf():
    """UDF để tạo property fingerprint cho exact matching"""

    def _create_fingerprint(record):
        """Internal function để tạo fingerprint"""
        import hashlib

        # Inline normalize_text function
        def normalize_text_inline(text):
            if not text:
                return ""
            return (
                text.lower().strip().replace(" ", "").replace("-", "").replace("_", "")
            )

        # Inline get_price_bucket function
        def get_price_bucket_inline(price, tolerance=0.05):
            if not price or price <= 0:
                return "unknown"
            lower_bound = price * (1 - tolerance)
            upper_bound = price * (1 + tolerance)
            lower_ty = lower_bound / 1_000_000_000
            upper_ty = upper_bound / 1_000_000_000
            return f"{lower_ty:.1f}-{upper_ty:.1f}ty"

        # Inline get_area_bucket function
        def get_area_bucket_inline(area, tolerance=10.0):
            if not area or area <= 0:
                return "unknown"
            lower_bound = max(0, area - tolerance)
            upper_bound = area + tolerance
            return f"{int(lower_bound)}-{int(upper_bound)}m2"

        # Location fingerprint
        location_parts = [
            record.province or "",
            record.district or "",
            record.ward or "",
            record.street or "",
        ]
        location_fp = normalize_text_inline("_".join(location_parts))

        # Price và area buckets
        price_bucket = get_price_bucket_inline(record.price or 0, tolerance=0.05)
        area_bucket = get_area_bucket_inline(record.area or 0, tolerance=10)

        # Room configuration
        bedroom = int(record.bedroom or 0)
        bathroom = int(record.bathroom or 0)
        rooms = f"{bedroom}br_{bathroom}ba"

        # Composite fingerprint
        composite = f"{location_fp}_{price_bucket}_{area_bucket}_{rooms}"
        return hashlib.md5(composite.encode()).hexdigest()

    return udf(_create_fingerprint, StringType())


def create_loose_fingerprint_udf():
    """UDF để tạo loose fingerprint cho grouping candidates"""

    def _create_loose_fingerprint(record):
        """Internal function để tạo loose fingerprint"""

        # Inline normalize_text function
        def normalize_text_inline(text):
            if not text:
                return ""
            return (
                text.lower().strip().replace(" ", "").replace("-", "").replace("_", "")
            )

        # Inline get_price_bucket function
        def get_price_bucket_inline(price, tolerance=0.2):
            if not price or price <= 0:
                return "unknown"
            lower_bound = price * (1 - tolerance)
            upper_bound = price * (1 + tolerance)
            lower_ty = lower_bound / 1_000_000_000
            upper_ty = upper_bound / 1_000_000_000
            return f"{lower_ty:.1f}-{upper_ty:.1f}ty"

        # Chỉ dùng location chính để group
        location_parts = [record.province or "", record.district or ""]
        location_fp = normalize_text_inline("_".join(location_parts))

        # Wide price range
        price_bucket = get_price_bucket_inline(record.price or 0, tolerance=0.2)

        return f"{location_fp}_{price_bucket}"

    return udf(_create_loose_fingerprint, StringType())


# ============================================================================
# SIMILARITY CALCULATION (Shared logic)
# ============================================================================


def calculate_similarity_score_udf():
    """UDF để tính similarity score giữa 2 records"""

    def _calculate_similarity(record1, record2):
        """Internal function để tính similarity"""
        import math

        # Inline calculate_distance function
        def calculate_distance_inline(lat1, lon1, lat2, lon2):
            if not all([lat1, lon1, lat2, lon2]):
                return float("inf")
            # Convert to radians
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            # Haversine formula
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))
            return c * 6371000  # Earth radius in meters

        # Address similarity (exact match cho data đã clean)
        addr_match = (
            (record1.province == record2.province)
            and (record1.district == record2.district)
            and (record1.ward == record2.ward)
            and (record1.street == record2.street)
        )
        addr_score = 1.0 if addr_match else 0.0

        # Price similarity
        price_score = 0.0
        if record1.price and record2.price:
            price_diff = abs(record1.price - record2.price) / max(
                record1.price, record2.price
            )
            price_score = max(0, 1 - price_diff * 2)  # Penalize >50% diff

        # Area similarity
        area_score = 0.0
        if record1.area and record2.area:
            area_diff = abs(record1.area - record2.area) / max(
                record1.area, record2.area
            )
            area_score = max(0, 1 - area_diff * 5)  # Penalize >20% diff

        # Room similarity
        room_score = (
            1.0
            if (
                record1.bedroom == record2.bedroom
                and record1.bathroom == record2.bathroom
            )
            else 0.0
        )

        # Coordinate similarity
        coord_score = 0.0
        if all(
            [record1.latitude, record1.longitude, record2.latitude, record2.longitude]
        ):
            distance = calculate_distance_inline(
                record1.latitude, record1.longitude, record2.latitude, record2.longitude
            )
            coord_score = 1.0 if distance < 50 else max(0, 1 - distance / 1000)

        # Weighted average
        weights = {"addr": 0.3, "price": 0.25, "area": 0.2, "rooms": 0.15, "coord": 0.1}
        total_score = (
            weights["addr"] * addr_score
            + weights["price"] * price_score
            + weights["area"] * area_score
            + weights["rooms"] * room_score
            + weights["coord"] * coord_score
        )

        return float(total_score)

    return udf(_calculate_similarity, DoubleType())


# ============================================================================
# LEGACY FUNCTIONS (Keep for backward compatibility)
# ============================================================================


def create_property_fingerprint(df: DataFrame) -> DataFrame:
    """
    Legacy function - Tạo property fingerprint để identify duplicate properties
    Dựa trên location + price + area + bedrooms + bathrooms
    """

    # Chuẩn hóa các trường chính để tạo fingerprint
    df_normalized = (
        df.withColumn(
            "normalized_location",
            lower(trim(regexp_replace(col("location"), r"[^\w\s]", ""))),
        )
        .withColumn(
            "normalized_title",
            lower(trim(regexp_replace(col("title"), r"[^\w\s]", ""))),
        )
        .withColumn(
            "price_rounded",
            when(
                col("price").isNotNull(),
                spark_round(col("price") / 100000000) * 100000000,
            ).otherwise(lit(0)),
        )
        .withColumn(
            "area_rounded",
            when(col("area").isNotNull(), spark_round(col("area"))).otherwise(lit(0)),
        )
    )

    # Tạo property fingerprint từ các thuộc tính chính
    df_with_fingerprint = df_normalized.withColumn(
        "property_fingerprint",
        md5(
            concat_ws(
                "|",
                coalesce(col("normalized_location"), lit("")),
                coalesce(col("price_rounded").cast("string"), lit("0")),
                coalesce(col("area_rounded").cast("string"), lit("0")),
                coalesce(col("bedroom").cast("string"), lit("0")),
                coalesce(col("bathroom").cast("string"), lit("0")),
                coalesce(col("province"), lit("")),
                coalesce(col("district"), lit("")),
                coalesce(col("ward"), lit("")),
            )
        ),
    )

    # Cleanup temporary columns
    return df_with_fingerprint.drop(
        "normalized_location", "normalized_title", "price_rounded", "area_rounded"
    )


# ============================================================================
# UNIFY STAGE: Comprehensive Deduplication
# ============================================================================


def deduplicate_unify_stage(df: DataFrame) -> DataFrame:
    """
    Comprehensive deduplication cho UNIFY stage

    Args:
        df: DataFrame đã unified từ multiple sources

    Returns:
        DataFrame đã loại bỏ duplicates
    """

    print("🔄 Starting comprehensive deduplication in UNIFY stage...")

    # Cache để tối ưu performance
    df.cache()
    original_count = df.count()
    print(f"📊 Original records: {original_count:,}")

    # Step 1: ID-based deduplication
    print("🔍 Step 1: ID-based deduplication...")
    df_step1 = df.dropDuplicates(["id"])
    step1_count = df_step1.count()
    print(
        f"📊 After ID dedup: {step1_count:,} (-{original_count - step1_count:,} duplicates)"
    )

    # Step 2: Cross-source exact matching
    print("🔍 Step 2: Cross-source exact matching...")
    df_step2 = deduplicate_cross_source_exact(df_step1)
    step2_count = df_step2.count()
    print(
        f"📊 After cross-source: {step2_count:,} (-{step1_count - step2_count:,} duplicates)"
    )

    # Step 3: Within-source fuzzy matching
    print("🔍 Step 3: Within-source fuzzy matching...")
    df_final = deduplicate_within_source_fuzzy(df_step2)
    final_count = df_final.count()
    print(
        f"📊 After fuzzy matching: {final_count:,} (-{step2_count - final_count:,} duplicates)"
    )

    print(
        f"✅ UNIFY deduplication completed: {final_count:,}/{original_count:,} records kept"
    )
    print(
        f"📉 Total reduction: {original_count - final_count:,} duplicates ({(original_count - final_count)/original_count*100:.1f}%)"
    )

    return df_final


def deduplicate_cross_source_exact(df: DataFrame) -> DataFrame:
    """Cross-source exact matching với property fingerprint"""

    # Tạo property fingerprint
    property_fp_udf = create_property_fingerprint_udf()
    df_with_fp = df.withColumn("property_fingerprint", property_fp_udf(struct("*")))

    # Window để chọn record tốt nhất trong mỗi fingerprint group
    window_spec = Window.partitionBy("property_fingerprint").orderBy(
        desc("data_quality_score"),
        desc("processing_timestamp"),
        when(col("source") == "batdongsan", 1)
        .when(col("source") == "chotot", 2)
        .otherwise(3),  # Source priority
    )

    # Chỉ giữ record tốt nhất
    result_df = (
        df_with_fp.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "property_fingerprint")
    )

    return result_df


def deduplicate_within_source_fuzzy(df: DataFrame) -> DataFrame:
    """Within-source fuzzy matching"""

    # Tạo loose fingerprint để group candidates
    loose_fp_udf = create_loose_fingerprint_udf()
    df_with_loose = df.withColumn("loose_fingerprint", loose_fp_udf(struct("*")))

    # Group theo loose fingerprint
    grouped = (
        df_with_loose.groupBy("loose_fingerprint")
        .agg(collect_list(struct("*")).alias("records"), count("*").alias("group_size"))
        .filter(col("group_size") > 1)
    )  # Chỉ xử lý groups có >1 record

    # Apply fuzzy matching trong mỗi group
    similarity_udf = calculate_similarity_score_udf()

    # Tìm duplicates trong mỗi group
    fuzzy_duplicates_udf = find_fuzzy_duplicates_in_group_udf()
    duplicate_pairs = grouped.select(
        explode(fuzzy_duplicates_udf(col("records"))).alias("duplicate_pair")
    )

    # Lấy danh sách IDs cần loại bỏ
    duplicate_ids = duplicate_pairs.select("duplicate_pair.remove_id").distinct()

    # Loại bỏ duplicates
    result_df = df_with_loose.join(
        duplicate_ids, col("id") == col("remove_id"), "left_anti"
    ).drop("loose_fingerprint")

    return result_df


def find_fuzzy_duplicates_in_group_udf():
    """UDF để tìm fuzzy duplicates trong 1 group"""

    def _find_duplicates(records_list):
        """Internal function để tìm duplicates - all logic inlined"""
        import math
        import hashlib

        def _normalize_text(text):
            """Normalize text for comparison"""
            if not text:
                return ""
            return str(text).lower().strip()

        def _calculate_distance(lat1, lon1, lat2, lon2):
            """Calculate distance between two points"""
            if not all([lat1, lon1, lat2, lon2]):
                return float("inf")

            try:
                lat1, lon1, lat2, lon2 = (
                    float(lat1),
                    float(lon1),
                    float(lat2),
                    float(lon2),
                )

                # Haversine formula
                R = 6371000  # Earth radius in meters
                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)
                a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
                    math.radians(lat1)
                ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(
                    dlon / 2
                )
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                return R * c
            except:
                return float("inf")

        def _calculate_similarity(r1, r2):
            """Calculate similarity between two records - all inlined"""
            score = 0.0
            weights = {
                "address": 0.3,
                "price": 0.25,
                "area": 0.2,
                "rooms": 0.15,
                "location": 0.1,
            }

            # Address similarity - use actual location fields
            # Build address string from components
            def _build_address(record):
                parts = []
                if hasattr(record, "street") and record.street:
                    parts.append(str(record.street))
                if hasattr(record, "ward") and record.ward:
                    parts.append(str(record.ward))
                if hasattr(record, "district") and record.district:
                    parts.append(str(record.district))
                if hasattr(record, "province") and record.province:
                    parts.append(str(record.province))
                return " ".join(parts)

            addr1 = _normalize_text(_build_address(r1))
            addr2 = _normalize_text(_build_address(r2))
            if addr1 and addr2:
                # Simple string similarity
                if addr1 == addr2:
                    score += weights["address"]
                elif addr1 in addr2 or addr2 in addr1:
                    score += weights["address"] * 0.8
                else:
                    # Basic token overlap
                    tokens1 = set(addr1.split())
                    tokens2 = set(addr2.split())
                    if tokens1 and tokens2:
                        overlap = len(tokens1.intersection(tokens2)) / len(
                            tokens1.union(tokens2)
                        )
                        score += weights["address"] * overlap

            # Price similarity
            price1 = getattr(r1, "price", None)
            price2 = getattr(r2, "price", None)
            if price1 and price2:
                try:
                    p1, p2 = float(price1), float(price2)
                    if p1 > 0 and p2 > 0:
                        diff = abs(p1 - p2) / max(p1, p2)
                        if diff <= 0.05:
                            score += weights["price"]
                        elif diff <= 0.1:
                            score += weights["price"] * 0.8
                        elif diff <= 0.2:
                            score += weights["price"] * 0.5
                except:
                    pass

            # Area similarity
            area1 = getattr(r1, "area", None)
            area2 = getattr(r2, "area", None)
            if area1 and area2:
                try:
                    a1, a2 = float(area1), float(area2)
                    if a1 > 0 and a2 > 0:
                        diff = abs(a1 - a2) / max(a1, a2)
                        if diff <= 0.05:
                            score += weights["area"]
                        elif diff <= 0.1:
                            score += weights["area"] * 0.8
                        elif diff <= 0.2:
                            score += weights["area"] * 0.5
                except:
                    pass

            # Location similarity (distance)
            lat1 = getattr(r1, "latitude", None)
            lon1 = getattr(r1, "longitude", None)
            lat2 = getattr(r2, "latitude", None)
            lon2 = getattr(r2, "longitude", None)

            if all([lat1, lon1, lat2, lon2]):
                distance = _calculate_distance(lat1, lon1, lat2, lon2)
                if distance <= 100:  # 100m
                    score += weights["location"]
                elif distance <= 500:  # 500m
                    score += weights["location"] * 0.8
                elif distance <= 1000:  # 1km
                    score += weights["location"] * 0.5

            # Rooms similarity - use correct field name 'bedroom' not 'bedrooms'
            rooms1 = getattr(r1, "bedroom", None)
            rooms2 = getattr(r2, "bedroom", None)
            if rooms1 == rooms2 and rooms1 is not None:
                score += weights["rooms"]

            return score

        duplicates = []

        for i in range(len(records_list)):
            for j in range(i + 1, len(records_list)):
                r1, r2 = records_list[i], records_list[j]

                # Tính similarity
                similarity = _calculate_similarity(r1, r2)

                if similarity >= 0.85:  # Threshold cho duplicate
                    # Chọn record tốt hơn để keep
                    if r1.data_quality_score > r2.data_quality_score or (
                        r1.data_quality_score == r2.data_quality_score
                        and r1.processing_timestamp > r2.processing_timestamp
                    ):
                        remove_id = r2.id
                    else:
                        remove_id = r1.id

                    duplicates.append(
                        {"remove_id": remove_id, "similarity": similarity}
                    )

        return duplicates

    return udf(
        _find_duplicates,
        ArrayType(
            StructType(
                [
                    StructField("remove_id", StringType()),
                    StructField("similarity", DoubleType()),
                ]
            )
        ),
    )


# ============================================================================
# LOAD TO SERVING STAGE: Limited Window Deduplication
# ============================================================================


def deduplicate_load_stage(new_df: DataFrame, postgres_config: Dict) -> DataFrame:
    """
    Limited window deduplication cho LOAD TO SERVING stage

    Args:
        new_df: New data từ UNIFY stage
        postgres_config: PostgreSQL connection config

    Returns:
        DataFrame đã resolved conflicts với existing data
    """

    print("🔄 Starting limited window deduplication in LOAD stage...")

    # Safety check for empty input
    if new_df.count() == 0:
        print("📊 Input DataFrame is empty, returning as-is")
        return new_df

    # Load existing data (30 ngày gần nhất)
    existing_df = load_existing_data_window(postgres_config, days=30)

    if existing_df.count() == 0:
        print("📊 No existing data found, loading all new records")
        return new_df

    print(f"📊 Loaded {existing_df.count():,} existing records (30 days)")

    # Find potential duplicates
    conflicts = find_conflicts_with_existing(new_df, existing_df)

    # Resolve conflicts
    resolved_df = resolve_load_conflicts(new_df, conflicts)

    print(f"✅ LOAD deduplication completed: {resolved_df.count():,} records to load")

    return resolved_df


def load_existing_data_window(postgres_config: Dict, days: int = 30) -> DataFrame:
    """Load existing data từ PostgreSQL với time window - with error handling"""

    try:
        # Safety check for postgres_config
        if postgres_config is None:
            print("⚠️ postgres_config is None")
            raise ValueError("postgres_config is required")

        spark = SparkSession.getActiveSession()

        # Test connection first
        print(
            f"🔍 Testing PostgreSQL connection to {postgres_config.get('url', 'unknown URL')}..."
        )

        # Query để load existing data
        query = f"""
        (SELECT
            id, url, source, title, location,
            province, district, ward, street,
            latitude, longitude,
            price, area, bedroom, bathroom,
            data_quality_score, processing_timestamp,
            created_at, updated_at
        FROM properties
        WHERE updated_at >= NOW() - INTERVAL '{days} days'
        LIMIT 200000) as existing_data
        """

        existing_df = (
            spark.read.format("jdbc")
            .option("url", postgres_config["url"])
            .option("dbtable", query)
            .option("user", postgres_config["user"])
            .option("password", postgres_config["password"])
            .option("driver", postgres_config["driver"])
            .option("fetchsize", "1000")
            .option("queryTimeout", "30")
            .load()
        )

        # Force evaluation to test connection
        count = existing_df.count()
        print(
            f"✅ Successfully connected to PostgreSQL, found {count:,} existing records"
        )
        return existing_df

    except Exception as e:
        print(f"⚠️ Failed to load existing data from PostgreSQL: {e}")
        print("💡 This might be the first run or PostgreSQL is not accessible")
        print("🔄 Proceeding without existing data comparison...")

        # Return empty DataFrame with expected schema
        spark = SparkSession.getActiveSession()
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DoubleType,
            TimestampType,
            LongType,
        )

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("source", StringType(), True),
                StructField("title", StringType(), True),
                StructField("location", StringType(), True),
                StructField("province", StringType(), True),
                StructField("district", StringType(), True),
                StructField("ward", StringType(), True),
                StructField("street", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("price", LongType(), True),
                StructField("area", DoubleType(), True),
                StructField("bedroom", DoubleType(), True),
                StructField("bathroom", DoubleType(), True),
                StructField("data_quality_score", DoubleType(), True),
                StructField("processing_timestamp", TimestampType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        return spark.createDataFrame([], schema)


def find_conflicts_with_existing(
    new_df: DataFrame, existing_df: DataFrame
) -> DataFrame:
    """Tìm conflicts giữa new data và existing data"""

    try:
        # Strategy 1: Exact ID matching
        id_conflicts = (
            new_df.alias("new")
            .join(
                existing_df.alias("existing"),
                col("new.id") == col("existing.id"),
                "inner",
            )
            .select(
                *[col(f"new.{c}").alias(f"new_{c}") for c in new_df.columns],
                *[col(f"existing.{c}").alias(f"existing_{c}") for c in existing_df.columns],
                lit("ID_MATCH").alias("conflict_type"),
            )
        )

        # Strategy 2: Location + characteristics matching
        location_conflicts = (
            new_df.alias("new")
            .join(
                existing_df.alias("existing"),
                # Same location
                (col("new.province") == col("existing.province"))
                & (col("new.district") == col("existing.district"))
                & (col("new.ward") == col("existing.ward"))
                & (col("new.street") == col("existing.street"))
                &
                # Similar characteristics - với null handling
                (
                    (col("new.price").isNull() & col("existing.price").isNull())
                    | (
                        col("new.price").isNotNull()
                        & col("existing.price").isNotNull()
                        & (
                            spark_abs(col("new.price") - col("existing.price"))
                            / greatest(col("existing.price"), lit(1))
                            <= 0.15
                        )
                    )
                )
                & (
                    (col("new.area").isNull() & col("existing.area").isNull())
                    | (
                        col("new.area").isNotNull()
                        & col("existing.area").isNotNull()
                        & (spark_abs(col("new.area") - col("existing.area")) <= 20)
                    )
                )
                & (
                    (col("new.bedroom").isNull() & col("existing.bedroom").isNull())
                    | (col("new.bedroom") == col("existing.bedroom"))
                )
                & (
                    (col("new.bathroom").isNull() & col("existing.bathroom").isNull())
                    | (col("new.bathroom") == col("existing.bathroom"))
                )
                &
                # Different IDs
                (col("new.id") != col("existing.id")),
                "inner",
            )
            .select(
                *[col(f"new.{c}").alias(f"new_{c}") for c in new_df.columns],
                *[col(f"existing.{c}").alias(f"existing_{c}") for c in existing_df.columns],
                lit("LOCATION_MATCH").alias("conflict_type"),
            )
        )

        return id_conflicts.union(location_conflicts)

    except Exception as e:
        print(f"❌ Error in find_conflicts_with_existing: {e}")
        # Return empty conflicts DataFrame
        from pyspark.sql.types import StructType, StructField, StringType

        spark = SparkSession.getActiveSession()
        conflict_schema = StructType([StructField("conflict_type", StringType(), True)])
        return spark.createDataFrame([], conflict_schema)


def resolve_load_conflicts(new_df: DataFrame, conflicts_df: DataFrame) -> DataFrame:
    """Resolve conflicts với existing data"""

    if conflicts_df.count() == 0:
        return new_df

    try:
        # Apply resolution rules - using the new column naming convention
        resolved_conflicts = conflicts_df.select(
            "*",  # Keep all existing columns
            when(
                # Rule 1: New data significantly better (20+ points)
                (
                    col("new_data_quality_score").isNotNull()
                    & col("existing_data_quality_score").isNotNull()
                )
                & (
                    col("new_data_quality_score")
                    - col("existing_data_quality_score")
                    >= 20
                ),
                lit("USE_NEW"),
            )
            .when(
                # Rule 2: Existing data significantly better
                (
                    col("new_data_quality_score").isNotNull()
                    & col("existing_data_quality_score").isNotNull()
                )
                & (
                    col("existing_data_quality_score")
                    - col("new_data_quality_score")
                    >= 20
                ),
                lit("SKIP_NEW"),
            )
            .when(
                # Rule 3: Different sources with similar quality
                (
                    col("new_source").isNotNull()
                    & col("existing_source").isNotNull()
                )
                & (col("new_source") != col("existing_source"))
                & (
                    spark_abs(
                        coalesce(col("new_data_quality_score"), lit(0))
                        - coalesce(col("existing_data_quality_score"), lit(0))
                    )
                    < 20
                ),
                lit("USE_NEW"),
            )
            .when(
                # Rule 4: Same source, newer timestamp wins
                (
                    col("new_source").isNotNull()
                    & col("existing_source").isNotNull()
                )
                & (col("new_source") == col("existing_source"))
                & (
                    col("new_processing_timestamp").isNotNull()
                    & col("existing_processing_timestamp").isNotNull()
                )
                & (
                    col("new_processing_timestamp")
                    > col("existing_processing_timestamp")
                ),
                lit("USE_NEW"),
            )
            .otherwise(lit("SKIP_NEW"))
            .alias("resolution"),
        )

        # Get IDs to skip
        skip_records = resolved_conflicts.filter(col("resolution") == "SKIP_NEW")

        if skip_records.count() > 0:
            skip_ids = skip_records.select(
                col("new_id").alias("skip_id")
            ).distinct()
            result_df = new_df.join(
                skip_ids, new_df.id == skip_ids.skip_id, "left_anti"
            )
        else:
            result_df = new_df

        return result_df

    except Exception as e:
        print(f"❌ Conflict resolution failed: {e}")
        return new_df


# ============================================================================
# MAIN INTERFACE FUNCTIONS
# ============================================================================


def apply_unify_deduplication(df: DataFrame) -> DataFrame:
    """
    Main interface cho UNIFY stage deduplication
    """
    return deduplicate_unify_stage(df)


def apply_load_deduplication(df: DataFrame, postgres_config: Dict) -> DataFrame:
    """
    Main interface cho LOAD stage deduplication with error handling
    """
    try:
        return deduplicate_load_stage(df, postgres_config)
    except Exception as e:
        print(f"⚠️ Load deduplication failed: {e}")
        print("🔄 Returning original data without load-stage deduplication")
        return df
