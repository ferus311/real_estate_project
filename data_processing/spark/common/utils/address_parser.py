"""
Vietnamese Address Parser for Real Estate Data
Sử dụng Spark SQL functions và vietnamaddress_utf8.json để parse và chuẩn hóa địa chỉ Việt Nam
"""

import json
import os
import unicodedata

# Default path to Vietnamese address JSON file
DEFAULT_JSON_PATH = "address/vietnamaddress_utf8.json"


def normalize_vietnamese_text(text):
    """
    Normalize Vietnamese text using unicodedata for accurate comparison

    Args:
        text: Input text to normalize

    Returns:
        Normalized text string
    """
    if not text:
        return ""

    # Convert to string and normalize unicode
    text_str = str(text).strip()

    # Normalize unicode (NFC normalization)
    normalized = unicodedata.normalize("NFC", text_str)

    # Convert to lowercase for comparison
    normalized = normalized.lower()

    return normalized


def load_vietnamese_address_data(json_path=None):
    """
    Load và chuẩn bị dữ liệu địa chỉ Việt Nam cho lookup

    Args:
        json_path: Đường dẫn đến vietnamaddress_utf8.json. Nếu None sẽ dùng default path

    Returns:
        Dict chứa mapping data để lookup nhanh
    """
    if json_path is None:
        json_path = DEFAULT_JSON_PATH

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            address_data = json.load(f)

        # Tạo lookup dictionaries cho việc mapping nhanh
        province_lookup = {}
        district_lookup = {}
        ward_lookup = {}
        street_lookup = {}

        for province in address_data:
            province_id = province["id"]
            province_name = normalize_vietnamese_text(province["name"])
            province_code = normalize_vietnamese_text(province.get("code", ""))

            # Province mapping
            province_lookup[province_name] = {
                "id": province_id,
                "name": province["name"],
                "code": province.get("code", ""),
            }
            if province_code:
                province_lookup[province_code] = province_lookup[province_name]

            # District mapping
            for district in province.get("districts", []):
                district_id = district["id"]
                district_name = normalize_vietnamese_text(district["name"])
                full_key = f"{district_name}_{province_name}"

                district_lookup[full_key] = {
                    "id": district_id,
                    "name": district["name"],
                    "province_id": province_id,
                    "province_name": province["name"],
                }

                # ALSO add district name only for fallback lookup (but with province check)
                district_lookup[district_name] = district_lookup.get(district_name, [])
                if not isinstance(district_lookup[district_name], list):
                    district_lookup[district_name] = [district_lookup[district_name]]
                district_lookup[district_name].append(
                    {
                        "id": district_id,
                        "name": district["name"],
                        "province_id": province_id,
                        "province_name": province["name"],
                    }
                )

                # Ward mapping
                for ward in district.get("wards", []):
                    ward_id = ward["id"]
                    ward_name = normalize_vietnamese_text(ward["name"])
                    ward_key = f"{ward_name}_{district_name}_{province_name}"

                    ward_lookup[ward_key] = {
                        "id": ward_id,
                        "name": ward["name"],
                        "prefix": ward.get("prefix", ""),
                        "district_id": district_id,
                        "district_name": district["name"],
                        "province_id": province_id,
                        "province_name": province["name"],
                    }

                # Street mapping với hierarchical support
                for street in district.get("streets", []):
                    street_id = street["id"]
                    street_name = normalize_vietnamese_text(street["name"])
                    street_key = f"{street_name}_{district_name}_{province_name}"

                    street_lookup[street_key] = {
                        "id": street_id,
                        "name": street["name"],
                        "prefix": street.get("prefix", ""),
                        "district_id": district_id,
                        "district_name": district["name"],
                        "province_id": province_id,
                        "province_name": province["name"],
                    }

                    # ALSO add street name only for fallback lookup (but with district/province check)
                    street_lookup[street_name] = street_lookup.get(street_name, [])
                    if not isinstance(street_lookup[street_name], list):
                        street_lookup[street_name] = [street_lookup[street_name]]
                    street_lookup[street_name].append(
                        {
                            "id": street_id,
                            "name": street["name"],
                            "prefix": street.get("prefix", ""),
                            "district_id": district_id,
                            "district_name": district["name"],
                            "province_id": province_id,
                            "province_name": province["name"],
                        }
                    )

        return {
            "provinces": province_lookup,
            "districts": district_lookup,
            "wards": ward_lookup,
            "streets": street_lookup,
        }
    except Exception as e:
        print(f"Warning: Could not load Vietnamese address data from {json_path}: {e}")
        return None


def standardize_address_component_manual(
    component_value,
    component_type,
    district_context=None,
    province_context=None,
    address_mapping=None,
):
    """
    Manual version of address standardization (không dùng PySpark) với improved hierarchical mapping
    """
    if not address_mapping or not component_value:
        return {"name": component_value or "unknown", "id": -1}

    component_value = normalize_vietnamese_text(component_value)

    # Remove common prefixes - improved logic
    prefixes_to_remove = {
        "province": ["tỉnh", "thành phố", "tp.", "tp", "t.p"],
        "district": [
            "quận",
            "huyện",
            "thị xã",
            "thành phố",
            "tx.",
            "tx",
            "t.x",
            "tp.",
            "tp",
            "t.p",
        ],
        "ward": ["phường", "xã", "thị trấn", "tt.", "tt", "t.t"],
        "street": ["đường", "phố", "ngõ", "hẻm", "số", "đ.", "p."],
    }

    if component_type in prefixes_to_remove:
        for prefix in prefixes_to_remove[component_type]:
            # Try with space after prefix
            if component_value.startswith(prefix + " "):
                component_value = component_value[len(prefix + " ") :].strip()
                break
            # Try without space
            elif component_value.startswith(prefix):
                component_value = component_value[len(prefix) :].strip()
                break

    # Try to find in mapping with hierarchical support
    if component_type == "province":
        match = address_mapping["provinces"].get(component_value)
        if match:
            return {"name": match["name"], "id": match["id"]}

    elif component_type == "district" and province_context:
        # Clean province context
        province_clean = normalize_vietnamese_text(province_context)
        for prefix in ["tỉnh", "thành phố", "tp.", "tp", "t.p"]:
            if province_clean.startswith(prefix + " "):
                province_clean = province_clean[len(prefix + " ") :].strip()
                break
            elif province_clean.startswith(prefix):
                province_clean = province_clean[len(prefix) :].strip()
                break

        # Try with province context first (most accurate)
        key_with_province = f"{component_value}_{province_clean}"
        match = address_mapping["districts"].get(key_with_province)
        if match:
            return {"name": match["name"], "id": match["id"]}

        # Fallback: search in districts with same name but check province compatibility
        potential_matches = address_mapping["districts"].get(component_value)
        if potential_matches:
            if isinstance(potential_matches, list):
                # Find the one that matches our province context
                for candidate in potential_matches:
                    candidate_province = normalize_vietnamese_text(
                        candidate["province_name"]
                    )
                    if candidate_province == province_clean:
                        return {"name": candidate["name"], "id": candidate["id"]}
                # If no exact province match, return first as fallback
                return {
                    "name": potential_matches[0]["name"],
                    "id": potential_matches[0]["id"],
                }
            else:
                return {
                    "name": potential_matches["name"],
                    "id": potential_matches["id"],
                }

        # Final fallback: fuzzy search by name only
        for key, value in address_mapping["districts"].items():
            if isinstance(value, dict) and "name" in value:
                json_name_clean = normalize_vietnamese_text(value["name"])
                for prefix in [
                    "quận",
                    "huyện",
                    "thị xã",
                    "thành phố",
                    "tx.",
                    "tx",
                    "t.x",
                    "tp.",
                    "tp",
                    "t.p",
                ]:
                    if json_name_clean.startswith(prefix + " "):
                        json_name_clean = json_name_clean[len(prefix + " ") :].strip()
                        break
                    elif json_name_clean.startswith(prefix):
                        json_name_clean = json_name_clean[len(prefix) :].strip()
                        break
                if json_name_clean == component_value:
                    return {"name": value["name"], "id": value["id"]}

    elif component_type == "ward" and district_context and province_context:
        # Clean contexts
        district_clean = normalize_vietnamese_text(district_context)
        for prefix in ["quận", "huyện", "thị xã", "tx.", "tx", "t.x"]:
            if district_clean.startswith(prefix + " "):
                district_clean = district_clean[len(prefix + " ") :].strip()
                break
            elif district_clean.startswith(prefix):
                district_clean = district_clean[len(prefix) :].strip()
                break

        province_clean = normalize_vietnamese_text(province_context)
        for prefix in ["tỉnh", "thành phố", "tp.", "tp", "t.p"]:
            if province_clean.startswith(prefix + " "):
                province_clean = province_clean[len(prefix + " ") :].strip()
                break
            elif province_clean.startswith(prefix):
                province_clean = province_clean[len(prefix) :].strip()
                break

        # Try with full context (most accurate)
        key_with_context = f"{component_value}_{district_clean}_{province_clean}"
        match = address_mapping["wards"].get(key_with_context)
        if match:
            return {"name": match["name"], "id": match["id"]}

        # Fallback: fuzzy search by name only
        for key, value in address_mapping["wards"].items():
            if isinstance(value, dict) and "name" in value:
                json_name_clean = normalize_vietnamese_text(value["name"])
                for prefix in ["phường", "xã", "thị trấn", "tt.", "tt", "t.t"]:
                    if json_name_clean.startswith(prefix + " "):
                        json_name_clean = json_name_clean[len(prefix + " ") :].strip()
                        break
                    elif json_name_clean.startswith(prefix):
                        json_name_clean = json_name_clean[len(prefix) :].strip()
                        break
                if json_name_clean == component_value:
                    return {"name": value["name"], "id": value["id"]}

    elif component_type == "street" and district_context and province_context:
        # Clean contexts
        district_clean = normalize_vietnamese_text(district_context)
        for prefix in ["quận", "huyện", "thị xã", "tx.", "tx", "t.x"]:
            if district_clean.startswith(prefix + " "):
                district_clean = district_clean[len(prefix + " ") :].strip()
                break
            elif district_clean.startswith(prefix):
                district_clean = district_clean[len(prefix) :].strip()
                break

        province_clean = normalize_vietnamese_text(province_context)
        for prefix in ["tỉnh", "thành phố", "tp.", "tp", "t.p"]:
            if province_clean.startswith(prefix + " "):
                province_clean = province_clean[len(prefix + " ") :].strip()
                break
            elif province_clean.startswith(prefix):
                province_clean = province_clean[len(prefix) :].strip()
                break

        # Try with full context first (most accurate hierarchical mapping)
        key_with_context = f"{component_value}_{district_clean}_{province_clean}"
        match = address_mapping["streets"].get(key_with_context)
        if match:
            return {"name": match["name"], "id": match["id"]}

        # IMPROVED: Search for streets with same name but ensure district/province compatibility
        potential_matches = address_mapping["streets"].get(component_value)
        if potential_matches:
            if isinstance(potential_matches, list):
                # Find the one that matches our district context (hierarchical accuracy)
                for candidate in potential_matches:
                    candidate_district = normalize_vietnamese_text(
                        candidate["district_name"]
                    )
                    candidate_province = normalize_vietnamese_text(
                        candidate["province_name"]
                    )

                    # Clean candidate district name for comparison
                    for prefix in ["quận", "huyện", "thị xã", "tx.", "tx", "t.x"]:
                        if candidate_district.startswith(prefix + " "):
                            candidate_district = candidate_district[
                                len(prefix + " ") :
                            ].strip()
                            break
                        elif candidate_district.startswith(prefix):
                            candidate_district = candidate_district[
                                len(prefix) :
                            ].strip()
                            break

                    if (
                        candidate_district == district_clean
                        and candidate_province == province_clean
                    ):
                        return {"name": candidate["name"], "id": candidate["id"]}

                # If no exact match, check district only
                for candidate in potential_matches:
                    candidate_district = normalize_vietnamese_text(
                        candidate["district_name"]
                    )
                    for prefix in ["quận", "huyện", "thị xã", "tx.", "tx", "t.x"]:
                        if candidate_district.startswith(prefix + " "):
                            candidate_district = candidate_district[
                                len(prefix + " ") :
                            ].strip()
                            break
                        elif candidate_district.startswith(prefix):
                            candidate_district = candidate_district[
                                len(prefix) :
                            ].strip()
                            break
                    if candidate_district == district_clean:
                        return {"name": candidate["name"], "id": candidate["id"]}

                # If still no match, return first as fallback
                return {
                    "name": potential_matches[0]["name"],
                    "id": potential_matches[0]["id"],
                }
            else:
                return {
                    "name": potential_matches["name"],
                    "id": potential_matches["id"],
                }

        # Final fallback: fuzzy search by name only
        for key, value in address_mapping["streets"].items():
            if isinstance(value, dict) and "name" in value:
                json_name_clean = normalize_vietnamese_text(value["name"])
                for prefix in ["đường", "phố", "ngõ", "hẻm", "số", "đ.", "p."]:
                    if json_name_clean.startswith(prefix + " "):
                        json_name_clean = json_name_clean[len(prefix + " ") :].strip()
                        break
                    elif json_name_clean.startswith(prefix):
                        json_name_clean = json_name_clean[len(prefix) :].strip()
                        break
                if json_name_clean == component_value:
                    return {"name": value["name"], "id": value["id"]}

    # Return cleaned component if no mapping found - use ID = -1 for unknown
    cleaned_name = component_value.title() if component_value else "unknown"
    return {"name": cleaned_name, "id": -1}


def add_address_parsing_to_dataframe(
    df,  # DataFrame type
    location_col: str,
    json_path: str = None,
    existing_street_col: str = None,
    existing_ward_col: str = None,
    existing_district_col: str = None,
    existing_province_col: str = None,
):
    """
    Thêm address parsing vào DataFrame bằng cách sử dụng Spark SQL và Vietnamese address mapping

    Args:
        df: Input DataFrame
        location_col: Tên cột chứa location string
        json_path: Đường dẫn đến vietnamaddress_utf8.json. Nếu None sẽ dùng default path
        existing_street_col: Cột street đã có (cho Chotot)
        existing_ward_col: Cột ward đã có (cho Chotot)
        existing_district_col: Cột district đã có (cho Chotot)
        existing_province_col: Cột province đã có (cho Chotot)
    """

    # Import Spark functions inside function to avoid import errors
    try:
        from pyspark.sql.functions import (
            col,
            when,
            lit,
            trim,
            lower,
            regexp_replace,
            split,
            element_at,
            size,
            regexp_extract,
            coalesce,
            udf,
        )
        from pyspark.sql.types import IntegerType, StringType
    except ImportError:
        # Fallback for development environment
        print("Warning: PySpark not available in development environment")
        return df

    # Load Vietnamese address mapping data if available
    if json_path is None:
        json_path = DEFAULT_JSON_PATH

    address_mapping = None
    if os.path.exists(json_path):
        address_mapping = load_vietnamese_address_data(json_path)

    # Create UDF for address standardization if mapping is available
    def standardize_address_component(
        component_value, component_type, district_context=None, province_context=None
    ):
        """Standardize address component using Vietnamese address data with improved hierarchical mapping"""
        return standardize_address_component_manual(
            component_value,
            component_type,
            district_context,
            province_context,
            address_mapping,
        )

    # Create UDFs for standardization that return both name and ID
    if address_mapping:

        def standardize_province_udf_func(x):
            result = standardize_address_component(x, "province")
            return result["name"]

        def standardize_district_udf_func(x, province_ctx):
            result = standardize_address_component(
                x, "district", province_context=province_ctx
            )
            return result["name"]

        def standardize_ward_udf_func(x, district_ctx, province_ctx):
            result = standardize_address_component(
                x, "ward", district_ctx, province_ctx
            )
            return result["name"]

        def standardize_street_udf_func(x, district_ctx, province_ctx):
            result = standardize_address_component(
                x, "street", district_ctx, province_ctx
            )
            return result["name"]

        # ID UDFs
        def get_province_id_udf_func(x):
            result = standardize_address_component(x, "province")
            return result["id"]

        def get_district_id_udf_func(x, province_ctx):
            result = standardize_address_component(
                x, "district", province_context=province_ctx
            )
            return result["id"]

        def get_ward_id_udf_func(x, district_ctx, province_ctx):
            result = standardize_address_component(
                x, "ward", district_ctx, province_ctx
            )
            return result["id"]

        def get_street_id_udf_func(x, district_ctx, province_ctx):
            result = standardize_address_component(
                x, "street", district_ctx, province_ctx
            )
            return result["id"]

        standardize_province_udf = udf(standardize_province_udf_func, StringType())
        standardize_district_udf = udf(standardize_district_udf_func, StringType())
        standardize_ward_udf = udf(standardize_ward_udf_func, StringType())
        standardize_street_udf = udf(standardize_street_udf_func, StringType())

        get_province_id_udf = udf(get_province_id_udf_func, IntegerType())
        get_district_id_udf = udf(get_district_id_udf_func, IntegerType())
        get_ward_id_udf = udf(get_ward_id_udf_func, IntegerType())
        get_street_id_udf = udf(get_street_id_udf_func, IntegerType())

    # Parse location string using Spark SQL functions
    df_with_split = df.withColumn(
        "location_parts", split(col(location_col), ",")
    ).withColumn("parts_count", size(col("location_parts")))

    # Extract province (always last part)
    df_result = df_with_split.withColumn(
        "province_raw",
        when(
            existing_province_col
            and col(existing_province_col).isNotNull()
            and (col(existing_province_col) != ""),
            trim(col(existing_province_col)),
        )
        .when(
            col("parts_count") >= 1,
            trim(
                regexp_replace(
                    element_at(col("location_parts"), -1),
                    r"(?i)(tỉnh|thành phố|tp\.?\s*)",
                    "",
                )
            ),
        )
        .otherwise(lit("unknown")),
    )

    # Extract district (second to last part)
    df_result = df_result.withColumn(
        "district_raw",
        when(
            existing_district_col
            and col(existing_district_col).isNotNull()
            and (col(existing_district_col) != ""),
            trim(col(existing_district_col)),
        )
        .when(
            col("parts_count") >= 2,
            trim(
                regexp_replace(
                    element_at(col("location_parts"), -2),
                    r"(?i)(quận|huyện|thị xã|tx\.?\s*)",
                    "",
                )
            ),
        )
        .otherwise(lit("unknown")),
    )

    # Extract ward (third to last part)
    df_result = df_result.withColumn(
        "ward_raw",
        when(
            existing_ward_col
            and col(existing_ward_col).isNotNull()
            and (col(existing_ward_col) != ""),
            trim(col(existing_ward_col)),
        )
        .when(
            col("parts_count") >= 3,
            trim(
                regexp_replace(
                    element_at(col("location_parts"), -3),
                    r"(?i)(phường|xã|thị trấn|tt\.?\s*)",
                    "",
                )
            ),
        )
        .otherwise(lit("unknown")),
    )

    # Special handling for Chotot ward format with parentheses
    df_result = (
        df_result.withColumn(
            "ward_parentheses_content", regexp_extract(col("ward_raw"), r"\((.*?)\)", 1)
        )
        .withColumn(
            "ward_cleaned", trim(regexp_replace(col("ward_raw"), r"\s*\(.*?\)\s*", ""))
        )
        .withColumn(
            "district_from_ward",
            when(
                col("ward_parentheses_content").rlike(
                    r"(?i)(quận|huyện|thị xã|thành phố)"
                ),
                trim(
                    regexp_replace(
                        regexp_replace(
                            col("ward_parentheses_content"), r"(?i)\s*cũ\s*", ""
                        ),
                        r"(?i)(quận|huyện|thị xã|thành phố|tx\.?\s*|tp\.?\s*)",
                        "",
                    )
                ),
            ).otherwise(lit("")),
        )
    )

    # Update district if we found district info in ward parentheses
    df_result = df_result.withColumn(
        "district_raw",
        when(
            (col("district_from_ward") != "") & (col("district_from_ward").isNotNull()),
            col("district_from_ward"),
        ).otherwise(col("district_raw")),
    ).withColumn("ward_raw", col("ward_cleaned"))

    # Extract street (fourth to last part)
    df_result = df_result.withColumn(
        "street_raw",
        when(
            existing_street_col
            and col(existing_street_col).isNotNull()
            and (col(existing_street_col) != ""),
            trim(col(existing_street_col)),
        )
        .when(
            col("parts_count") >= 4,
            trim(
                regexp_replace(
                    element_at(col("location_parts"), -4),
                    r"(?i)(đường|phố|ngõ|hẻm|số)\s*",
                    "",
                )
            ),
        )
        .when(col("parts_count") == 3, lit("unknown"))
        .otherwise(lit("unknown")),
    )

    # Apply standardization using UDFs if available
    if address_mapping:
        # Apply name standardization
        df_result = (
            df_result.withColumn(
                "province", standardize_province_udf(col("province_raw"))
            )
            .withColumn(
                "district",
                standardize_district_udf(col("district_raw"), col("province_raw")),
            )
            .withColumn(
                "ward",
                standardize_ward_udf(
                    col("ward_raw"), col("district_raw"), col("province_raw")
                ),
            )
            .withColumn(
                "street",
                standardize_street_udf(
                    col("street_raw"), col("district_raw"), col("province_raw")
                ),
            )
        )

        # Apply ID mapping
        df_result = (
            df_result.withColumn(
                "province_id", get_province_id_udf(col("province_raw"))
            )
            .withColumn(
                "district_id",
                get_district_id_udf(col("district_raw"), col("province_raw")),
            )
            .withColumn(
                "ward_id",
                get_ward_id_udf(
                    col("ward_raw"), col("district_raw"), col("province_raw")
                ),
            )
            .withColumn(
                "street_id",
                get_street_id_udf(
                    col("street_raw"), col("district_raw"), col("province_raw")
                ),
            )
        )

    else:
        # Simple standardization without mapping
        df_result = (
            df_result.withColumnRenamed("province_raw", "province")
            .withColumnRenamed("district_raw", "district")
            .withColumnRenamed("ward_raw", "ward")
            .withColumnRenamed("street_raw", "street")
        )

        # Add placeholder IDs when no mapping available
        df_result = (
            df_result.withColumn("street_id", lit(-1).cast(IntegerType()))
            .withColumn("ward_id", lit(-1).cast(IntegerType()))
            .withColumn("district_id", lit(-1).cast(IntegerType()))
            .withColumn("province_id", lit(-1).cast(IntegerType()))
        )

    # Clean empty strings to "unknown"
    df_result = (
        df_result.withColumn(
            "province",
            when(
                (col("province") == "") | (col("province").isNull()), lit("unknown")
            ).otherwise(col("province")),
        )
        .withColumn(
            "district",
            when(
                (col("district") == "") | (col("district").isNull()), lit("unknown")
            ).otherwise(col("district")),
        )
        .withColumn(
            "ward",
            when(
                (col("ward") == "") | (col("ward").isNull()), lit("unknown")
            ).otherwise(col("ward")),
        )
        .withColumn(
            "street",
            when(
                (col("street") == "") | (col("street").isNull()), lit("unknown")
            ).otherwise(col("street")),
        )
    )

    # Drop temporary columns
    df_result = df_result.drop(
        "location_parts",
        "parts_count",
        "province_raw",
        "district_raw",
        "ward_raw",
        "street_raw",
        "ward_parentheses_content",
        "ward_cleaned",
        "district_from_ward",
    )

    return df_result
