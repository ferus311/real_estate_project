"""
Address parser for Spark ETL
Focus: Robust address parsing from location column with proper prefix handling
"""

import json
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    split,
    trim,
    lower,
    when,
    lit,
    size,
    element_at,
    regexp_replace,
    regexp_extract,
)

DEFAULT_JSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "address", "vietnamaddress_utf8.json"
)


def create_lookup_tables(spark, json_path):
    """Create lookup tables for address mapping"""
    print(f"[INFO] Loading address mapping from: {json_path}")

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        provinces_data = []
        districts_data = []
        wards_data = []
        streets_data = []

        for province in data:
            p_name = province["name"]
            # Clean province name: remove common prefixes
            p_clean = p_name.lower().strip()
            if p_clean.startswith("tỉnh "):
                p_clean = p_clean[5:].strip()
            elif p_clean.startswith("thành phố "):
                p_clean = p_clean[10:].strip()
            elif p_clean.startswith("tp."):
                p_clean = p_clean[3:].strip()
            elif p_clean.startswith("tp "):
                p_clean = p_clean[3:].strip()

            provinces_data.append((p_clean, int(province["id"]), p_name))

            for district in province.get("districts", []):
                d_name = district["name"]
                d_clean = d_name.lower().strip()
                if d_clean.startswith("quận "):
                    d_clean = d_clean[5:].strip()
                elif d_clean.startswith("huyện "):
                    d_clean = d_clean[6:].strip()
                elif d_clean.startswith("thị xã "):
                    d_clean = d_clean[7:].strip()
                elif d_clean.startswith("thành phố "):
                    d_clean = d_clean[10:].strip()

                districts_data.append((d_clean, p_clean, int(district["id"]), d_name))

                for ward in district.get("wards", []):
                    w_name = ward["name"]
                    w_clean = w_name.lower().strip()
                    if w_clean.startswith("phường "):
                        w_clean = w_clean[7:].strip()
                    elif w_clean.startswith("xã "):
                        w_clean = w_clean[3:].strip()
                    elif w_clean.startswith("thị trấn "):
                        w_clean = w_clean[9:].strip()

                    wards_data.append(
                        (w_clean, d_clean, p_clean, int(ward["id"]), w_name)
                    )

                for street in district.get("streets", []):
                    s_name = street["name"]
                    s_clean = s_name.lower().strip()
                    # Street: handle more cases
                    if s_clean.startswith("đường "):
                        s_clean = s_clean[7:].strip()
                    elif s_clean.startswith("phố "):
                        s_clean = s_clean[5:].strip()
                    elif s_clean.startswith("ngõ "):
                        s_clean = s_clean[5:].strip()
                    elif s_clean.startswith("hẻm "):
                        s_clean = s_clean[5:].strip()
                    # Handle "hẻm number street_name" pattern
                    elif "hẻm " in s_clean and any(
                        word in s_clean for word in ["đường", "phố"]
                    ):
                        # Extract main street name after hẻm number
                        import re

                        match = re.search(r"hẻm\s+\d+[/\d]*\s+(.+)", s_clean)
                        if match:
                            s_clean = match.group(1).strip()

                    streets_data.append(
                        (s_clean, d_clean, p_clean, int(street["id"]), s_name)
                    )

        # Create DataFrames with unique column names
        provinces_df = spark.createDataFrame(
            provinces_data, ["prov_key", "prov_id", "prov_name"]
        )
        districts_df = spark.createDataFrame(
            districts_data, ["dist_key", "dist_prov_key", "dist_id", "dist_name"]
        )
        wards_df = spark.createDataFrame(
            wards_data,
            ["ward_key", "ward_dist_key", "ward_prov_key", "ward_id", "ward_name"],
        )
        streets_df = spark.createDataFrame(
            streets_data,
            [
                "street_key",
                "street_dist_key",
                "street_prov_key",
                "street_id",
                "street_name",
            ],
        )

        print(
            f"[INFO] Created lookup: {provinces_df.count()} provinces, {districts_df.count()} districts, {wards_df.count()} wards, {streets_df.count()} streets"
        )

        return provinces_df, districts_df, wards_df, streets_df

    except Exception as e:
        print(f"[ERROR] Failed to load address mapping: {e}")
        return None, None, None, None


def add_address_parsing_to_dataframe(df, location_col: str, json_path: str = None):
    """
    Add address parsing to dataframe with robust prefix handling and mapping
    """
    if json_path is None:
        json_path = DEFAULT_JSON_PATH

    spark = df.sql_ctx.sparkSession

    # Load lookup tables
    provinces_df, districts_df, wards_df, streets_df = create_lookup_tables(
        spark, json_path
    )
    if provinces_df is None:
        return df

    print(f"[INFO] Starting address parsing for column: {location_col}")

    # Step 1: Split location and extract basic components
    # Logic: Take from end - province is last, district is 2nd last, etc.
    df_split = (
        df.withColumn("parts", split(col(location_col), ","))
        .withColumn("parts_count", size(col("parts")))
        .withColumn(
            "province_raw",
            when(col("parts_count") >= 1, trim(element_at(col("parts"), -1))).otherwise(
                lit("unknown")
            ),
        )
        .withColumn(
            "district_raw",
            when(col("parts_count") >= 2, trim(element_at(col("parts"), -2))).otherwise(
                lit("unknown")
            ),
        )
        .withColumn(
            "ward_raw",
            when(col("parts_count") >= 3, trim(element_at(col("parts"), -3))).otherwise(
                lit("unknown")
            ),
        )
        .withColumn(
            "street_raw",
            when(col("parts_count") >= 4, trim(element_at(col("parts"), -4))).otherwise(
                when(
                    col("parts_count") == 1, trim(element_at(col("parts"), 1))
                ).otherwise(lit("unknown"))
            ),
        )
    )

    # Step 1.5: Handle ward with parentheses (extract district from parentheses)
    df_ward_fixed = (
        df_split.withColumn(
            "ward_has_parentheses",
            col("ward_raw").contains("(") & col("ward_raw").contains(")"),
        )
        .withColumn(
            "district_from_ward",
            when(
                col("ward_has_parentheses"),
                regexp_extract(col("ward_raw"), r"\((.*?)\)", 1),
            ).otherwise(lit("")),
        )
        .withColumn(
            "ward_cleaned_parentheses",
            when(
                col("ward_has_parentheses"),
                regexp_replace(col("ward_raw"), r"\s*\(.*?\)\s*", ""),
            ).otherwise(col("ward_raw")),
        )
        .withColumn(
            "district_final",
            when(
                (col("district_from_ward") != "")
                & col("district_from_ward").rlike("(?i)(quận|huyện|thị xã|thành phố)"),
                regexp_replace(col("district_from_ward"), r"(?i)\s*cũ\s*", ""),
            ).otherwise(col("district_raw")),
        )
        .withColumn("ward_final", col("ward_cleaned_parentheses"))
    )

    # Step 2: Clean components - case insensitive prefix removal
    df_clean = (
        df_ward_fixed.withColumn(
            "my_province_key",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim(col("province_raw"))), r"^(?i)tỉnh\s+", ""
                        ),
                        r"^(?i)thành phố\s+",
                        "",
                    ),
                    r"^(?i)tp\.\s*",
                    "",
                ),
                r"^(?i)tp\s+",
                "",
            ),
        )
        .withColumn(
            "my_district_key",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim(col("district_final"))), r"^(?i)quận\s+", ""
                        ),
                        r"^(?i)huyện\s+",
                        "",
                    ),
                    r"^(?i)thị xã\s+",
                    "",
                ),
                r"^(?i)thành phố\s+",
                "",
            ),
        )
        .withColumn(
            "my_ward_key",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        lower(trim(col("ward_final"))), r"^(?i)phường\s+", ""
                    ),
                    r"^(?i)xã\s+",
                    "",
                ),
                r"^(?i)thị trấn\s+",
                "",
            ),
        )
        .withColumn(
            "my_street_key",
            # Improved street cleaning - handle hẻm patterns better
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    lower(trim(col("street_raw"))),
                                    r"^(?i)hẻm\s+\d+[/\d]*\s+",
                                    "",  # Remove "Hẻm 136/10 " pattern first
                                ),
                                r"^(?i)đường\s+",
                                "",  # Remove "Đường " at start
                            ),
                            r"^(?i)phố\s+",
                            "",  # Remove "Phố " at start
                        ),
                        r"^(?i)ngõ\s+",
                        "",  # Remove "Ngõ " at start
                    ),
                    r"^(?i)hẻm\s+",
                    "",  # Remove remaining "Hẻm " at start
                ),
                r"^(?i).*(?:đường|phố|ngõ|hẻm)\s+",
                "",  # Fallback: remove everything up to street type
            ),
        )
    )

    # Step 3: JOIN with unique column names - NO AMBIGUITY

    # Join Province
    df_with_province = (
        df_clean.join(provinces_df, col("my_province_key") == col("prov_key"), "left")
        .withColumn(
            "province",
            when(col("prov_id").isNotNull(), col("prov_name")).otherwise(
                when(col("province_raw") != "unknown", col("province_raw")).otherwise(
                    lit("unknown")
                )
            ),
        )
        .withColumn(
            "province_id",
            when(col("prov_id").isNotNull(), col("prov_id")).otherwise(lit(-1)),
        )
        .drop("prov_key", "prov_id", "prov_name")
    )

    # Join District
    df_with_district = (
        df_with_province.join(
            districts_df,
            (col("my_district_key") == col("dist_key"))
            & (col("my_province_key") == col("dist_prov_key")),
            "left",
        )
        .withColumn(
            "district",
            when(col("dist_id").isNotNull(), col("dist_name")).otherwise(
                when(col("district_raw") != "unknown", col("district_raw")).otherwise(
                    lit("unknown")
                )
            ),
        )
        .withColumn(
            "district_id",
            when(col("dist_id").isNotNull(), col("dist_id")).otherwise(lit(-1)),
        )
        .drop("dist_key", "dist_prov_key", "dist_id", "dist_name")
    )

    # Join Ward
    df_with_ward = (
        df_with_district.join(
            wards_df,
            (col("my_ward_key") == col("ward_key"))
            & (col("my_district_key") == col("ward_dist_key"))
            & (col("my_province_key") == col("ward_prov_key")),
            "left",
        )
        .withColumn(
            "ward",
            when(col("ward_id").isNotNull(), col("ward_name")).otherwise(
                when(col("ward_raw") != "unknown", col("ward_raw")).otherwise(
                    lit("unknown")
                )
            ),
        )
        .withColumn(
            "ward_id",
            when(col("ward_id").isNotNull(), col("ward_id")).otherwise(lit(-1)),
        )
        .drop("ward_key", "ward_dist_key", "ward_prov_key", "ward_name")
    )

    # Join Street
    df_with_street = (
        df_with_ward.join(
            streets_df,
            (col("my_street_key") == col("street_key"))
            & (col("my_district_key") == col("street_dist_key"))
            & (col("my_province_key") == col("street_prov_key")),
            "left",
        )
        .withColumn(
            "street",
            when(col("street_id").isNotNull(), col("street_name")).otherwise(
                when(col("street_raw") != "unknown", col("street_raw")).otherwise(
                    lit("unknown")
                )
            ),
        )
        .withColumn(
            "street_id",
            when(col("street_id").isNotNull(), col("street_id")).otherwise(lit(-1)),
        )
        .drop("street_key", "street_dist_key", "street_prov_key", "street_name")
    )

    # Clean up temporary columns
    result_df = df_with_street.drop(
        "parts",
        "parts_count",
        "province_raw",
        "district_raw",
        "ward_raw",
        "street_raw",
        "ward_has_parentheses",
        "district_from_ward",
        "ward_cleaned_parentheses",
        "district_final",
        "ward_final",
        "my_province_key",
        "my_district_key",
        "my_ward_key",
        "my_street_key",
    )

    print("[INFO] ✓ Address parsing completed successfully")
    return result_df
