#!/usr/bin/env python3
"""
Export address parsing results to separate CSV files for Chotot and Batdongsan
Tạo 2 file kết quả parsing cho Chotot và Batdongsan

This script now uses the improved address parser with:
- Unicode normalization for accurate Vietnamese text comparison
- Default JSON path configuration
- Enhanced hierarchical mapping with street disambiguation
"""

import sys
import os
import pandas as pd
import re
from datetime import datetime


from address_parser import (
    load_vietnamese_address_data,
    standardize_address_component_manual,
    normalize_vietnamese_text,
)


def parse_address_manual(
    address_string,
    source,
    existing_ward=None,
    existing_district=None,
    existing_province=None,
    address_data=None,
):
    """
    Enhanced address parsing using improved standardize_address_component_manual
    """
    if not address_string or pd.isna(address_string):
        return {
            "street": "unknown",
            "ward": "unknown",
            "district": "unknown",
            "province": "unknown",
            "street_id": -1,
            "ward_id": -1,
            "district_id": -1,
            "province_id": -1,
        }

    # Clean và split address
    address_string = str(address_string).strip()
    parts = [p.strip() for p in address_string.split(",")]

    result = {
        "street": "unknown",
        "ward": "unknown",
        "district": "unknown",
        "province": "unknown",
        "street_id": -1,
        "ward_id": -1,
        "district_id": -1,
        "province_id": -1,
    }

    # Extract components using existing fields first (for Chotot)
    if existing_province and str(existing_province).strip():
        province = str(existing_province).strip()
        result["province"] = province
    elif len(parts) >= 1:
        # Extract province (last part)
        province = parts[-1]
        result["province"] = province if province else "unknown"

    if existing_district and str(existing_district).strip():
        district = str(existing_district).strip()
        result["district"] = district
    elif len(parts) >= 2:
        # Extract district (second to last)
        district = parts[-2]
        result["district"] = district if district else "unknown"

    if existing_ward and str(existing_ward).strip():
        ward = str(existing_ward).strip()
        result["ward"] = ward
    elif len(parts) >= 3:
        # Extract ward (third to last)
        ward = parts[-3]
        result["ward"] = ward if ward else "unknown"

    # Handle special ward cases like "(Quận 9 cũ)" for Chotot
    ward_value = result["ward"]
    if ward_value and "(" in ward_value and ")" in ward_value:
        # Extract content inside parentheses
        parentheses_match = re.search(r"\((.*?)\)", ward_value)
        if parentheses_match:
            parentheses_content = parentheses_match.group(1).strip()

            # Check if parentheses contain district info
            if re.search(r"(?i)(quận|huyện|thị xã|thành phố)", parentheses_content):
                # Extract district from parentheses, remove "cũ"
                district_from_parentheses = re.sub(
                    r"(?i)\s*cũ\s*", "", parentheses_content
                ).strip()

                # Override district with the one from parentheses
                if district_from_parentheses:
                    result["district"] = district_from_parentheses

            # Remove parentheses from ward
            result["ward"] = re.sub(r"\s*\(.*?\)\s*", "", ward_value).strip()
            if not result["ward"]:
                result["ward"] = "unknown"

    # Street extraction - parse from bottom-up, ignore extra parts at beginning
    if len(parts) >= 4:
        # Extract street from 4th to last position
        street = parts[-4]
        result["street"] = street if street else "unknown"
    elif len(parts) == 3:
        # Only ward, district, province -> no street
        result["street"] = "unknown"

    # Clean empty values
    for key in result:
        if key.endswith("_id"):
            continue  # Keep IDs as they are
        if not result[key] or result[key].strip() == "":
            result[key] = "unknown"

    # Use improved standardization with hierarchical mapping if address_data available
    if address_data:
        # Standardize province
        if result["province"] != "unknown":
            province_result = standardize_address_component_manual(
                result["province"], "province", address_mapping=address_data
            )
            result["province"] = province_result["name"]
            result["province_id"] = province_result["id"]

        # Standardize district with province context
        if result["district"] != "unknown":
            district_result = standardize_address_component_manual(
                result["district"],
                "district",
                province_context=(
                    result["province"] if result["province"] != "unknown" else None
                ),
                address_mapping=address_data,
            )
            result["district"] = district_result["name"]
            result["district_id"] = district_result["id"]

        # Standardize ward with district and province context
        if result["ward"] != "unknown":
            ward_result = standardize_address_component_manual(
                result["ward"],
                "ward",
                district_context=(
                    result["district"] if result["district"] != "unknown" else None
                ),
                province_context=(
                    result["province"] if result["province"] != "unknown" else None
                ),
                address_mapping=address_data,
            )
            result["ward"] = ward_result["name"]
            result["ward_id"] = ward_result["id"]

        # Standardize street with enhanced hierarchical mapping
        if result["street"] != "unknown":
            street_result = standardize_address_component_manual(
                result["street"],
                "street",
                district_context=(
                    result["district"] if result["district"] != "unknown" else None
                ),
                province_context=(
                    result["province"] if result["province"] != "unknown" else None
                ),
                address_mapping=address_data,
            )
            result["street"] = street_result["name"]
            result["street_id"] = street_result["id"]

    return result


def process_chotot_data(address_data, output_dir, num_records=100):
    """Process Chotot data and export results"""
    print("=== PROCESSING CHOTOT DATA ===")

    # Load Chotot CSV
    csv_path = "/home/fer/data/real_estate_project/tmp/raw_data/chotot_data_may2025.csv"
    try:
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} records from Chotot data")
    except Exception as e:
        print(f"✗ Error loading Chotot data: {e}")
        return

    # Process first N records
    df_sample = df.head(num_records).copy()

    results = []
    for idx, row in df_sample.iterrows():
        location = row.get("location", "")

        # Parse address
        result = parse_address_manual(
            location,
            "chotot",
            existing_ward=None,
            existing_district=None,
            existing_province=None,
            address_data=address_data,
        )

        # Combine original data with parsed results
        record = {
            "original_id": idx,
            "source": "chotot",
            "original_location": location,
            "area": row.get("area", ""),
            "price": row.get("price", ""),
            "title": row.get("title", ""),
            # Parsed address fields
            "street": result["street"],
            "ward": result["ward"],
            "district": result["district"],
            "province": result["province"],
            "street_id": result["street_id"],
            "ward_id": result["ward_id"],
            "district_id": result["district_id"],
            "province_id": result["province_id"],
        }
        results.append(record)

        if (idx + 1) % 20 == 0:
            print(f"  Processed {idx + 1} records...")

    # Save to CSV
    results_df = pd.DataFrame(results)
    output_file = os.path.join(
        output_dir,
        f"chotot_address_parsing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )
    results_df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"✓ Exported {len(results)} Chotot records to: {output_file}")

    # Print summary stats
    print("\nChotot Summary:")
    print(f"  - Records processed: {len(results)}")
    print(
        f"  - Records with street ID: {len([r for r in results if r['street_id'] != -1])}"
    )
    print(
        f"  - Records with ward ID: {len([r for r in results if r['ward_id'] != -1])}"
    )
    print(
        f"  - Records with district ID: {len([r for r in results if r['district_id'] != -1])}"
    )
    print(
        f"  - Records with province ID: {len([r for r in results if r['province_id'] != -1])}"
    )

    return output_file


def process_batdongsan_data(address_data, output_dir, num_records=100):
    """Process Batdongsan data and export results"""
    print("\n=== PROCESSING BATDONGSAN DATA ===")

    # Load Batdongsan CSV
    csv_path = "/home/fer/data/real_estate_project/tmp/raw_data/bds_data_may2025.csv"
    try:
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} records from Batdongsan data")
    except Exception as e:
        print(f"✗ Error loading Batdongsan data: {e}")
        return

    # Process first N records
    df_sample = df.head(num_records).copy()

    results = []
    for idx, row in df_sample.iterrows():
        location = row.get("location", "")

        # Parse address
        result = parse_address_manual(location, "batdongsan", address_data=address_data)

        # Combine original data with parsed results
        record = {
            "original_id": idx,
            "source": "batdongsan",
            "original_location": location,
            # Parsed address fields
            "street": result["street"],
            "ward": result["ward"],
            "district": result["district"],
            "province": result["province"],
            "street_id": result["street_id"],
            "ward_id": result["ward_id"],
            "district_id": result["district_id"],
            "province_id": result["province_id"],
        }
        results.append(record)

        if (idx + 1) % 20 == 0:
            print(f"  Processed {idx + 1} records...")

    # Save to CSV
    results_df = pd.DataFrame(results)
    output_file = os.path.join(
        output_dir,
        f"batdongsan_address_parsing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )
    results_df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"✓ Exported {len(results)} Batdongsan records to: {output_file}")

    # Print summary stats
    print("\nBatdongsan Summary:")
    print(f"  - Records processed: {len(results)}")
    print(
        f"  - Records with street ID: {len([r for r in results if r['street_id'] != -1])}"
    )
    print(
        f"  - Records with ward ID: {len([r for r in results if r['ward_id'] != -1])}"
    )
    print(
        f"  - Records with district ID: {len([r for r in results if r['district_id'] != -1])}"
    )
    print(
        f"  - Records with province ID: {len([r for r in results if r['province_id'] != -1])}"
    )

    return output_file


def main():
    """Main function to export address parsing results"""
    print("VIETNAMESE ADDRESS PARSER - EXPORT RESULTS (ENHANCED)")
    print("=" * 60)

    # Load Vietnamese address data using default path
    address_data = load_vietnamese_address_data(
        "/home/fer/data/real_estate_project/data_processing/spark/common/utils/address/vietnamaddress_utf8.json"
    )

    if not address_data:
        print("✗ Cannot load Vietnamese address data")
        return

    print(f"✓ Loaded Vietnamese address data with enhanced features:")
    print(f"  - Provinces: {len(address_data['provinces'])} entries")
    print(f"  - Districts: {len(address_data['districts'])} entries")
    print(f"  - Wards: {len(address_data['wards'])} entries")
    print(f"  - Streets: {len(address_data['streets'])} entries")
    print(f"  - Unicode normalization: Enabled")
    print(f"  - Hierarchical mapping: Enhanced")

    # Create output directory
    output_dir = "/home/fer/data/real_estate_project/tmp/address_parsing_results"
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n✓ Output directory: {output_dir}")

    # Process both sources
    num_records = 100  # Process first 100 records for testing

    chotot_file = process_chotot_data(address_data, output_dir, num_records)
    batdongsan_file = process_batdongsan_data(address_data, output_dir, num_records)

    print("\n" + "=" * 60)
    print("✅ EXPORT COMPLETED WITH ENHANCED FEATURES!")
    if chotot_file:
        print(f"📄 Chotot results: {chotot_file}")
    if batdongsan_file:
        print(f"📄 Batdongsan results: {batdongsan_file}")

    print(f"\n💡 Files saved in: {output_dir}")
    print("🔍 Results now include:")
    print("   - Unicode normalized Vietnamese text comparison")
    print("   - Enhanced hierarchical street-district-province mapping")
    print("   - Improved address component standardization")
    print("   - Better disambiguation for same-named streets in different districts")


if __name__ == "__main__":
    main()
