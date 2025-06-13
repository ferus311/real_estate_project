#!/usr/bin/env python3
"""
Validation script for serving database load process
"""

import psycopg2
import sys
from datetime import datetime


def validate_database_setup(config):
    """Validate database setup before loading"""
    print("🔍 Validating database setup...")

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        cursor = conn.cursor()

        # 1. Check if properties table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'properties'
            );
        """
        )

        if not cursor.fetchone()[0]:
            print("❌ Properties table does not exist!")
            print("💡 Run: ./scripts/setup_serving_db.sh")
            return False

        # 2. Check table structure
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'properties'
            ORDER BY ordinal_position;
        """
        )

        columns = cursor.fetchall()
        required_columns = [
            "id",
            "source",
            "title",
            "province_id",
            "district_id",
            "area",
            "price",
            "bedroom",
            "bathroom",
            "floor_count",
        ]

        existing_columns = [col[0] for col in columns]
        missing_columns = [
            col for col in required_columns if col not in existing_columns
        ]

        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False

        # 3. Check indexes
        cursor.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'properties'
            AND schemaname = 'public';
        """
        )

        indexes = [row[0] for row in cursor.fetchall()]
        print(f"📊 Found {len(indexes)} indexes on properties table")

        print("✅ Database validation passed!")
        return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    finally:
        if "conn" in locals():
            conn.close()


def validate_load_results(config, expected_count=None):
    """Validate data after loading"""
    print("🔍 Validating load results...")

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        cursor = conn.cursor()

        # 1. Count total records
        cursor.execute("SELECT COUNT(*) FROM properties;")
        total_count = cursor.fetchone()[0]
        print(f"📊 Total records: {total_count:,}")

        if expected_count and total_count < expected_count * 0.9:
            print(
                f"⚠️ Loaded records ({total_count:,}) is significantly less than expected ({expected_count:,})"
            )

        # 2. Check data quality
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN title IS NULL OR title = '' THEN 1 END) as missing_title,
                COUNT(CASE WHEN source IS NULL OR source = '' THEN 1 END) as missing_source,
                COUNT(CASE WHEN province_id IS NULL THEN 1 END) as missing_province,
                COUNT(CASE WHEN price IS NOT NULL AND price > 0 THEN 1 END) as has_price,
                COUNT(CASE WHEN area IS NOT NULL AND area > 0 THEN 1 END) as has_area
            FROM properties;
        """
        )

        quality = cursor.fetchone()
        print(f"📊 Data Quality:")
        print(f"  - Missing title: {quality[1]:,} ({quality[1]/quality[0]*100:.1f}%)")
        print(f"  - Missing source: {quality[2]:,} ({quality[2]/quality[0]*100:.1f}%)")
        print(
            f"  - Missing province: {quality[3]:,} ({quality[3]/quality[0]*100:.1f}%)"
        )
        print(f"  - Has price: {quality[4]:,} ({quality[4]/quality[0]*100:.1f}%)")
        print(f"  - Has area: {quality[5]:,} ({quality[5]/quality[0]*100:.1f}%)")

        # 3. Check recent data
        cursor.execute(
            """
            SELECT
                source,
                COUNT(*) as count,
                MAX(created_at) as latest_load
            FROM properties
            GROUP BY source
            ORDER BY count DESC;
        """
        )

        sources = cursor.fetchall()
        print(f"📊 Data by source:")
        for source, count, latest in sources:
            print(f"  - {source}: {count:,} records (latest: {latest})")

        print("✅ Load validation completed!")
        return True

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    # Database configuration
    config = {
        "host": "localhost",
        "port": "5432",
        "database": "realestate",
        "user": "postgres",
        "password": "password",
    }

    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--setup":
            success = validate_database_setup(config)
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "--results":
            expected = int(sys.argv[2]) if len(sys.argv) > 2 else None
            success = validate_load_results(config, expected)
            sys.exit(0 if success else 1)

    # Default: run both validations
    print("🚀 Running complete validation...")
    setup_ok = validate_database_setup(config)
    if setup_ok:
        results_ok = validate_load_results(config)
        sys.exit(0 if (setup_ok and results_ok) else 1)
    else:
        sys.exit(1)
