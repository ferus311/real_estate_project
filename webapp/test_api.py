#!/usr/bin/env python3
"""
Test script cho Real Estate API
Chạy để kiểm tra tất cả các API endpoints
"""

import requests
import json
import time
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"
TIMEOUT = 30


def print_section(title):
    """Print section header"""
    print(f"\n{'='*60}")
    print(f"🔥 {title}")
    print(f"{'='*60}")


def print_result(endpoint, status_code, data=None, error=None):
    """Print test result"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    status_emoji = "✅" if status_code == 200 else "❌"

    print(f"{status_emoji} [{timestamp}] {endpoint} - Status: {status_code}")

    if error:
        print(f"   Error: {error}")
    elif data:
        if isinstance(data, dict):
            if "success" in data:
                print(f"   Success: {data.get('success')}")
            if "predicted_price_formatted" in data:
                print(f"   Price: {data.get('predicted_price_formatted')}")
            if "total_count" in data:
                print(f"   Total: {data.get('total_count')}")
            if "properties" in data and isinstance(data["properties"], list):
                print(f"   Properties: {len(data['properties'])} items")
        print(f"   Data: {str(data)[:100]}...")
    print()


def test_health_endpoints():
    """Test health and info endpoints"""
    print_section("HEALTH & INFO ENDPOINTS")

    endpoints = ["/api/health/", "/api/info/"]

    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=TIMEOUT)
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))


def test_prediction_endpoints():
    """Test ML prediction endpoints"""
    print_section("PREDICTION ENDPOINTS")

    # Sample prediction data
    sample_data = {
        "area": 80.0,
        "latitude": 10.762622,
        "longitude": 106.660172,
        "bedroom": 3,
        "bathroom": 2,
        "floor_count": 3,
        "house_direction_code": 3,
        "legal_status_code": 1,
        "interior_code": 2,
        "province_id": 79,
        "district_id": 769,
        "ward_id": 27000,
    }

    # Test info endpoints first (GET)
    info_endpoints = ["/api/prediction/model-info/", "/api/prediction/feature-info/"]

    for endpoint in info_endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=TIMEOUT)
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))

    # Test prediction endpoints (POST)
    prediction_endpoints = [
        "/api/prediction/linear-regression/",
        "/api/prediction/xgboost/",
        "/api/prediction/lightgbm/",
        "/api/prediction/ensemble/",
    ]

    for endpoint in prediction_endpoints:
        try:
            response = requests.post(
                f"{BASE_URL}{endpoint}",
                json=sample_data,
                headers={"Content-Type": "application/json"},
                timeout=TIMEOUT,
            )
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))


def test_search_endpoints():
    """Test search endpoints"""
    print_section("SEARCH ENDPOINTS")

    # Test GET endpoints
    get_endpoints = [
        "/api/search/advanced/?province=Hồ Chí Minh&price_min=2000000000&price_max=5000000000",
        "/api/search/location/?province_id=79&district_id=769",
        "/api/search/radius/?lat=10.762622&lng=106.660172&radius=5",
        "/api/search/suggestions/?q=Quận 1",
    ]

    for endpoint in get_endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=TIMEOUT)
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))

    # Test POST advanced search
    search_data = {
        "province": "Hồ Chí Minh",
        "district": "Quận 1",
        "price_min": 2000000000,
        "price_max": 5000000000,
        "bedroom_min": 2,
        "area_min": 50,
        "sort_by": "price",
        "page": 1,
    }

    try:
        response = requests.post(
            f"{BASE_URL}/api/search/advanced/",
            json=search_data,
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        print_result(
            "/api/search/advanced/ (POST)",
            response.status_code,
            (
                response.json()
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                )
                else response.text
            ),
        )
    except requests.exceptions.RequestException as e:
        print_result("/api/search/advanced/ (POST)", 0, error=str(e))


def test_analytics_endpoints():
    """Test analytics endpoints"""
    print_section("ANALYTICS ENDPOINTS")

    endpoints = [
        "/api/analytics/market-overview/",
        "/api/analytics/price-distribution/",
        "/api/analytics/property-type-stats/",
        "/api/analytics/location-stats/",
        "/api/analytics/price-trends/",
        "/api/analytics/area-distribution/",
        "/api/analytics/dashboard-summary/",
    ]

    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=TIMEOUT)
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))


def test_properties_endpoints():
    """Test core properties endpoints"""
    print_section("PROPERTIES ENDPOINTS")

    endpoints = [
        "/api/properties/api/properties/",
        "/api/properties/api/provinces/",
        "/api/properties/api/districts/",
        "/api/properties/api/wards/",
        "/api/properties/api/streets/",
    ]

    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}?limit=5", timeout=TIMEOUT)
            print_result(
                endpoint,
                response.status_code,
                (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else response.text
                ),
            )
        except requests.exceptions.RequestException as e:
            print_result(endpoint, 0, error=str(e))


def main():
    """Main test function"""
    print("🏠 Real Estate API Test Suite")
    print(f"🎯 Target: {BASE_URL}")
    print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Test all endpoints
    test_health_endpoints()
    test_properties_endpoints()
    test_prediction_endpoints()
    test_search_endpoints()
    test_analytics_endpoints()

    print_section("TEST COMPLETED")
    print(f"🏁 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
