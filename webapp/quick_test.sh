#!/bin/bash

# Quick test script for Real Estate API
echo "🏠 Real Estate API Quick Test"
echo "=============================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

BASE_URL="http://localhost:8000"

# Function to test endpoint
test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3

    echo -e "${BLUE}Testing: $method $endpoint${NC}"

    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$BASE_URL$endpoint")
    else
        response=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$BASE_URL$endpoint")
    fi

    # Split response and status code
    status_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)

    if [ "$status_code" = "200" ]; then
        echo -e "${GREEN}✅ Status: $status_code${NC}"
        # Show first 100 chars of response
        echo "   Response: $(echo "$body" | cut -c1-100)..."
    else
        echo -e "${RED}❌ Status: $status_code${NC}"
        echo "   Error: $body"
    fi
    echo
}

echo "🔍 Testing API endpoints..."
echo

# Test health endpoints
echo -e "${YELLOW}=== HEALTH & INFO ===${NC}"
test_endpoint "GET" "/api/health/"
test_endpoint "GET" "/api/info/"

# Test properties endpoints
echo -e "${YELLOW}=== PROPERTIES ===${NC}"
test_endpoint "GET" "/api/properties/api/properties/?limit=3"
test_endpoint "GET" "/api/properties/api/provinces/"

# Test prediction info endpoints
echo -e "${YELLOW}=== PREDICTION INFO ===${NC}"
test_endpoint "GET" "/api/prediction/model-info/"
test_endpoint "GET" "/api/prediction/feature-info/"

# Test prediction with sample data
echo -e "${YELLOW}=== PREDICTION ===${NC}"
SAMPLE_DATA='{
    "area": 80.0,
    "latitude": 10.8454852487382,
    "longitude": 106.718556132489,
    "bedroom": 3,
    "bathroom": 2,
    "floor_count": 3,
    "house_direction_code": 3,
    "legal_status_code": 1,
    "interior_code": 2,
    "province_id": 2,
    "district_id": 28,
    "ward_id": -1
}'

test_endpoint "POST" "/api/prediction/linear-regression/" "$SAMPLE_DATA"

# Test search endpoints
echo -e "${YELLOW}=== SEARCH ===${NC}"
test_endpoint "GET" "/api/search/advanced/?price_min=1000000000&price_max=3000000000&limit=3"
test_endpoint "GET" "/api/search/suggestions/?q=Quận"

# Test analytics endpoints
echo -e "${YELLOW}=== ANALYTICS ===${NC}"
test_endpoint "GET" "/api/analytics/market-overview/"
test_endpoint "GET" "/api/analytics/dashboard-summary/"

echo -e "${GREEN}🏁 Testing completed!${NC}"
echo
echo "📋 Additional testing options:"
echo "   • Run Python test suite: python3 test_api.py"
echo "   • Import Postman collection: Real_Estate_API.postman_collection.json"
echo "   • Start container: docker compose up backend"
echo "   • View logs: docker logs realestate_backend"
