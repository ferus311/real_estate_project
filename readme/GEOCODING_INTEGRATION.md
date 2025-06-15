# Geocoding Integration Documentation

## Overview

The Home.tsx component now includes Google Geocoding API integration for better location handling and map display.

## Features Added

### 1. Street Address Input

-   Added a "Đường/Số nhà" (Street/House Number) field
-   Users can input their street address for more precise location
-   Not required for price prediction, but helps with coordinate conversion

### 2. Address to Coordinates Conversion

-   Click the 📍 button next to the street input to convert address to coordinates
-   Uses Google Geocoding API to find exact coordinates based on:
    -   Street address
    -   Ward (if selected)
    -   District
    -   Province
-   Automatically updates latitude/longitude fields

### 3. Interactive Google Map

-   Real Google Maps integration with:
    -   Draggable marker for precise location selection
    -   Click anywhere on map to set new location
    -   "Vị trí hiện tại" button to use device GPS
    -   Automatic reverse geocoding (shows address when coordinates change)

### 4. Coordinates to Address Display

-   When user selects a location on the map, automatically shows the corresponding address
-   Helpful for users to verify their selected location

## Environment Setup

### .env File

```
VITE_GOOGLE_GEOCODING_API_KEY=AIzaSyB76D5ENfgqeMpJtWgJ4ASeoRDRzPcoTaI
```

Note: The API key is prefixed with `VITE_` as required by Vite for client-side environment variables.

## Usage Flow

1. **Address Input Method:**

    - User selects Province → District → Ward
    - User enters street address
    - Clicks 📍 button to convert to coordinates
    - Map updates to show the location

2. **Map Selection Method:**

    - User clicks anywhere on the map
    - Coordinates update automatically
    - Reverse geocoding shows the address for reference
    - User can drag the marker for fine-tuning

3. **GPS Method:**
    - User clicks "Vị trí hiện tại" on the map
    - Browser requests location permission
    - Current GPS coordinates are used
    - Map centers on user's location

## Technical Details

### Files Created/Modified:

-   `src/services/geocoding.ts` - Geocoding API service
-   `src/components/GoogleMapComponent.tsx` - Interactive map component
-   `src/pages/Home.tsx` - Main form with geocoding integration
-   `.env` - Environment variables

### API Calls:

-   Forward Geocoding: Address → Coordinates
-   Reverse Geocoding: Coordinates → Address
-   Google Maps JavaScript API for interactive map

### Error Handling:

-   API key validation
-   Network error handling
-   Invalid address handling
-   GPS permission handling

## Benefits

1. **User Experience:**

    - No need to manually find coordinates
    - Visual map confirmation
    - Multiple input methods (address, map click, GPS)

2. **Accuracy:**

    - Google's precise geocoding
    - Visual verification on map
    - Fine-tuning with draggable marker

3. **Flexibility:**
    - Works with or without street address
    - Fallback to manual coordinate input
    - Quick location buttons for major cities
