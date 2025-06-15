// Google Geocoding API utilities
const GOOGLE_GEOCODING_API_KEY = import.meta.env.VITE_GOOGLE_GEOCODING_API_KEY;

export interface GeocodeResult {
    latitude: number;
    longitude: number;
    formatted_address: string;
    address_components: any[];
}

export interface ReverseGeocodeResult {
    formatted_address: string;
    address_components: any[];
}

/**
 * Convert address text to coordinates using Google Geocoding API
 */
export const geocodeAddress = async (address: string): Promise<GeocodeResult | null> => {
    if (!GOOGLE_GEOCODING_API_KEY) {
        console.error('Google Geocoding API key not found');
        return null;
    }

    try {
        const response = await fetch(
            `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${GOOGLE_GEOCODING_API_KEY}&region=vn`
        );

        const data = await response.json();

        if (data.status === 'OK' && data.results.length > 0) {
            const result = data.results[0];
            return {
                latitude: result.geometry.location.lat,
                longitude: result.geometry.location.lng,
                formatted_address: result.formatted_address,
                address_components: result.address_components,
            };
        } else {
            console.error('Geocoding failed:', data.status);
            return null;
        }
    } catch (error) {
        console.error('Error geocoding address:', error);
        return null;
    }
};

/**
 * Convert coordinates to address using Google Reverse Geocoding API
 */
export const reverseGeocode = async (lat: number, lng: number): Promise<ReverseGeocodeResult | null> => {
    if (!GOOGLE_GEOCODING_API_KEY) {
        console.error('Google Geocoding API key not found');
        return null;
    }

    try {
        const response = await fetch(
            `https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${lng}&key=${GOOGLE_GEOCODING_API_KEY}&region=vn`
        );

        const data = await response.json();

        if (data.status === 'OK' && data.results.length > 0) {
            const result = data.results[0];
            return {
                formatted_address: result.formatted_address,
                address_components: result.address_components,
            };
        } else {
            console.error('Reverse geocoding failed:', data.status);
            return null;
        }
    } catch (error) {
        console.error('Error reverse geocoding:', error);
        return null;
    }
};

/**
 * Build full address string from form values and street
 */
export const buildFullAddress = (
    street: string,
    wardName: string,
    districtName: string,
    provinceName: string
): string => {
    const parts = [street, wardName, districtName, provinceName, 'Vietnam'].filter(Boolean);
    return parts.join(', ');
};
