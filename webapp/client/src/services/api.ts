import axios from 'axios';

// API Base URL
const API_BASE_URL = 'http://localhost:8000/api';

// Create axios instance
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
apiClient.interceptors.request.use(
  (config: any) => {
    console.log('🚀 Making API request:', config.method?.toUpperCase(), config.url);
    return config;
  },
  (error: any) => {
    return Promise.reject(error);
  }
);

// Response interceptor
apiClient.interceptors.response.use(
  (response: any) => {
    console.log('✅ API response:', response.status, response.config.url);
    return response;
  },
  (error: any) => {
    console.error('❌ API error:', error.response?.status, error.config?.url, error.message);
    return Promise.reject(error);
  }
);

// Types
export interface PredictionInput {
  area: number;
  latitude: number;
  longitude: number;
  bedroom: number;
  bathroom: number;
  floor_count: number;
  house_direction_code: number;
  legal_status_code: number;
  interior_code: number;
  province_id: number;
  district_id: number;
  ward_id: number;
}

export interface PredictionResult {
  success: boolean;
  model: string;
  predicted_price: number;
  predicted_price_formatted: string;
  model_metrics: {
    r2: number;
    rmse: number;
    mae: number;
  };
  input_features: PredictionInput;
  error?: string;
}

export interface Property {
  id: string;
  title: string;
  price: number;
  area: number;
  bedroom: number;
  bathroom: number;
  address: string;
  province: string;
  district: string;
  ward?: string;
  street?: string;
  latitude?: number;
  longitude?: number;
  images?: string[];
  description?: string;
}

export interface SearchFilters {
  province?: string;
  district?: string;
  ward?: string;
  price_min?: number;
  price_max?: number;
  area_min?: number;
  area_max?: number;
  bedroom_min?: number;
  bedroom_max?: number;
  bathroom_min?: number;
  bathroom_max?: number;
  limit?: number;
  page?: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  results?: T[];
  count?: number;
  next?: string;
  previous?: string;
  error?: string;
}

// API Services
export const realEstateAPI = {
  // Health check
  healthCheck: async (): Promise<any> => {
    const response = await apiClient.get('/health/');
    return response.data;
  },

  // API info
  getApiInfo: async (): Promise<any> => {
    const response = await apiClient.get('/info/');
    return response.data;
  },

  // Prediction APIs
  prediction: {
    // Get model info
    getModelInfo: async (): Promise<any> => {
      const response = await apiClient.get('/prediction/model-info/');
      return response.data;
    },

    // Get feature info
    getFeatureInfo: async (): Promise<any> => {
      const response = await apiClient.get('/prediction/feature-info/');
      return response.data;
    },

    // Linear Regression prediction
    predictLinearRegression: async (data: PredictionInput): Promise<PredictionResult> => {
      const response = await apiClient.post('/prediction/linear-regression/', data);
      return response.data;
    },

    // XGBoost prediction
    predictXGBoost: async (data: PredictionInput): Promise<PredictionResult> => {
      const response = await apiClient.post('/prediction/xgboost/', data);
      return response.data;
    },

    // LightGBM prediction
    predictLightGBM: async (data: PredictionInput): Promise<PredictionResult> => {
      const response = await apiClient.post('/prediction/lightgbm/', data);
      return response.data;
    },

    // Ensemble prediction
    predictEnsemble: async (data: PredictionInput): Promise<PredictionResult> => {
      const response = await apiClient.post('/prediction/ensemble/', data);
      return response.data;
    },

    // Predict with selected models (XGBoost, LightGBM only)
    predictAll: async (data: PredictionInput): Promise<{
      xgboost: PredictionResult;
      lightgbm: PredictionResult;
      average?: number;
    }> => {
      const [xgb, lgb] = await Promise.allSettled([
        realEstateAPI.prediction.predictXGBoost(data),
        realEstateAPI.prediction.predictLightGBM(data),
      ]);

      const results: {
        xgboost: PredictionResult;
        lightgbm: PredictionResult;
        average?: number;
      } = {
        xgboost: xgb.status === 'fulfilled' ? xgb.value : { success: false, error: 'Failed to predict' } as PredictionResult,
        lightgbm: lgb.status === 'fulfilled' ? lgb.value : { success: false, error: 'Failed to predict' } as PredictionResult,
      };

      // Calculate average from successful predictions
      const successfulPredictions = [
        results.xgboost,
        results.lightgbm,
      ].filter(r => r.success && r.predicted_price);

      if (successfulPredictions.length > 0) {
        const average = successfulPredictions.reduce((sum, r) => sum + r.predicted_price, 0) / successfulPredictions.length;
        results.average = average;
      }

      return results;
    },
  },

  // Properties APIs
  properties: {
    // List properties
    list: async (params?: { limit?: number; page?: number }): Promise<ApiResponse<Property>> => {
      const response = await apiClient.get('/properties/api/properties/', { params });
      return response.data;
    },

    // Get property detail
    getById: async (id: string): Promise<Property> => {
      const response = await apiClient.get(`/properties/api/properties/${id}/`);
      return response.data;
    },

    // List provinces
    getProvinces: async (): Promise<any[]> => {
      const response = await apiClient.get('/properties/api/provinces/');
      return response.data.results;
    },

    // List districts
    getDistricts: async (provinceId?: number): Promise<any[]> => {
      const params = provinceId ? { province_id: provinceId } : {};
      const response = await apiClient.get('/properties/api/districts/', { params });
      return response.data.results;
    },

    // List wards
    getWards: async (districtId?: number): Promise<any[]> => {
      const params = districtId ? { district_id: districtId } : {};
      const response = await apiClient.get('/properties/api/wards/', { params });
      return response.data.results;
    },

    // List streets
    getStreets: async (wardId?: number): Promise<any[]> => {
      const params = wardId ? { ward_id: wardId } : {};
      const response = await apiClient.get('/properties/api/streets/', { params });
      return response.data.results;
    },
  },

  // Search APIs
  search: {
    // Advanced search
    advanced: async (filters: SearchFilters): Promise<ApiResponse<Property>> => {
      const response = await apiClient.get('/search/advanced/', { params: filters });
      return response.data;
    },

    // Location search
    byLocation: async (provinceId?: number, districtId?: number, wardId?: number): Promise<ApiResponse<Property>> => {
      const params = { province_id: provinceId, district_id: districtId, ward_id: wardId };
      const response = await apiClient.get('/search/location/', { params });
      return response.data;
    },

    // Radius search
    byRadius: async (lat: number, lng: number, radius: number = 5): Promise<ApiResponse<Property>> => {
      const params = { lat, lng, radius };
      const response = await apiClient.get('/search/radius/', { params });
      return response.data;
    },

    // Search suggestions
    suggestions: async (query: string): Promise<any[]> => {
      const response = await apiClient.get('/search/suggestions/', { params: { q: query } });
      return response.data.suggestions;
    },

    // Property detail
    propertyDetail: async (propertyId: string): Promise<Property> => {
      const response = await apiClient.get(`/search/property/${propertyId}/`);
      return response.data;
    },
  },

  // Analytics APIs
  analytics: {
    // Market overview
    marketOverview: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/market-overview/');
      return response.data;
    },

    // Price distribution
    priceDistribution: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/price-distribution/');
      return response.data;
    },

    // Property type stats
    propertyTypeStats: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/property-type-stats/');
      return response.data;
    },

    // Location stats
    locationStats: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/location-stats/');
      return response.data;
    },

    // Price trends
    priceTrends: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/price-trends/');
      return response.data;
    },

    // Area distribution
    areaDistribution: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/area-distribution/');
      return response.data;
    },

    // Dashboard summary
    dashboardSummary: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/dashboard-summary/');
      return response.data;
    },
  },
};

export default realEstateAPI;
