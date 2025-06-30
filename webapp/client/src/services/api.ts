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
  description?: string;
  price: number;
  area: number;
  price_per_m2: number;
  province: string;
  district: string;
  ward: string;
  street: string;
  latitude: number;
  longitude: number;
  bedroom: number;
  bathroom: number;
  floor_count: number;
  house_type: string;
  house_direction: string;
  legal_status: string;
  interior: string;
  posted_date?: string | null;
  source: string;
  url: string;
  price_formatted: string;
}

// Utility function to create full address from Property fields
// Tạo địa chỉ đầy đủ từ các trường của Property: street, ward, district, province
export const getPropertyAddress = (property: Property): string => {
  const parts = [property.street, property.ward, property.district, property.province];
  return parts.filter(part => part && part.trim() !== '').join(', ');
};

// Utility function to create short address (without street)
// Tạo địa chỉ ngắn gọn (không bao gồm đường/phố): ward, district, province
export const getPropertyShortAddress = (property: Property): string => {
  const parts = [property.ward, property.district, property.province];
  return parts.filter(part => part && part.trim() !== '').join(', ');
};

// Extended Property interface with computed address field
// Interface Property mở rộng với trường address được tính toán
export interface PropertyWithAddress extends Property {
  address: string;
  shortAddress: string;
}

// Function to convert Property to PropertyWithAddress
// Hàm chuyển đổi Property thành PropertyWithAddress với address tự động tạo
export const addAddressToProperty = (property: Property): PropertyWithAddress => {
  return {
    ...property,
    address: getPropertyAddress(property),
    shortAddress: getPropertyShortAddress(property),
  };
};
export interface SearchFilters {
  province?: string;
  district?: string;
  ward?: string;
  street?: string;
  keyword?: string;
  price_min?: number;
  price_max?: number;
  area_min?: number;
  area_max?: number;
  bedroom_min?: number;
  bedroom_max?: number;
  bedroom?: number;
  bathroom_min?: number;
  bathroom_max?: number;
  limit?: number;
  page?: number;
  page_size?: number;
  sort_by?: string;
  sort_order?: string;
  offset?: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  results?: T[];
  count?: number;
  next?: string;
  previous?: string;
  error?: string;
  pagination?: {
    page: number;
    page_size: number;
    total_count: number;
    total_pages: number;
    has_next: boolean;
    has_previous: boolean;
  };
  search_params?: any;
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

    // Model Management APIs
    reloadModels: async (forceReload: boolean = false): Promise<any> => {
      const response = await apiClient.post('/prediction/reload_models/', { force_reload: forceReload });
      return response.data;
    },

    loadModelsByDate: async (targetDate: string, propertyType: string = 'house'): Promise<any> => {
      const response = await apiClient.post('/prediction/load_models_by_date/', {
        target_date: targetDate,
        property_type: propertyType
      });
      return response.data;
    },

    getAvailableDates: async (propertyType: string = 'house'): Promise<any> => {
      const response = await apiClient.get('/prediction/available_dates/', {
        params: { property_type: propertyType }
      });
      return response.data;
    },

    getCurrentModelInfo: async (): Promise<any> => {
      const response = await apiClient.get('/prediction/model-info/');
      return response.data;
    },

    predictWithLatestModel: async (data: PredictionInput & { model_name: string }): Promise<any> => {
      const response = await apiClient.post('/prediction/predict_latest/', data);
      return response.data;
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
    marketOverview: async (params?: { province?: string; district?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/market-overview/', { params });
      return response.data;
    },

    // Price distribution
    priceDistribution: async (params?: { province?: string; district?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/price-distribution/', { params });
      return response.data;
    },

    // Property type stats
    propertyTypeStats: async (params?: { province?: string; district?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/property-type-stats/', { params });
      return response.data;
    },

    // Location stats
    locationStats: async (params?: { province?: string; district?: string; level?: string; limit?: number }): Promise<any> => {
      const response = await apiClient.get('/analytics/location-stats/', { params });
      return response.data;
    },

    // Price trends
    priceTrends: async (params?: { province?: string; district?: string; months?: number }): Promise<any> => {
      const response = await apiClient.get('/analytics/price-trends/', { params });
      return response.data;
    },

    // Area distribution
    areaDistribution: async (params?: { province?: string; district?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/area-distribution/', { params });
      return response.data;
    },

    // Dashboard summary
    dashboardSummary: async (params?: { province?: string; district?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/dashboard-summary/', { params });
      return response.data;
    },

    // Enhanced analytics APIs
    priceDistributionByLocation: async (params: { province_id: string; district_id?: string }): Promise<any> => {
      const response = await apiClient.get('/analytics/price_distribution_by_location/', { params });
      return response.data;
    },

    marketOverviewEnhanced: async (): Promise<any> => {
      const response = await apiClient.get('/analytics/market_overview_enhanced/');
      return response.data;
    },

    districtComparison: async (provinceId: string): Promise<any> => {
      const response = await apiClient.get('/analytics/district_comparison/', {
        params: { province_id: provinceId }
      });
      return response.data;
    },

    refreshModel: async (): Promise<any> => {
      const response = await apiClient.post('/analytics/refresh_model/');
      return response.data;
    },
  },

  // Model Management APIs (moved to prediction section)
};

export default realEstateAPI;
