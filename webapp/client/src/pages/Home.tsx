import { useState, useEffect, useCallback } from 'react';
import {
    Card,
    Form,
    InputNumber,
    Select,
    Button,
    Row,
    Col,
    Statistic,
    Spin,
    Divider,
    Tag,
    Tooltip,
    Badge,
    notification,
    Empty,
    Modal,
    Typography,
    message,
} from 'antd';
import {
    HomeOutlined,
    CalculatorOutlined,
    BankOutlined,
    SearchOutlined,
    StarOutlined,
    InfoCircleOutlined,
    TrophyOutlined,
    ThunderboltOutlined,
    EyeOutlined,
    LinkOutlined,
    EnvironmentOutlined,
    CopyOutlined,
} from '@ant-design/icons';
import { useAddressData } from '../hooks/useAddressData';
import GoogleMapComponent from '../components/GoogleMapComponent';
import { geocodeAddress, reverseGeocode, buildFullAddress } from '../services/geocoding';
import realEstateAPI, {
    PredictionInput,
    PredictionResult,
    Property,
    SearchFilters,
    getPropertyAddress,
    getPropertyShortAddress
} from '../services/api';
import {
    HOUSE_DIRECTION_OPTIONS,
    LEGAL_STATUS_OPTIONS,
    INTERIOR_OPTIONS,
    DEFAULT_CODES,
} from '../constants/formOptions';

const { Option } = Select;
const { Text, Title } = Typography;

interface PredictionResults {
    xgboost: PredictionResult;
    lightgbm: PredictionResult;
    average?: number;
}

export default function Home() {
    console.log('🏠 Home component initialized');

    const [form] = Form.useForm();
    const [loading, setLoading] = useState(false);
    const [predictionResults, setPredictionResults] = useState<PredictionResults | null>(null);
    const [properties, setProperties] = useState<Property[]>([]);
    const [loadingProperties, setLoadingProperties] = useState(false);
    const [marketStats, setMarketStats] = useState<any>(null);
    const [selectedProperty, setSelectedProperty] = useState<Property | null>(null);
    const [propertyModalVisible, setPropertyModalVisible] = useState(false);

    const {
        getProvinceOptions,
        getDistrictOptions,
        getWardOptions,
        getStreetOptions,
        getFullAddress,
        loading: addressLoading,
    } = useAddressData();

    console.log('🗺️ Address data status:', {
        addressLoading,
        provinceCount: getProvinceOptions()?.length || 0,
        hasGetDistrictOptions: typeof getDistrictOptions === 'function',
        hasGetWardOptions: typeof getWardOptions === 'function',
        hasGetStreetOptions: typeof getStreetOptions === 'function',
    });

    // Form state
    const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');
    const [selectedWard, setSelectedWard] = useState<string>('');
    const [selectedStreet, setSelectedStreet] = useState<string>('');
    const [geocodingLoading, setGeocodingLoading] = useState(false);
    const [currentCoordinates, setCurrentCoordinates] = useState({ lat: 10.762622, lng: 106.660172 });

    // Auto-geocoding timer
    const [geocodingTimer, setGeocodingTimer] = useState<NodeJS.Timeout | null>(null);

    // Load market stats on mount
    useEffect(() => {
        const loadMarketStats = async () => {
            try {
                const stats = await realEstateAPI.analytics.dashboardSummary();
                setMarketStats(stats.dashboard_summary);
            } catch (error) {
                console.error('Failed to load market stats:', error);
            }
        };
        loadMarketStats();

        // Initialize coordinates from form default values
        const initLat = form.getFieldValue('latitude') || 10.762622;
        const initLng = form.getFieldValue('longitude') || 106.660172;
        setCurrentCoordinates({ lat: initLat, lng: initLng });
    }, []);

    // Handle prediction
    const handlePredict = async (values: any) => {
        setLoading(true);
        try {
            // Lấy tọa độ hiện tại từ form (đã được cập nhật qua handleLocationChange hoặc geocoding)
            const currentLat = form.getFieldValue('latitude') || values.latitude || 10.762622;
            const currentLng = form.getFieldValue('longitude') || values.longitude || 106.660172;

            console.log('🗺️ Using current coordinates for prediction:', { lat: currentLat, lng: currentLng });
            console.log('🗺️ Form coordinates:', { lat: form.getFieldValue('latitude'), lng: form.getFieldValue('longitude') });
            console.log('🗺️ Values coordinates:', { lat: values.latitude, lng: values.longitude });

            const predictionData: PredictionInput = {
                area: values.area,
                latitude: currentLat,  // Sử dụng tọa độ hiện tại
                longitude: currentLng, // Sử dụng tọa độ hiện tại
                bedroom: values.bedroom,
                bathroom: values.bathroom,
                floor_count: values.floor_count,
                house_direction_code: values.house_direction_code,
                legal_status_code: values.legal_status_code,
                interior_code: values.interior_code,
                province_id: parseInt(values.province_id),
                district_id: parseInt(values.district_id),
                ward_id: parseInt(values.ward_id || '-1'),
            };

            console.log('🚀 Starting prediction with data:', predictionData);

            const results = await realEstateAPI.prediction.predictAll(predictionData);

            console.log('✅ Prediction results:', results);

            // Set the results directly (only XGBoost and LightGBM)
            const filteredResults: PredictionResults = {
                xgboost: results.xgboost,
                lightgbm: results.lightgbm,
                average: results.average,
            };

            setPredictionResults(filteredResults);

            console.log('💡 Prediction results before searching similar properties:', {
                xgboost: results.xgboost?.predicted_price,
                lightgbm: results.lightgbm?.predicted_price,
                average: results.average
            });

            // Thêm delay nhỏ để đảm bảo prediction results được xử lý đúng
            setTimeout(async () => {
                // Search for similar properties với tọa độ chính xác từ predictionData
                await searchSimilarProperties(predictionData, filteredResults);
            }, 100); // Delay 100ms

            notification.success({
                message: '🎉 Dự đoán thành công!',
                description: 'Đã dự đoán giá nhà từ XGBoost và LightGBM models',
            });
        } catch (error) {
            console.error('❌ Prediction failed:', error);
            console.error('Error stack:', error instanceof Error ? error.stack : 'No stack trace');

            // Check if this is a navigation error
            if (error instanceof Error && error.message.includes('navigation')) {
                console.error('🚨 Navigation error detected! This might be causing the 404.');
            }

            notification.error({
                message: '❌ Dự đoán thất bại',
                description: 'Có lỗi xảy ra khi dự đoán giá nhà. Vui lòng thử lại.',
            });
        } finally {
            setLoading(false);
        }
    };

    // Search for similar properties
    const searchSimilarProperties = async (predictionData: PredictionInput, predictionResults?: PredictionResults) => {
        setLoadingProperties(true);
        try {
            console.log('🔍 searchSimilarProperties called with coordinates:', {
                lat: predictionData.latitude,
                lng: predictionData.longitude,
                area: predictionData.area,
                province_id: predictionData.province_id,
                district_id: predictionData.district_id,
                ward_id: predictionData.ward_id
            });

            console.log('🗺️ Current coordinates from predictionData:', {
                lat: predictionData.latitude,
                lng: predictionData.longitude
            });

            console.log('🗺️ Current coordinates from form:', {
                lat: form.getFieldValue('latitude'),
                lng: form.getFieldValue('longitude')
            });

            // Use the best prediction or average of successful predictions for price range
            let priceRange = 2000000000; // Default fallback

            // Use passed predictionResults or fallback to state
            const currentResults = predictionResults; // Use fresh results from parameter

            console.log('💰 Current prediction results for search:', currentResults);

            if (currentResults) {
                const validPrices = [];
                if (currentResults.xgboost?.success && currentResults.xgboost?.predicted_price) {
                    validPrices.push(currentResults.xgboost.predicted_price);
                    console.log('✅ XGBoost price:', currentResults.xgboost.predicted_price);
                }
                if (currentResults.lightgbm?.success && currentResults.lightgbm?.predicted_price) {
                    validPrices.push(currentResults.lightgbm.predicted_price);
                    console.log('✅ LightGBM price:', currentResults.lightgbm.predicted_price);
                }

                if (validPrices.length > 0) {
                    priceRange = validPrices.reduce((sum, price) => sum + price, 0) / validPrices.length;
                    console.log('💰 Calculated average price range for search:', formatPrice(priceRange));
                } else {
                    console.log('⚠️ No valid prices found, using default range:', formatPrice(priceRange));
                }
            } else {
                console.log('⚠️ No prediction results provided, using default range:', formatPrice(priceRange));
            }

            // Get province name from the prediction data thay vì từ state cũ
            const provinceOption = (getProvinceOptions() || []).find(p => p.value === predictionData.province_id.toString());
            const districtOption = (getDistrictOptions(predictionData.province_id.toString()) || []).find(d => d.value === predictionData.district_id.toString());

            const filters: SearchFilters = {
                // Tìm theo khu vực gần nhất - ưu tiên district trước, nếu không có thì province
                province: provinceOption?.label || '',
                district: districtOption?.label || '',
                // Khoảng giá rộng hơn để tìm được nhiều nhà hơn: ±30%
                price_min: Math.max(0, priceRange * 0.7),
                price_max: priceRange * 1.3,
                // Diện tích linh hoạt hơn: ±40%
                area_min: Math.max(0, predictionData.area * 0.6),
                area_max: predictionData.area * 1.4,
                // Số phòng linh hoạt: có thể khác ±2 phòng
                bedroom_min: Math.max(1, predictionData.bedroom - 2),
                bedroom_max: predictionData.bedroom + 2,
                // Tăng limit để có nhiều lựa chọn hơn
                limit: 20,
                page: 1,
            };

            console.log('🔍 Searching for similar properties with filters:', filters);
            console.log('💰 Price range for search:', formatPrice(filters.price_min || 0), '-', formatPrice(filters.price_max || 0));
            const response = await realEstateAPI.search.advanced(filters);
            console.log('🏘️ Search response:', response);

            // Lấy properties từ response và sort theo khoảng cách giá
            const foundProperties = response.results || [];

            if (foundProperties && foundProperties.length > 0) {
                // Sort theo độ gần của giá so với giá dự đoán
                const sortedProperties = foundProperties.sort((a, b) => {
                    const diffA = Math.abs(a.price - priceRange);
                    const diffB = Math.abs(b.price - priceRange);
                    return diffA - diffB;
                });

                // Chỉ lấy 12 nhà tương tự nhất
                setProperties(sortedProperties.slice(0, 12));
                console.log('🏘️ Found and sorted similar properties:', sortedProperties.length);
            } else {
                setProperties([]);
                console.log('🏘️ No similar properties found');
            }

        } catch (error) {
            console.error('Failed to search properties:', error);
            message.error('Không thể tìm kiếm nhà tương tự');
            setProperties([]);
        } finally {
            setLoadingProperties(false);
        }
    };

    // Handle location change from map
    const handleLocationChange = (lat: number, lng: number) => {
        form.setFieldsValue({
            latitude: lat,
            longitude: lng,
        });

        // Update coordinate state for real-time display
        setCurrentCoordinates({ lat, lng });

        // Reverse geocode to show address
        handleCoordinatesToAddress(lat, lng);
    };

    // Format price
    const formatPrice = (price: number) => {
        if (price >= 1000000000) {
            return `${(price / 1000000000).toFixed(1)} tỷ VND`;
        } else if (price >= 1000000) {
            return `${(price / 1000000).toFixed(0)} triệu VND`;
        }
        return `${price.toLocaleString()} VND`;
    };

    // Get best prediction (based on predicted price comparison)
    const getBestPrediction = (): { model: string; result: PredictionResult } | null => {
        if (!predictionResults) return null;

        const models = [
            { name: 'XGBoost', result: predictionResults.xgboost },
            { name: 'LightGBM', result: predictionResults.lightgbm },
        ];

        const validModels = models.filter(m => m.result?.success && m.result?.predicted_price);
        if (validModels.length === 0) return null;

        // Return XGBoost as default best (can be enhanced with more sophisticated logic later)
        const xgboostModel = validModels.find(m => m.name === 'XGBoost');
        const bestModel = xgboostModel || validModels[0];
        return { model: bestModel.name, result: bestModel.result };
    };

    // Handle property click
    const handlePropertyClick = (property: Property) => {
        setSelectedProperty(property);
        setPropertyModalVisible(true);
    };

    // Copy URL to clipboard
    const copyUrl = (url: string) => {
        navigator.clipboard.writeText(url);
        notification.success({
            message: 'Đã sao chép URL',
            description: 'URL bất động sản đã được sao chép vào clipboard'
        });
    };

    // Handle address to coordinates conversion
    const handleAddressToCoordinates = async (auto = false) => {
        const customStreet = form.getFieldValue('street'); // Custom street input
        const wardName = selectedWard ? (getWardOptions(selectedProvince, selectedDistrict) || []).find(w => w.value === selectedWard)?.label : '';
        const districtName = selectedDistrict ? (getDistrictOptions(selectedProvince) || []).find(d => d.value === selectedDistrict)?.label : '';
        const provinceName = selectedProvince ? (getProvinceOptions() || []).find(p => p.value === selectedProvince)?.label : '';
        const streetName = selectedStreet ? (getStreetOptions(selectedProvince, selectedDistrict) || []).find(s => s.value === selectedStreet)?.label : '';

        if (!provinceName || !districtName) {
            if (!auto) {
                notification.warning({
                    message: 'Thông tin chưa đầy đủ',
                    description: 'Vui lòng chọn tỉnh/thành phố và quận/huyện',
                });
            }
            return;
        }

        // Use custom street if provided, otherwise use selected street from dropdown
        const finalStreet = customStreet || streetName;

        if (!finalStreet && !auto) {
            notification.warning({
                message: 'Thiếu thông tin đường',
                description: 'Vui lòng nhập địa chỉ đường hoặc chọn từ danh sách',
            });
            return;
        }

        setGeocodingLoading(true);
        try {
            const fullAddress = buildFullAddress(finalStreet || '', wardName || '', districtName || '', provinceName || '');
            const result = await geocodeAddress(fullAddress);

            if (result) {
                form.setFieldsValue({
                    latitude: result.latitude,
                    longitude: result.longitude,
                });

                // Update coordinate state for real-time display
                setCurrentCoordinates({ lat: result.latitude, lng: result.longitude });

                if (!auto) {
                    notification.success({
                        message: 'Chuyển đổi thành công',
                        description: `Đã tìm thấy tọa độ cho địa chỉ: ${result.formatted_address}`,
                    });
                }
            } else {
                if (!auto) {
                    notification.error({
                        message: 'Không tìm thấy địa chỉ',
                        description: 'Không thể chuyển đổi địa chỉ thành tọa độ. Vui lòng kiểm tra lại.',
                    });
                }
            }
        } catch (error) {
            if (!auto) {
                notification.error({
                    message: 'Lỗi chuyển đổi',
                    description: 'Có lỗi xảy ra khi chuyển đổi địa chỉ',
                });
            }
        } finally {
            setGeocodingLoading(false);
        }
    };

    // Handle coordinates to address conversion
    const handleCoordinatesToAddress = async (lat: number, lng: number) => {
        try {
            const result = await reverseGeocode(lat, lng);
            if (result) {
                // Optionally show the formatted address in a notification
                notification.info({
                    message: 'Địa chỉ tương ứng',
                    description: result.formatted_address,
                    duration: 3,
                });
            }
        } catch (error) {
            console.error('Error reverse geocoding:', error);
        }
    };    // Auto-geocoding when address changes
    const autoGeocodeFromAddress = async () => {
        // Need at least province to geocode
        if (!selectedProvince) return;

        setGeocodingLoading(true);

        try {
            const customStreet = form.getFieldValue('street');
            const provinceName = selectedProvince ? (getProvinceOptions() || []).find(p => p.value === selectedProvince)?.label : '';

            // If no district selected, just geocode with province
            if (!selectedDistrict) {
                console.log('Geocoding with province only:', provinceName);
                const result = await geocodeAddress(provinceName || '');
                if (result) {
                    form.setFieldsValue({
                        latitude: result.latitude,
                        longitude: result.longitude,
                    });

                    // Update coordinate state for real-time display
                    setCurrentCoordinates({ lat: result.latitude, lng: result.longitude });

                    // Show subtle notification for auto-geocoding
                    notification.info({
                        message: 'Tự động cập nhật tọa độ',
                        description: `Đã tìm thấy tọa độ cho: ${result.formatted_address}`,
                        duration: 2,
                        placement: 'bottomRight',
                    });
                }
                return;
            }

            const wardName = selectedWard ? (getWardOptions(selectedProvince, selectedDistrict) || []).find(w => w.value === selectedWard)?.label : '';
            const districtName = selectedDistrict ? (getDistrictOptions(selectedProvince) || []).find(d => d.value === selectedDistrict)?.label : '';
            const streetName = selectedStreet ? (getStreetOptions(selectedProvince, selectedDistrict) || []).find(s => s.value === selectedStreet)?.label : '';

            const finalStreet = customStreet || streetName;

            // If no street info, still try to geocode with district + province
            if (!finalStreet && !wardName) {
                // Try to geocode with just district and province
                const basicAddress = `${districtName}, ${provinceName}`;
                const result = await geocodeAddress(basicAddress);
                if (result) {
                    form.setFieldsValue({
                        latitude: result.latitude,
                        longitude: result.longitude,
                    });

                    // Update coordinate state for real-time display
                    setCurrentCoordinates({ lat: result.latitude, lng: result.longitude });

                    console.log('Auto-geocoded basic address:', basicAddress);
                }
                return;
            }

            const fullAddress = buildFullAddress(finalStreet || '', wardName || '', districtName || '', provinceName || '');
            const result = await geocodeAddress(fullAddress);

            if (result) {
                form.setFieldsValue({
                    latitude: result.latitude,
                    longitude: result.longitude,
                });

                // Update coordinate state for real-time display
                setCurrentCoordinates({ lat: result.latitude, lng: result.longitude });

                // Show subtle notification for auto-geocoding
                notification.info({
                    message: 'Tự động cập nhật tọa độ',
                    description: `Đã tìm thấy tọa độ cho: ${result.formatted_address}`,
                    duration: 2,
                    placement: 'bottomRight',
                });
            }
        } catch (error) {
            console.error('Auto-geocoding failed:', error);
            // Don't show error notification for auto-geocoding failures
        } finally {
            setGeocodingLoading(false);
        }
    };

    // Debounced auto-geocoding to avoid too many API calls
    const [autoGeocodeTimeout, setAutoGeocodeTimeout] = useState<NodeJS.Timeout | null>(null);

    const debouncedAutoGeocode = () => {
        if (autoGeocodeTimeout) {
            clearTimeout(autoGeocodeTimeout);
        }

        const timeout = setTimeout(() => {
            autoGeocodeFromAddress();
        }, 800); // Reduce to 0.8 second delay for faster response

        setAutoGeocodeTimeout(timeout);
    };

    // Auto-geocode when address components change
    useEffect(() => {
        // Auto-geocode if we have at least province (even without district)
        if (selectedProvince) {
            console.log('Address changed, triggering auto-geocode:', {
                province: selectedProvince,
                district: selectedDistrict,
                ward: selectedWard,
                street: selectedStreet
            });
            debouncedAutoGeocode();
        }
        return () => {
            if (autoGeocodeTimeout) {
                clearTimeout(autoGeocodeTimeout);
            }
        };
    }, [selectedProvince, selectedDistrict, selectedWard, selectedStreet]);

    // Auto-geocode when custom street changes (with longer delay for typing)
    useEffect(() => {
        const customStreet = form.getFieldValue('street');
        if (selectedProvince && customStreet) {
            console.log('Custom street changed, triggering auto-geocode with delay');
            if (autoGeocodeTimeout) {
                clearTimeout(autoGeocodeTimeout);
            }

            const timeout = setTimeout(() => {
                autoGeocodeFromAddress();
            }, 1500); // Longer delay for typing

            setAutoGeocodeTimeout(timeout);
        }
    }, [form.getFieldValue('street')]);

    // Sync coordinates state with form values
    useEffect(() => {
        const lat = form.getFieldValue('latitude') || 10.762622;
        const lng = form.getFieldValue('longitude') || 106.660172;
        if (lat !== currentCoordinates.lat || lng !== currentCoordinates.lng) {
            setCurrentCoordinates({ lat, lng });
        }
    }, [form.getFieldValue('latitude'), form.getFieldValue('longitude')]);

    // Auto-geocoding with debounce
    const triggerAutoGeocode = useCallback((delay = 1500) => {
        console.log('Triggering auto-geocode with delay:', delay);
        if (geocodingTimer) {
            clearTimeout(geocodingTimer);
        }

        const timer = setTimeout(() => {
            autoGeocodeFromAddress();
        }, delay);

        setGeocodingTimer(timer);
    }, [selectedProvince, selectedDistrict, selectedWard, selectedStreet]);

    // Cleanup timer on unmount
    useEffect(() => {
        return () => {
            if (geocodingTimer) {
                clearTimeout(geocodingTimer);
            }
        };
    }, [geocodingTimer]);

    const bestPrediction = getBestPrediction();

    console.log('🏠 Home component render with state:', {
        loading,
        predictionResults: !!predictionResults,
        selectedProvince,
        selectedDistrict,
        currentCoordinates
    });

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
            {/* Add error boundary wrapper */}
            <div style={{ position: 'relative', minHeight: '100vh' }}>
                {/* Hero Section */}
                <div className="bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-600 text-white py-16">
                    <div className="max-w-7xl mx-auto px-4">
                        <div className="text-center">
                            <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-white to-blue-200 bg-clip-text text-transparent">
                                🏠 Real Estate AI Predictor
                            </h1>
                            <p className="text-xl opacity-90 max-w-2xl mx-auto">
                                Dự đoán giá nhà thông minh với AI - Sử dụng XGBoost & LightGBM models
                            </p>
                            <div className="mt-8 flex justify-center space-x-8">
                                <Statistic
                                    title={<span className="text-blue-200">Tổng bất động sản</span>}
                                    value={marketStats?.total_properties || 141629}
                                    prefix={<HomeOutlined />}
                                    valueStyle={{ color: 'white' }}
                                />
                                <Statistic
                                    title={<span className="text-blue-200">Giá trung bình</span>}
                                    value={marketStats?.avg_price || 18433935387}
                                    formatter={(value) => formatPrice(Number(value))}
                                    prefix={<BankOutlined />}
                                    valueStyle={{ color: 'white' }}
                                />
                            </div>
                        </div>
                    </div>
                </div>

                <div className="max-w-7xl mx-auto px-4 py-8">
                    <Row gutter={[24, 24]}>
                        {/* Prediction Form */}
                        <Col xs={24} lg={14}>
                            <Card
                                title={
                                    <div className="flex items-center space-x-2">
                                        <CalculatorOutlined className="text-blue-600" />
                                        <span>Nhập thông tin nhà để dự đoán giá</span>
                                    </div>
                                }
                                className="shadow-lg"
                            >
                                <Form
                                    form={form}
                                    layout="vertical"
                                    onFinish={handlePredict}
                                    initialValues={{
                                        area: 80,
                                        latitude: 10.762622,
                                        longitude: 106.660172,
                                        bedroom: 3,
                                        bathroom: 2,
                                        floor_count: 3,
                                        house_direction_code: DEFAULT_CODES.HOUSE_DIRECTION,
                                        legal_status_code: DEFAULT_CODES.LEGAL_STATUS,
                                        interior_code: DEFAULT_CODES.INTERIOR,
                                        // Remove default province/district values
                                    }}
                                >
                                    <Row gutter={16}>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🏠 Diện tích (m²)"
                                                name="area"
                                                rules={[{ required: true, message: 'Vui lòng nhập diện tích!' }]}
                                            >
                                                <InputNumber
                                                    min={20}
                                                    max={1000}
                                                    className="w-full"
                                                    placeholder="Ví dụ: 80"
                                                    addonAfter="m²"
                                                />
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🛏️ Số phòng ngủ"
                                                name="bedroom"
                                                rules={[{ required: true, message: 'Vui lòng nhập số phòng ngủ!' }]}
                                            >
                                                <InputNumber min={1} max={10} className="w-full" />
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🚿 Số phòng tắm"
                                                name="bathroom"
                                                rules={[{ required: true, message: 'Vui lòng nhập số phòng tắm!' }]}
                                            >
                                                <InputNumber min={1} max={10} className="w-full" />
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🏢 Số tầng"
                                                name="floor_count"
                                                rules={[{ required: true, message: 'Vui lòng nhập số tầng!' }]}
                                            >
                                                <InputNumber min={1} max={20} className="w-full" />
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    <Divider orientation="left">📍 Vị trí địa lý</Divider>

                                    {/* Province and District */}
                                    <Row gutter={16}>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🏙️ Tỉnh/Thành phố"
                                                name="province_id"
                                                rules={[{ required: true, message: 'Vui lòng chọn tỉnh/thành phố!' }]}
                                            >
                                                <Select
                                                    placeholder="Vui lòng chọn tỉnh/thành phố"
                                                    loading={addressLoading}
                                                    onChange={(value) => {
                                                        console.log('Province changed:', value);
                                                        setSelectedProvince(value);
                                                        setSelectedDistrict('');
                                                        setSelectedWard('');
                                                        setSelectedStreet('');
                                                        form.setFieldsValue({
                                                            district_id: undefined,
                                                            ward_id: undefined,
                                                            street_id: undefined,
                                                            street: undefined
                                                        });
                                                        // useEffect will handle auto-geocoding
                                                    }}
                                                    showSearch
                                                    filterOption={(input, option) =>
                                                        (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                    }
                                                >                                                {(getProvinceOptions() || []).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="🏘️ Quận/Huyện"
                                                name="district_id"
                                                rules={[{ required: true, message: 'Vui lòng chọn quận/huyện!' }]}
                                            >
                                                <Select
                                                    placeholder="Vui lòng chọn quận/huyện"
                                                    disabled={!selectedProvince}
                                                    onChange={(value) => {
                                                        console.log('District changed:', value);
                                                        setSelectedDistrict(value);
                                                        setSelectedWard('');
                                                        setSelectedStreet('');
                                                        form.setFieldsValue({
                                                            ward_id: undefined,
                                                            street_id: undefined,
                                                            street: undefined
                                                        });
                                                        // useEffect will handle auto-geocoding
                                                    }}
                                                    showSearch
                                                    filterOption={(input, option) =>
                                                        (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                    }
                                                >                                                {(getDistrictOptions(selectedProvince) || []).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    {/* Ward and Street Selection */}
                                    <Row gutter={16}>
                                        <Col xs={24} md={12}>
                                            <Form.Item label="🏠 Phường/Xã" name="ward_id">
                                                <Select
                                                    placeholder="Vui lòng chọn phường/xã (tùy chọn)"
                                                    disabled={!selectedDistrict}
                                                    allowClear
                                                    onChange={(value) => {
                                                        console.log('Ward changed:', value);
                                                        setSelectedWard(value || '');
                                                        // useEffect will handle auto-geocoding
                                                    }}
                                                    showSearch
                                                    filterOption={(input, option) =>
                                                        (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                    }
                                                >                                                {(getWardOptions(selectedProvince, selectedDistrict) || []).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item label="🛣️ Đường (từ danh sách)" name="street_id">
                                                <Select
                                                    placeholder="Chọn đường từ danh sách (tùy chọn)"
                                                    disabled={!selectedDistrict}
                                                    allowClear
                                                    onChange={(value) => {
                                                        console.log('Street dropdown changed:', value);
                                                        setSelectedStreet(value || '');
                                                        if (value) {
                                                            // Clear custom street input when selecting from dropdown
                                                            form.setFieldValue('street', undefined);
                                                        }
                                                        // useEffect will handle auto-geocoding
                                                    }}
                                                    showSearch
                                                    filterOption={(input, option) =>
                                                        (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                    }
                                                >                                                {(getStreetOptions(selectedProvince, selectedDistrict) || []).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    {/* Custom Street Address */}
                                    <Row gutter={16}>
                                        <Col xs={24} md={18}>
                                            <Form.Item label="🏡 Hoặc nhập địa chỉ cụ thể" name="street">
                                                <input
                                                    type="text"
                                                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                                    placeholder="Ví dụ: Số 123 Nguyễn Văn A, Ngõ 5..."
                                                    value={form.getFieldValue('street') || ''}
                                                    onChange={(e) => {
                                                        form.setFieldValue('street', e.target.value);
                                                        if (e.target.value) {
                                                            // Clear street dropdown when typing custom street
                                                            setSelectedStreet('');
                                                            form.setFieldValue('street_id', undefined);
                                                        }
                                                        console.log('Custom street changed:', e.target.value);
                                                        // useEffect will handle auto-geocoding
                                                    }}
                                                />
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={6}>
                                            <Form.Item label=" " colon={false}>
                                                <Button
                                                    type="primary"
                                                    onClick={() => handleAddressToCoordinates(false)}
                                                    loading={geocodingLoading}
                                                    disabled={!selectedProvince || !selectedDistrict}
                                                    className="w-full"
                                                    icon={<EnvironmentOutlined />}
                                                >
                                                    Lấy tọa độ
                                                </Button>
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    {/* Coordinates */}
                                    <Row gutter={16}>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="📐 Vĩ độ (Latitude)"
                                                name="latitude"
                                                rules={[{ required: true, message: 'Vui lòng nhập vĩ độ!' }]}
                                            >
                                                <InputNumber
                                                    className="w-full"
                                                    precision={6}
                                                    placeholder="10.762622"
                                                    onChange={(value) => {
                                                        if (value && typeof value === 'number') {
                                                            setCurrentCoordinates(prev => ({ ...prev, lat: value }));
                                                        }
                                                    }}
                                                />
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={12}>
                                            <Form.Item
                                                label="📐 Kinh độ (Longitude)"
                                                name="longitude"
                                                rules={[{ required: true, message: 'Vui lòng nhập kinh độ!' }]}
                                            >
                                                <InputNumber
                                                    className="w-full"
                                                    precision={6}
                                                    placeholder="106.660172"
                                                    onChange={(value) => {
                                                        if (value && typeof value === 'number') {
                                                            setCurrentCoordinates(prev => ({ ...prev, lng: value }));
                                                        }
                                                    }}
                                                />
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    <Divider orientation="left">🏡 Thông tin bổ sung</Divider>

                                    <Row gutter={16}>
                                        <Col xs={24} md={8}>
                                            <Form.Item
                                                label={
                                                    <Tooltip title="1: Đông, 2: Tây, 3: Nam, 4: Bắc, 5: Đông Bắc, 6: Đông Nam, 7: Tây Bắc, 8: Tây Nam">
                                                        🧭 Hướng nhà <InfoCircleOutlined />
                                                    </Tooltip>
                                                }
                                                name="house_direction_code"
                                                rules={[{ required: true, message: 'Vui lòng chọn hướng nhà!' }]}
                                            >
                                                <Select placeholder="Chọn hướng nhà">
                                                    {(HOUSE_DIRECTION_OPTIONS || []).map(option => (
                                                        <Option key={option.value} value={option.value} title={option.description}>
                                                            {option.label}
                                                        </Option>
                                                    ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={8}>
                                            <Form.Item
                                                label={
                                                    <Tooltip title="1: Đã có sổ, 2: Đang chờ sổ, 4: Không có sổ, 5: Sổ chung, 6: Giấy tờ viết tay">
                                                        📋 Tình trạng pháp lý <InfoCircleOutlined />
                                                    </Tooltip>
                                                }
                                                name="legal_status_code"
                                                rules={[{ required: true, message: 'Vui lòng chọn tình trạng pháp lý!' }]}
                                            >
                                                <Select placeholder="Chọn tình trạng pháp lý">
                                                    {(LEGAL_STATUS_OPTIONS || []).map(option => (
                                                        <Option key={option.value} value={option.value} title={option.description}>
                                                            {option.label}
                                                        </Option>
                                                    ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                        <Col xs={24} md={8}>
                                            <Form.Item
                                                label={
                                                    <Tooltip title="1: Cao cấp, 2: Đầy đủ, 3: Cơ bản, 4: Bàn giao thô">
                                                        🏠 Nội thất <InfoCircleOutlined />
                                                    </Tooltip>
                                                }
                                                name="interior_code"
                                                rules={[{ required: true, message: 'Vui lòng chọn tình trạng nội thất!' }]}
                                            >
                                                <Select placeholder="Chọn tình trạng nội thất">
                                                    {(INTERIOR_OPTIONS || []).map(option => (
                                                        <Option key={option.value} value={option.value} title={option.description}>
                                                            {option.label}
                                                        </Option>
                                                    ))}
                                                </Select>
                                            </Form.Item>
                                        </Col>
                                    </Row>

                                    <Form.Item>
                                        <Button
                                            type="primary"
                                            htmlType="submit"
                                            loading={loading}
                                            size="large"
                                            className="w-full bg-gradient-to-r from-blue-500 to-purple-600 border-0 h-12"
                                            icon={<ThunderboltOutlined />}
                                            onClick={(e) => {
                                                console.log('🔵 Predict button clicked');
                                                // Prevent any potential redirect
                                                e.preventDefault();
                                                e.stopPropagation();

                                                // Validate and submit form
                                                form.validateFields()
                                                    .then(values => {
                                                        console.log('✅ Form validation passed:', values);
                                                        handlePredict(values);
                                                    })
                                                    .catch(errorInfo => {
                                                        console.error('❌ Form validation failed:', errorInfo);
                                                        notification.error({
                                                            message: 'Lỗi validation',
                                                            description: 'Vui lòng kiểm tra lại các trường bắt buộc',
                                                        });
                                                    });
                                            }}
                                        >
                                            🚀 Dự đoán giá nhà với AI
                                        </Button>
                                    </Form.Item>
                                </Form>
                            </Card>
                        </Col>

                        {/* Google Map */}
                        <Col xs={24} lg={10}>
                            <Card
                                title={
                                    <div className="flex items-center space-x-2">
                                        <EnvironmentOutlined className="text-blue-600" />
                                        <span>Bản đồ vị trí</span>
                                    </div>
                                }
                                className="shadow-lg"
                            >
                                <GoogleMapComponent
                                    latitude={currentCoordinates.lat}
                                    longitude={currentCoordinates.lng}
                                    onLocationChange={handleLocationChange}
                                    height={500}
                                    zoom={15}
                                />

                                {/* Address Summary */}
                                <div className="mt-4 p-4 bg-gray-50 rounded-lg">
                                    <div className="space-y-3">
                                        <div>
                                            <div className="font-medium text-gray-700 mb-1">📍 Tọa độ:</div>
                                            <div className="text-blue-600 font-mono text-sm flex items-center space-x-2">
                                                <span>{currentCoordinates.lat.toFixed(6)}, {currentCoordinates.lng.toFixed(6)}</span>
                                                {geocodingLoading && <Spin size="small" />}
                                            </div>
                                        </div>

                                        <div>
                                            <div className="font-medium text-gray-700 mb-1">🏙️ Tỉnh/Thành phố:</div>
                                            <div className="text-gray-600">{selectedProvince ? (getProvinceOptions() || []).find(p => p.value === selectedProvince)?.label : 'Chưa chọn'}</div>
                                        </div>

                                        <div>
                                            <div className="font-medium text-gray-700 mb-1">🏘️ Quận/Huyện:</div>
                                            <div className="text-gray-600">{selectedDistrict ? (getDistrictOptions(selectedProvince) || []).find(d => d.value === selectedDistrict)?.label : 'Chưa chọn'}</div>
                                        </div>

                                        {selectedWard && (
                                            <div>
                                                <div className="font-medium text-gray-700 mb-1">🏠 Phường/Xã:</div>
                                                <div className="text-gray-600">{(getWardOptions(selectedProvince, selectedDistrict) || []).find(w => w.value === selectedWard)?.label}</div>
                                            </div>
                                        )}

                                        {selectedStreet && (
                                            <div>
                                                <div className="font-medium text-gray-700 mb-1">🛣️ Đường (từ danh sách):</div>
                                                <div className="text-gray-600">{(getStreetOptions(selectedProvince, selectedDistrict) || []).find(s => s.value === selectedStreet)?.label}</div>
                                            </div>
                                        )}

                                        {form.getFieldValue('street') && (
                                            <div>
                                                <div className="font-medium text-gray-700 mb-1">🏡 Địa chỉ cụ thể:</div>
                                                <div className="text-gray-600">{form.getFieldValue('street')}</div>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {/* Quick Location Buttons */}
                                {/* <div className="mt-4 grid grid-cols-2 gap-2">
                                <Button
                                    size="small"
                                    onClick={() => {
                                        form.setFieldsValue({
                                            latitude: 10.762622,
                                            longitude: 106.660172,
                                            province_id: '79',
                                        });
                                        setSelectedProvince('79');
                                    }}
                                >
                                    🏙️ TP.HCM
                                </Button>
                                <Button
                                    size="small"
                                    onClick={() => {
                                        form.setFieldsValue({
                                            latitude: 21.028511,
                                            longitude: 105.804817,
                                            province_id: '1',
                                        });
                                        setSelectedProvince('1');
                                    }}
                                >
                                    🏛️ Hà Nội
                                </Button>
                                <Button
                                    size="small"
                                    onClick={() => {
                                        form.setFieldsValue({
                                            latitude: 16.047079,
                                            longitude: 108.206230,
                                            province_id: '48',
                                        });
                                        setSelectedProvince('48');
                                    }}
                                >
                                    🏖️ Đà Nẵng
                                </Button>
                                <Button
                                    size="small"
                                    onClick={() => {
                                        form.setFieldsValue({
                                            latitude: 10.012317,
                                            longitude: 105.626219,
                                            province_id: '94',
                                        });
                                        setSelectedProvince('94');
                                    }}
                                >
                                    🌾 Cần Thơ
                                </Button>
                            </div> */}
                            </Card>
                        </Col>
                    </Row>

                    {/* Prediction Results */}
                    {predictionResults && (
                        <Row gutter={[24, 24]} className="mt-8">
                            <Col span={24}>
                                <Card
                                    title={
                                        <div className="flex items-center space-x-2">
                                            <TrophyOutlined className="text-yellow-500" />
                                            <span>Kết quả dự đoán giá từ XGBoost & LightGBM</span>
                                        </div>
                                    }
                                    className="shadow-lg"
                                >
                                    <Row gutter={[16, 16]}>
                                        {/* Average Price */}
                                        {predictionResults.average && (
                                            <Col xs={24} md={6}>
                                                <Card className="text-center bg-gradient-to-br from-yellow-50 to-orange-50 border-yellow-200">
                                                    <Statistic
                                                        title={<span className="text-yellow-700">⭐ Giá trung bình</span>}
                                                        value={predictionResults.average}
                                                        formatter={(value) => formatPrice(Number(value))}
                                                        valueStyle={{ color: '#d97706', fontSize: '1.5rem', fontWeight: 'bold' }}
                                                    />
                                                    <Badge count="BEST" className="mt-2" />
                                                </Card>
                                            </Col>
                                        )}

                                        {/* Individual Model Results */}
                                        {[
                                            { key: 'xgboost', name: 'XGBoost', icon: '🚀', color: 'green' },
                                            { key: 'lightgbm', name: 'LightGBM', icon: '⚡', color: 'purple' },
                                        ].map(({ key, name, icon, color }) => {
                                            const result = predictionResults[key as keyof PredictionResults] as PredictionResult;

                                            // Safety check for result
                                            if (!result) {
                                                return (
                                                    <Col xs={24} md={6} key={key}>
                                                        <Card className="text-center">
                                                            <div className="text-2xl mb-2">❓</div>
                                                            <div className="font-semibold text-gray-600 mb-2">{name}</div>
                                                            <div className="text-gray-500 text-sm">No data</div>
                                                        </Card>
                                                    </Col>
                                                );
                                            }

                                            const isBest = bestPrediction?.model === name;

                                            return (
                                                <Col xs={24} md={6} key={key}>
                                                    <Card
                                                        className={`text-center ${isBest ? 'ring-2 ring-yellow-400' : ''}`}
                                                        style={{
                                                            background: result.success
                                                                ? `linear-gradient(135deg, ${color === 'green' ? '#f0fdf4' : color === 'purple' ? '#faf5ff' : '#fef2f2'} 0%, white 100%)`
                                                                : '#fef2f2'
                                                        }}
                                                    >
                                                        {result?.success ? (
                                                            <>
                                                                <div className="text-2xl mb-2">{icon}</div>
                                                                <div className="font-semibold text-gray-700 mb-2">{name}</div>
                                                                <Statistic
                                                                    value={result.predicted_price || 0}
                                                                    formatter={(value) => formatPrice(Number(value))}
                                                                    valueStyle={{
                                                                        color: color === 'green' ? '#16a34a' : color === 'purple' ? '#9333ea' : '#dc2626',
                                                                        fontSize: '1.2rem'
                                                                    }}
                                                                />
                                                                {isBest && (
                                                                    <Tag color="gold" className="mt-2">
                                                                        <StarOutlined /> Highest Accuracy
                                                                    </Tag>
                                                                )}
                                                            </>
                                                        ) : (
                                                            <>
                                                                <div className="text-2xl mb-2">❌</div>
                                                                <div className="font-semibold text-red-600 mb-2">{name}</div>
                                                                <div className="text-red-500 text-sm">
                                                                    {result.error || 'Prediction failed'}
                                                                </div>
                                                            </>
                                                        )}
                                                    </Card>
                                                </Col>
                                            );
                                        })}
                                    </Row>
                                </Card>
                            </Col>
                        </Row>
                    )}

                    {/* Similar Properties */}
                    <Row gutter={[24, 24]} className="mt-8">
                        <Col span={24}>
                            <Card
                                title={
                                    <div className="flex items-center space-x-2">
                                        <SearchOutlined className="text-green-600" />
                                        <span>Nhà tương tự với nhà bạn dự đoán</span>
                                    </div>
                                }
                                className="shadow-lg"
                            >
                                {loadingProperties ? (
                                    <div className="text-center py-8">
                                        <Spin size="large" />
                                        <div className="mt-4 text-gray-500">Đang tìm kiếm nhà tương tự...</div>
                                    </div>
                                ) : properties.length > 0 ? (
                                    <Row gutter={[16, 16]}>
                                        {(properties || []).slice(0, 8).map((property) => (
                                            <Col xs={24} sm={12} md={8} lg={6} key={property.id}>
                                                <Card
                                                    hoverable
                                                    className="h-full cursor-pointer"
                                                    onClick={() => handlePropertyClick(property)}
                                                    cover={
                                                        <div className="h-20 bg-gradient-to-br from-blue-100 via-purple-100 to-green-100 flex items-center justify-center relative">
                                                            <HomeOutlined className="text-2xl text-gray-500" />
                                                            <div className="absolute top-1 right-1">
                                                                <EyeOutlined className="text-sm text-gray-600" />
                                                            </div>
                                                        </div>
                                                    }
                                                    actions={[
                                                        <Button
                                                            type="text"
                                                            icon={<EyeOutlined />}
                                                            onClick={(e) => {
                                                                e.stopPropagation();
                                                                handlePropertyClick(property);
                                                            }}
                                                        >
                                                            Chi tiết
                                                        </Button>,
                                                    ]}
                                                >
                                                    <div className="space-y-2">
                                                        <div className="font-semibold text-gray-800 text-sm leading-tight h-10 overflow-hidden">
                                                            {(property.title || '').length > 50 ? `${property.title.substring(0, 50)}...` : (property.title || '(Không có tiêu đề)')}
                                                        </div>
                                                        <div className="text-lg font-bold text-red-600">
                                                            {formatPrice(property.price)}
                                                        </div>
                                                        <div className="text-xs text-gray-600">
                                                            📐 {property.area}m² • 🛏️ {property.bedroom}PN • 🚿 {property.bathroom}PT
                                                        </div>                                        <div className="text-xs text-gray-500 h-8 overflow-hidden">
                                                            📍 {getPropertyAddress(property) || '(Không có địa chỉ)'}
                                                        </div>
                                                    </div>
                                                </Card>
                                            </Col>
                                        ))}
                                    </Row>
                                ) : (
                                    <Empty
                                        description="Chưa có dữ liệu nhà tương tự"
                                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                                    />
                                )}
                            </Card>
                        </Col>
                    </Row>
                </div>

                {/* Property Detail Modal */}
                <Modal
                    title={
                        <div className="flex items-center space-x-2">
                            <HomeOutlined className="text-blue-600" />
                            <span>Chi tiết bất động sản</span>
                        </div>
                    }
                    open={propertyModalVisible}
                    onCancel={() => setPropertyModalVisible(false)}
                    footer={[
                        <Button key="close" onClick={() => setPropertyModalVisible(false)}>
                            Đóng
                        </Button>,
                        (selectedProperty as any)?.url && (
                            <Button
                                key="url"
                                type="primary"
                                icon={<LinkOutlined />}
                                onClick={() => copyUrl((selectedProperty as any).url)}
                            >
                                Sao chép URL
                            </Button>
                        ),
                        (selectedProperty as any)?.url && (
                            <Button
                                key="visit"
                                type="primary"
                                onClick={() => window.open((selectedProperty as any).url, '_blank')}
                            >
                                Xem chi tiết
                            </Button>
                        )
                    ]}
                    width={700}
                >
                    {selectedProperty && (
                        <div className="space-y-4">
                            {/* Title */}
                            <Title level={4} className="mb-2">
                                {selectedProperty.title}
                            </Title>

                            {/* Price */}
                            <Card className="bg-gradient-to-r from-red-50 to-pink-50">
                                <Statistic
                                    title="Giá bán"
                                    value={selectedProperty.price}
                                    formatter={(value) => formatPrice(Number(value))}
                                    valueStyle={{ color: '#dc2626', fontSize: '1.8rem', fontWeight: 'bold' }}
                                    prefix="💰"
                                />
                            </Card>

                            {/* Basic Info */}
                            <Row gutter={[16, 16]}>
                                <Col span={8}>
                                    <Card className="text-center">
                                        <Statistic
                                            title="Diện tích"
                                            value={selectedProperty.area}
                                            suffix="m²"
                                            prefix="📐"
                                            valueStyle={{ color: '#2563eb' }}
                                        />
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card className="text-center">
                                        <Statistic
                                            title="Phòng ngủ"
                                            value={selectedProperty.bedroom}
                                            prefix="🛏️"
                                            valueStyle={{ color: '#16a34a' }}
                                        />
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card className="text-center">
                                        <Statistic
                                            title="Phòng tắm"
                                            value={selectedProperty.bathroom}
                                            prefix="🚿"
                                            valueStyle={{ color: '#9333ea' }}
                                        />
                                    </Card>
                                </Col>
                            </Row>

                            {/* Address */}
                            <div>
                                <Text strong className="block mb-2">📍 Địa chỉ:</Text>
                                <Card className="bg-gray-50"><Text>
                                    {getPropertyAddress(selectedProperty) || '(Không có địa chỉ)'}</Text>
                                </Card>
                            </div>

                            {/* Description */}
                            {selectedProperty.description && (
                                <div>
                                    <Text strong className="block mb-2">📝 Mô tả:</Text>
                                    <Card className="bg-gray-50">
                                        <Text>{selectedProperty.description}</Text>
                                    </Card>
                                </div>
                            )}

                            {/* Additional Info */}
                            <Row gutter={[16, 16]}>
                                {(selectedProperty as any).floor_count && (
                                    <Col span={12}>
                                        <div className="flex justify-between">
                                            <Text strong>🏢 Số tầng:</Text>
                                            <Text>{(selectedProperty as any).floor_count}</Text>
                                        </div>
                                    </Col>
                                )}
                                {(selectedProperty as any).listing_date && (
                                    <Col span={12}>
                                        <div className="flex justify-between">
                                            <Text strong>📅 Ngày đăng:</Text>
                                            <Text>{new Date((selectedProperty as any).listing_date).toLocaleDateString('vi-VN')}</Text>
                                        </div>
                                    </Col>
                                )}
                            </Row>

                            {/* URL Info */}
                            {(selectedProperty as any).url && (
                                <div>
                                    <Text strong className="block mb-2">🔗 Nguồn:</Text>
                                    <Card className="bg-blue-50">
                                        <div className="flex items-center justify-between">
                                            <Text ellipsis className="flex-1 mr-2">{(selectedProperty as any).url}</Text>
                                            <Button
                                                size="small"
                                                icon={<LinkOutlined />}
                                                onClick={() => copyUrl((selectedProperty as any).url)}
                                            >
                                                Copy
                                            </Button>
                                        </div>
                                    </Card>
                                </div>
                            )}
                        </div>
                    )}
                </Modal>
            </div> {/* Close error boundary wrapper */}
        </div>
    );
}
