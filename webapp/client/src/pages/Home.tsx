import { useState, useEffect } from 'react';
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
} from '@ant-design/icons';
import { useAddressData } from '../hooks/useAddressData';
import MapComponent from '../components/MapComponent';
import realEstateAPI, { PredictionInput, PredictionResult, Property, SearchFilters } from '../services/api';

const { Option } = Select;

interface PredictionResults {
    linearRegression: PredictionResult;
    xgboost: PredictionResult;
    lightgbm: PredictionResult;
    ensemble: PredictionResult;
    average?: number;
}

export default function Home() {
    const [form] = Form.useForm();
    const [loading, setLoading] = useState(false);
    const [predictionResults, setPredictionResults] = useState<PredictionResults | null>(null);
    const [properties, setProperties] = useState<Property[]>([]);
    const [loadingProperties, setLoadingProperties] = useState(false);
    const [marketStats, setMarketStats] = useState<any>(null);

    const {
        getProvinceOptions,
        getDistrictOptions,
        getWardOptions,
        getFullAddress,
        loading: addressLoading,
    } = useAddressData();

    // Form state
    const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');

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
    }, []);

    // Handle prediction
    const handlePredict = async (values: any) => {
        setLoading(true);
        try {
            const predictionData: PredictionInput = {
                area: values.area,
                latitude: values.latitude,
                longitude: values.longitude,
                bedroom: values.bedroom,
                bathroom: values.bathroom,
                floor_count: values.floor_count,
                house_direction_code: values.house_direction_code,
                legal_status_code: values.legal_status_code,
                interior_code: values.interior_code,
                province_id: parseInt(values.province_id),
                district_id: parseInt(values.district_id),
                ward_id: parseInt(values.ward_id || '0'),
            };

            const results = await realEstateAPI.prediction.predictAll(predictionData);
            setPredictionResults(results);

            // Search for similar properties
            await searchSimilarProperties(predictionData);

            notification.success({
                message: '🎉 Dự đoán thành công!',
                description: 'Đã dự đoán giá nhà từ 4 models ML khác nhau',
            });
        } catch (error) {
            console.error('Prediction failed:', error);
            notification.error({
                message: '❌ Dự đoán thất bại',
                description: 'Có lỗi xảy ra khi dự đoán giá nhà',
            });
        } finally {
            setLoading(false);
        }
    };

    // Search for similar properties
    const searchSimilarProperties = async (predictionData: PredictionInput) => {
        setLoadingProperties(true);
        try {
            const priceRange = predictionResults?.average || 2000000000;
            const filters: SearchFilters = {
                province: getFullAddress(predictionData.province_id.toString()).split(',').pop()?.trim(),
                price_min: priceRange * 0.8,
                price_max: priceRange * 1.2,
                area_min: predictionData.area * 0.8,
                area_max: predictionData.area * 1.2,
                bedroom_min: Math.max(1, predictionData.bedroom - 1),
                bedroom_max: predictionData.bedroom + 1,
                limit: 10,
            };

            const response = await realEstateAPI.search.advanced(filters);
            setProperties(response.results || []);
        } catch (error) {
            console.error('Failed to search properties:', error);
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

    // Get best prediction
    const getBestPrediction = (): { model: string; result: PredictionResult } | null => {
        if (!predictionResults) return null;

        const models = [
            { name: 'Linear Regression', result: predictionResults.linearRegression },
            { name: 'XGBoost', result: predictionResults.xgboost },
            { name: 'LightGBM', result: predictionResults.lightgbm },
            { name: 'Ensemble', result: predictionResults.ensemble },
        ];

        const validModels = models.filter(m => m.result.success && m.result.model_metrics?.r2);
        if (validModels.length === 0) return null;

        const best = validModels.reduce((prev, current) =>
            (current.result.model_metrics?.r2 || 0) > (prev.result.model_metrics?.r2 || 0) ? current : prev
        );

        return { model: best.name, result: best.result };
    };

    const bestPrediction = getBestPrediction();

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
            {/* Hero Section */}
            <div className="bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-600 text-white py-16">
                <div className="max-w-7xl mx-auto px-4">
                    <div className="text-center">
                        <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-white to-blue-200 bg-clip-text text-transparent">
                            🏠 Real Estate AI Predictor
                        </h1>
                        <p className="text-xl opacity-90 max-w-2xl mx-auto">
                            Dự đoán giá nhà thông minh với AI - Sử dụng 4 models Machine Learning tiên tiến
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
                                    house_direction_code: 3,
                                    legal_status_code: 1,
                                    interior_code: 2,
                                    province_id: '1',
                                    district_id: '1',
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

                                <Row gutter={16}>
                                    <Col xs={24} md={8}>
                                        <Form.Item
                                            label="🏙️ Tỉnh/Thành phố"
                                            name="province_id"
                                            rules={[{ required: true, message: 'Vui lòng chọn tỉnh/thành phố!' }]}
                                        >
                                            <Select
                                                placeholder="Chọn tỉnh/thành phố"
                                                loading={addressLoading}
                                                onChange={(value) => {
                                                    setSelectedProvince(value);
                                                    setSelectedDistrict('');
                                                    form.setFieldsValue({ district_id: undefined, ward_id: undefined });
                                                }}
                                                showSearch
                                                filterOption={(input, option) =>
                                                    (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                }
                                            >
                                                {getProvinceOptions().map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                            </Select>
                                        </Form.Item>
                                    </Col>
                                    <Col xs={24} md={8}>
                                        <Form.Item
                                            label="🏘️ Quận/Huyện"
                                            name="district_id"
                                            rules={[{ required: true, message: 'Vui lòng chọn quận/huyện!' }]}
                                        >
                                            <Select
                                                placeholder="Chọn quận/huyện"
                                                disabled={!selectedProvince}
                                                onChange={(value) => {
                                                    setSelectedDistrict(value);
                                                    form.setFieldsValue({ ward_id: undefined });
                                                }}
                                                showSearch
                                                filterOption={(input, option) =>
                                                    (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                }
                                            >
                                                {getDistrictOptions(selectedProvince).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                            </Select>
                                        </Form.Item>
                                    </Col>
                                    <Col xs={24} md={8}>
                                        <Form.Item label="🏠 Phường/Xã" name="ward_id">
                                            <Select
                                                placeholder="Chọn phường/xã"
                                                disabled={!selectedDistrict}
                                                allowClear
                                                onChange={() => { }}
                                                showSearch
                                                filterOption={(input, option) =>
                                                    (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                                                }
                                            >
                                                {getWardOptions(selectedProvince, selectedDistrict).map(option => (
                                                    <Option key={option.value} value={option.value}>
                                                        {option.label}
                                                    </Option>
                                                ))}
                                            </Select>
                                        </Form.Item>
                                    </Col>
                                </Row>

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
                                                <Option value={1}>🌅 Đông</Option>
                                                <Option value={2}>🌇 Tây</Option>
                                                <Option value={3}>☀️ Nam</Option>
                                                <Option value={4}>❄️ Bắc</Option>
                                                <Option value={5}>🌄 Đông Bắc</Option>
                                                <Option value={6}>🏖️ Đông Nam</Option>
                                                <Option value={7}>🏔️ Tây Bắc</Option>
                                                <Option value={8}>🌆 Tây Nam</Option>
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
                                                <Option value={1}>✅ Đã có sổ</Option>
                                                <Option value={2}>⏳ Đang chờ sổ</Option>
                                                <Option value={4}>❌ Không có sổ</Option>
                                                <Option value={5}>📑 Sổ chung</Option>
                                                <Option value={6}>✍️ Giấy tờ viết tay</Option>
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
                                                <Option value={1}>💎 Cao cấp</Option>
                                                <Option value={2}>🏡 Đầy đủ</Option>
                                                <Option value={3}>🔧 Cơ bản</Option>
                                                <Option value={4}>🏗️ Bàn giao thô</Option>
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
                                    >
                                        🚀 Dự đoán giá nhà với AI
                                    </Button>
                                </Form.Item>
                            </Form>
                        </Card>
                    </Col>

                    {/* Map Component */}
                    <Col xs={24} lg={10}>
                        <MapComponent
                            latitude={form.getFieldValue('latitude')}
                            longitude={form.getFieldValue('longitude')}
                            onLocationChange={handleLocationChange}
                            height={600}
                        />
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
                                        <span>Kết quả dự đoán giá từ 4 Models AI</span>
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
                                        { key: 'linearRegression', name: 'Linear Regression', icon: '📈', color: 'blue' },
                                        { key: 'xgboost', name: 'XGBoost', icon: '🚀', color: 'green' },
                                        { key: 'lightgbm', name: 'LightGBM', icon: '⚡', color: 'purple' },
                                        { key: 'ensemble', name: 'Ensemble', icon: '🎯', color: 'red' },
                                    ].map(({ key, name, icon, color }) => {
                                        const result = predictionResults[key as keyof PredictionResults] as PredictionResult;
                                        const isBest = bestPrediction?.model === name;

                                        return (
                                            <Col xs={24} md={6} key={key}>
                                                <Card
                                                    className={`text-center ${isBest ? 'ring-2 ring-yellow-400' : ''}`}
                                                    style={{
                                                        background: result.success
                                                            ? `linear-gradient(135deg, ${color === 'blue' ? '#eff6ff' : color === 'green' ? '#f0fdf4' : color === 'purple' ? '#faf5ff' : '#fef2f2'} 0%, white 100%)`
                                                            : '#fef2f2'
                                                    }}
                                                >
                                                    {result.success ? (
                                                        <>
                                                            <div className="text-2xl mb-2">{icon}</div>
                                                            <div className="font-semibold text-gray-700 mb-2">{name}</div>
                                                            <Statistic
                                                                value={result.predicted_price}
                                                                formatter={(value) => formatPrice(Number(value))}
                                                                valueStyle={{
                                                                    color: color === 'blue' ? '#2563eb' : color === 'green' ? '#16a34a' : color === 'purple' ? '#9333ea' : '#dc2626',
                                                                    fontSize: '1.2rem'
                                                                }}
                                                            />
                                                            {result.model_metrics?.r2 && (
                                                                <div className="text-xs text-gray-500 mt-1">
                                                                    R² Score: {(result.model_metrics.r2 * 100).toFixed(1)}%
                                                                </div>
                                                            )}
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
                                    <span>Nhà tương tự trong khu vực</span>
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
                                    {properties.slice(0, 8).map((property) => (
                                        <Col xs={24} sm={12} md={8} lg={6} key={property.id}>
                                            <Card
                                                hoverable
                                                className="h-full"
                                                cover={
                                                    <div className="h-32 bg-gradient-to-br from-blue-100 to-green-100 flex items-center justify-center">
                                                        <HomeOutlined className="text-4xl text-gray-400" />
                                                    </div>
                                                }
                                            >
                                                <div className="space-y-2">
                                                    <div className="font-semibold text-gray-800 truncate" title={property.title}>
                                                        {property.title}
                                                    </div>
                                                    <div className="text-lg font-bold text-red-600">
                                                        {formatPrice(property.price)}
                                                    </div>
                                                    <div className="text-sm text-gray-600">
                                                        📐 {property.area}m² • 🛏️ {property.bedroom}PN • 🚿 {property.bathroom}PT
                                                    </div>
                                                    <div className="text-xs text-gray-500 truncate" title={property.address}>
                                                        📍 {property.address}
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
        </div>
    );
}
