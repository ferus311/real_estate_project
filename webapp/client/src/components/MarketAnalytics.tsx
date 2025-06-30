import React, { useState, useEffect } from 'react';
import {
    Card,
    Row,
    Col,
    Select,
    Spin,
    Typography,
    Button,
    Table,
    Tag,
    Space,
    Statistic,
    Alert,
    notification,
    Divider,
    Tooltip,
    Progress,
} from 'antd';
import {
    ReloadOutlined,
    BarChartOutlined,
    RiseOutlined,
    FallOutlined,
    InfoCircleOutlined,
    ThunderboltOutlined,
} from '@ant-design/icons';
import realEstateAPI from '../services/api';
import { useAddressData } from '../hooks/useAddressData';

const { Option } = Select;
const { Title, Text } = Typography;

interface PriceDistribution {
    range: string;
    min_price: number;
    max_price: number;
    count: number;
    percentage: number;
}

interface DistrictData {
    district_id: number;
    district_name: string;
    property_count: number;
    avg_price: number;
    min_price: number;
    max_price: number;
    avg_area: number;
    avg_price_per_m2: number;
}

interface MarketData {
    total_properties: number;
    statistics: {
        avg_price: number;
        min_price: number;
        max_price: number;
        avg_area: number;
        avg_price_per_m2: number;
    };
    price_distribution: PriceDistribution[];
    price_per_m2_distribution: PriceDistribution[];
}

export default function MarketAnalytics() {
    const [loading, setLoading] = useState(false);
    const [modelLoading, setModelLoading] = useState(false);
    const [selectedProvince, setSelectedProvince] = useState<string>('79'); // Default HCM
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');
    const [marketData, setMarketData] = useState<MarketData | null>(null);
    const [districtData, setDistrictData] = useState<DistrictData[]>([]);
    const [modelInfo, setModelInfo] = useState<any>(null);

    const { getProvinceOptions, getDistrictOptions } = useAddressData();

    // Format price helper
    const formatPrice = (price: number): string => {
        if (price >= 1_000_000_000) {
            return `${(price / 1_000_000_000).toFixed(1)}B`;
        } else if (price >= 1_000_000) {
            return `${(price / 1_000_000).toFixed(1)}M`;
        }
        return price.toLocaleString();
    };

    // Load market data for selected location
    const loadMarketData = async () => {
        if (!selectedProvince) return;

        setLoading(true);
        try {
            const params: any = { province_id: selectedProvince };
            if (selectedDistrict) {
                params.district_id = selectedDistrict;
            }

            const response = await realEstateAPI.analytics.priceDistributionByLocation(params);
            setMarketData(response.data);

            notification.success({
                message: 'Dữ liệu thị trường đã được cập nhật',
                description: `Tìm thấy ${response.data.total_properties} bất động sản`,
            });
        } catch (error) {
            console.error('Error loading market data:', error);
            notification.error({
                message: 'Lỗi tải dữ liệu',
                description: 'Không thể tải dữ liệu thị trường',
            });
        } finally {
            setLoading(false);
        }
    };

    // Load district comparison data
    const loadDistrictData = async () => {
        if (!selectedProvince) return;

        try {
            const response = await realEstateAPI.analytics.districtComparison(selectedProvince);
            setDistrictData(response.data);
        } catch (error) {
            console.error('Error loading district data:', error);
        }
    };

    // Refresh ML models
    const refreshModels = async () => {
        setModelLoading(true);
        try {
            const response = await realEstateAPI.analytics.refreshModel();
            setModelInfo(response.data);

            notification.success({
                message: 'Model đã được làm mới',
                description: 'ML models đã được cập nhật thành công',
            });
        } catch (error) {
            console.error('Error refreshing models:', error);
            notification.error({
                message: 'Lỗi làm mới model',
                description: 'Không thể làm mới ML models',
            });
        } finally {
            setModelLoading(false);
        }
    };

    // Load current model info
    const loadModelInfo = async () => {
        try {
            const response = await realEstateAPI.prediction.getCurrentModelInfo();
            setModelInfo(response.data);
        } catch (error) {
            console.error('Error loading model info:', error);
        }
    };

    useEffect(() => {
        loadMarketData();
        loadDistrictData();
        loadModelInfo();
    }, [selectedProvince, selectedDistrict]);

    // District comparison table columns
    const districtColumns = [
        {
            title: 'Quận/Huyện',
            dataIndex: 'district_name',
            key: 'district_name',
            render: (text: string) => <Text strong>{text}</Text>,
        },
        {
            title: 'Số BĐS',
            dataIndex: 'property_count',
            key: 'property_count',
            sorter: (a: DistrictData, b: DistrictData) => a.property_count - b.property_count,
            render: (count: number) => <Tag color="blue">{count}</Tag>,
        },
        {
            title: 'Giá TB',
            dataIndex: 'avg_price',
            key: 'avg_price',
            sorter: (a: DistrictData, b: DistrictData) => a.avg_price - b.avg_price,
            render: (price: number) => <Text>{formatPrice(price)} VND</Text>,
        },
        {
            title: 'Giá/m²',
            dataIndex: 'avg_price_per_m2',
            key: 'avg_price_per_m2',
            sorter: (a: DistrictData, b: DistrictData) => a.avg_price_per_m2 - b.avg_price_per_m2,
            render: (pricePerM2: number) => (
                <Text strong style={{ color: '#1890ff' }}>
                    {formatPrice(pricePerM2)} VND/m²
                </Text>
            ),
        },
        {
            title: 'Khoảng giá',
            key: 'price_range',
            render: (record: DistrictData) => (
                <Space direction="vertical" size="small">
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                        Min: {formatPrice(record.min_price)}
                    </Text>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                        Max: {formatPrice(record.max_price)}
                    </Text>
                </Space>
            ),
        },
    ];

    // Prepare chart data for price distribution
    const priceChartData = marketData?.price_distribution?.map(item => ({
        range: item.range,
        count: item.count,
        percentage: item.percentage,
    })) || [];

    const pricePerM2ChartData = marketData?.price_per_m2_distribution?.map(item => ({
        range: item.range,
        count: item.count,
        percentage: item.percentage,
    })) || [];

    return (
        <div className="p-6 bg-gray-50 min-h-screen">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <Row justify="space-between" align="middle" className="mb-6">
                    <Col>
                        <Title level={2}>
                            <BarChartOutlined className="mr-2" />
                            Phân tích thị trường BĐS
                        </Title>
                    </Col>
                    <Col>
                        <Space>
                            <Button
                                type="primary"
                                icon={<ThunderboltOutlined />}
                                loading={modelLoading}
                                onClick={refreshModels}
                            >
                                Làm mới Model
                            </Button>
                            <Button
                                icon={<ReloadOutlined />}
                                onClick={() => {
                                    loadMarketData();
                                    loadDistrictData();
                                }}
                            >
                                Cập nhật dữ liệu
                            </Button>
                        </Space>
                    </Col>
                </Row>

                {/* Model Status */}
                {modelInfo && (
                    <Alert
                        message="Trạng thái ML Model"
                        description={
                            <Space direction="vertical">
                                <Text>
                                    Models đã tải: {Object.keys(modelInfo.loaded_models || {}).length > 0
                                        ? Object.keys(modelInfo.loaded_models).join(', ')
                                        : 'Chưa có models'}
                                </Text>
                                <Text type="secondary">
                                    HDFS: {modelInfo.hdfs_available ? '✅ Kết nối' : '❌ Không kết nối'}
                                </Text>
                                {modelInfo.current_model_path && (
                                    <Text type="secondary">Path: {modelInfo.current_model_path}</Text>
                                )}
                            </Space>
                        }
                        type={Object.keys(modelInfo.loaded_models || {}).length > 0 ? 'success' : 'warning'}
                        showIcon
                        className="mb-4"
                    />
                )}

                {/* Location Selector */}
                <Card className="mb-6">
                    <Row gutter={16} align="middle">
                        <Col span={6}>
                            <label className="block text-sm font-medium mb-2">Tỉnh/Thành phố:</label>
                            <Select
                                value={selectedProvince}
                                onChange={setSelectedProvince}
                                style={{ width: '100%' }}
                                placeholder="Chọn tỉnh/thành"
                            >
                                {getProvinceOptions()?.map(province => (
                                    <Option key={province.value} value={province.value}>
                                        {province.label}
                                    </Option>
                                ))}
                            </Select>
                        </Col>
                        <Col span={6}>
                            <label className="block text-sm font-medium mb-2">Quận/Huyện:</label>
                            <Select
                                value={selectedDistrict}
                                onChange={setSelectedDistrict}
                                style={{ width: '100%' }}
                                placeholder="Tất cả quận/huyện"
                                allowClear
                            >
                                {getDistrictOptions(selectedProvince)?.map(district => (
                                    <Option key={district.value} value={district.value}>
                                        {district.label}
                                    </Option>
                                ))}
                            </Select>
                        </Col>
                    </Row>
                </Card>

                {loading ? (
                    <div className="text-center py-12">
                        <Spin size="large" />
                        <p className="mt-4 text-gray-600">Đang tải dữ liệu thị trường...</p>
                    </div>
                ) : (
                    <>
                        {/* Market Statistics */}
                        {marketData && (
                            <Row gutter={[16, 16]} className="mb-6">
                                <Col xs={24} sm={12} md={6}>
                                    <Card>
                                        <Statistic
                                            title="Tổng BĐS"
                                            value={marketData.total_properties}
                                            prefix={<RiseOutlined />}
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} md={6}>
                                    <Card>
                                        <Statistic
                                            title="Giá trung bình"
                                            value={marketData.statistics.avg_price}
                                            formatter={(value) => formatPrice(Number(value)) + ' VND'}
                                            prefix={<BarChartOutlined />}
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} md={6}>
                                    <Card>
                                        <Statistic
                                            title="Giá/m² TB"
                                            value={marketData.statistics.avg_price_per_m2}
                                            formatter={(value) => formatPrice(Number(value)) + ' VND/m²'}
                                            prefix={<RiseOutlined />}
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} md={6}>
                                    <Card>
                                        <Statistic
                                            title="Diện tích TB"
                                            value={marketData.statistics.avg_area}
                                            formatter={(value) => Number(value).toFixed(1) + ' m²'}
                                            prefix={<InfoCircleOutlined />}
                                        />
                                    </Card>
                                </Col>
                            </Row>
                        )}

                        <Row gutter={[16, 16]}>
                            {/* Price Distribution Chart */}
                            <Col xs={24} lg={12}>
                                <Card title="📊 Phổ giá bất động sản" className="h-full">
                                    {priceChartData.length > 0 ? (
                                        <Space direction="vertical" className="w-full">
                                            {priceChartData.map((item, index) => (
                                                <div key={index} className="mb-4">
                                                    <div className="flex justify-between mb-1">
                                                        <Text style={{ fontSize: '12px' }}>{item.range}</Text>
                                                        <Text strong>{item.count} ({item.percentage}%)</Text>
                                                    </div>
                                                    <Progress
                                                        percent={item.percentage}
                                                        showInfo={false}
                                                        strokeColor={`hsl(${index * 36}, 70%, 50%)`}
                                                    />
                                                </div>
                                            ))}
                                        </Space>
                                    ) : (
                                        <div className="text-center py-8 text-gray-500">
                                            Không có dữ liệu
                                        </div>
                                    )}
                                </Card>
                            </Col>

                            {/* Price per M2 Distribution Chart */}
                            <Col xs={24} lg={12}>
                                <Card title="📏 Phổ giá theo m²" className="h-full">
                                    {pricePerM2ChartData.length > 0 ? (
                                        <Space direction="vertical" className="w-full">
                                            {pricePerM2ChartData.map((item, index) => (
                                                <div key={index} className="mb-4">
                                                    <div className="flex justify-between mb-1">
                                                        <Text style={{ fontSize: '12px' }}>{item.range}</Text>
                                                        <Text strong>{item.count} ({item.percentage}%)</Text>
                                                    </div>
                                                    <Progress
                                                        percent={item.percentage}
                                                        showInfo={false}
                                                        strokeColor={`hsl(${index * 36 + 180}, 70%, 50%)`}
                                                    />
                                                </div>
                                            ))}
                                        </Space>
                                    ) : (
                                        <div className="text-center py-8 text-gray-500">
                                            Không có dữ liệu
                                        </div>
                                    )}
                                </Card>
                            </Col>

                            {/* District Comparison Table */}
                            <Col xs={24}>
                                <Card title="🏙️ So sánh giá theo quận/huyện">
                                    <Table
                                        dataSource={districtData}
                                        columns={districtColumns}
                                        rowKey="district_id"
                                        pagination={{
                                            pageSize: 10,
                                            showSizeChanger: true,
                                            showQuickJumper: true,
                                        }}
                                        scroll={{ x: 800 }}
                                    />
                                </Card>
                            </Col>
                        </Row>
                    </>
                )}
            </div>
        </div>
    );
}
