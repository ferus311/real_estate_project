import { useState, useEffect } from 'react';
import {
    Card,
    Row,
    Col,
    Statistic,
    Spin,
    Select,
    Space,
    Typography,
    Table,
    Tag,
    Progress,
    Divider,
    Empty,
    notification,
    List,
} from 'antd';
import {
    BarChartOutlined,
    LineChartOutlined,
    PieChartOutlined,
    TrophyOutlined,
    RiseOutlined,
    FallOutlined,
    DollarOutlined,
    HomeOutlined,
    EnvironmentOutlined,
    CalendarOutlined,
} from '@ant-design/icons';
import { useAddressData } from '../hooks/useAddressData';
import realEstateAPI from '../services/api';

const { Option } = Select;
const { Title, Text } = Typography;

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D'];

export default function Analytics() {
    const [loading, setLoading] = useState(false);
    const [marketOverview, setMarketOverview] = useState<any>(null);
    const [priceDistribution, setPriceDistribution] = useState<any[]>([]);
    const [propertyTypeStats, setPropertyTypeStats] = useState<any[]>([]);
    const [locationStats, setLocationStats] = useState<any[]>([]);
    const [priceTrends, setPriceTrends] = useState<any[]>([]);
    const [areaDistribution, setAreaDistribution] = useState<any[]>([]);
    const [dashboardSummary, setDashboardSummary] = useState<any>(null); const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');
    const [selectedTimeRange, setSelectedTimeRange] = useState<string>('6months');

    const {
        getProvinceOptions,
        getDistrictOptions,
        loading: addressLoading,
    } = useAddressData();

    // Load all analytics data
    const loadAnalyticsData = async () => {
        setLoading(true);
        try {
            // Prepare filter params - linh hoạt chỉ truyền khi có dữ liệu
            const filterParams: { province?: string; district?: string } = {};

            if (selectedProvince) {
                // Lấy tên province từ selectedProvince
                const provinceOption = getProvinceOptions()?.find(p => p.value === selectedProvince);
                if (provinceOption) {
                    filterParams.province = provinceOption.label;
                }
            }

            if (selectedDistrict) {
                // Lấy tên district từ selectedDistrict
                const districtOption = getDistrictOptions(selectedProvince)?.find(d => d.value === selectedDistrict);
                if (districtOption) {
                    filterParams.district = districtOption.label;
                }
            }

            console.log('📊 Loading analytics data with filters:', filterParams);

            // === TỔNG QUAN TOÀN QUỐC (không filter) ===

            // Dashboard Summary - luôn hiển thị toàn quốc
            const summaryData = await realEstateAPI.analytics.dashboardSummary();
            setDashboardSummary(summaryData.dashboard_summary || {});
            console.log('📊 Dashboard Summary Data (toàn quốc):', summaryData);

            // Market Overview - luôn hiển thị toàn quốc
            const overview = await realEstateAPI.analytics.marketOverview();
            setMarketOverview(overview.market_overview || {});

            // === THỐNG KÊ CHI TIẾT (có filter theo province/district) ===

            // Price Distribution - áp dụng filter
            const priceData = await realEstateAPI.analytics.priceDistribution(filterParams);
            setPriceDistribution(priceData.price_distribution || []);

            // Property Type Stats - áp dụng filter
            const typeData = await realEstateAPI.analytics.propertyTypeStats(filterParams);
            setPropertyTypeStats(typeData.property_type_stats || []);

            // Location Stats - áp dụng filter
            const locationData = await realEstateAPI.analytics.locationStats(filterParams);
            setLocationStats(locationData.location_stats || []);

            // Price Trends - áp dụng filter
            const trendData = await realEstateAPI.analytics.priceTrends({
                ...filterParams,
                months: selectedTimeRange === '1month' ? 1 :
                    selectedTimeRange === '3months' ? 3 :
                        selectedTimeRange === '1year' ? 12 : 6
            });
            setPriceTrends(trendData.price_trends || []);

            // Area Distribution - áp dụng filter
            const areaData = await realEstateAPI.analytics.areaDistribution(filterParams);
            setAreaDistribution(areaData.area_distribution || []);

            notification.success({
                message: '📊 Tải dữ liệu thành công',
                description: 'Đã cập nhật tất cả thống kê thị trường',
            });

        } catch (error) {
            console.error('Failed to load analytics data:', error);
            notification.error({
                message: '❌ Tải dữ liệu thất bại',
                description: 'Có lỗi xảy ra khi tải dữ liệu phân tích',
            });
        } finally {
            setLoading(false);
        }
    };

    // Load data on mount and when filters change
    useEffect(() => {
        loadAnalyticsData();
    }, [selectedProvince, selectedDistrict, selectedTimeRange]);

    // Reset district when province changes
    useEffect(() => {
        if (selectedProvince) {
            setSelectedDistrict(''); // Clear district when province changes
        }
    }, [selectedProvince]);

    // Format price
    const formatPrice = (price: number | undefined | null) => {
        if (!price || isNaN(price)) return '0';
        if (price >= 1000000000) {
            return `${(price / 1000000000).toFixed(1)} tỷ`;
        } else if (price >= 1000000) {
            return `${(price / 1000000).toFixed(0)} triệu`;
        }
        return `${price.toLocaleString()}`;
    };

    // Format number
    const formatNumber = (num: number | undefined | null) => {
        if (!num || isNaN(num)) return '0';
        return num.toLocaleString();
    };

    // Get trend icon
    const getTrendIcon = (change: number | undefined | null) => {
        if (!change || isNaN(change)) return null;
        return change > 0 ? <RiseOutlined style={{ color: '#52c41a' }} /> :
            change < 0 ? <FallOutlined style={{ color: '#f5222d' }} /> : null;
    };

    // Table columns for location stats
    const locationColumns = [
        {
            title: 'Khu vực',
            dataIndex: 'location',
            key: 'location',
            render: (text: string) => (
                <Space>
                    <EnvironmentOutlined />
                    <Text strong>{text}</Text>
                </Space>
            ),
        },
        {
            title: 'Số lượng',
            dataIndex: 'count',
            key: 'count',
            render: (count: number) => formatNumber(count),
            sorter: (a: any, b: any) => a.count - b.count,
        },
        {
            title: 'Giá trung bình',
            dataIndex: 'avg_price',
            key: 'avg_price',
            render: (price: number) => `${formatPrice(price)} VND`,
            sorter: (a: any, b: any) => a.avg_price - b.avg_price,
        },
        {
            title: 'Xu hướng',
            dataIndex: 'trend',
            key: 'trend',
            render: (trend: number | undefined | null) => (
                <Space>
                    {getTrendIcon(trend)}
                    <Text type={trend && trend > 0 ? 'success' : trend && trend < 0 ? 'danger' : undefined}>
                        {trend && !isNaN(trend) ? `${trend > 0 ? '+' : ''}${trend.toFixed(1)}%` : '0%'}
                    </Text>
                </Space>
            ),
        },
    ];

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 p-6">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <div className="text-center mb-8">
                    <Title level={2} className="mb-2">
                        <BarChartOutlined className="mr-3 text-blue-600" />
                        Phân Tích Thị Trường
                    </Title>
                    <Text type="secondary" className="text-lg">
                        Thống kê và xu hướng thị trường bất động sản
                    </Text>
                </div>

                <Spin spinning={loading}>
                    {/* Market Overview Cards */}
                    {dashboardSummary && Object.keys(dashboardSummary).length > 0 && (
                        <div className="mb-6">
                            <div className="text-center mb-4">
                                <Title level={4} className="mb-1">📊 Tổng quan toàn quốc</Title>
                                <Text type="secondary">Dữ liệu tổng hợp từ tất cả tỉnh/thành</Text>
                            </div>
                            <Row gutter={[24, 24]} className="mb-6">
                                <Col xs={24} sm={12} lg={6}>
                                    <Card className="shadow-lg">
                                        <Statistic
                                            title="Tổng số BDS"
                                            value={dashboardSummary.total_properties || 0}
                                            prefix={<HomeOutlined />}
                                            formatter={(value) => formatNumber(Number(value))}
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} lg={6}>
                                    <Card className="shadow-lg">
                                        <Statistic
                                            title="Giá trung bình"
                                            value={dashboardSummary.avg_price || 0}
                                            prefix={<DollarOutlined />}
                                            formatter={(value) => formatPrice(Number(value))}
                                            suffix="VND"
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} lg={6}>
                                    <Card className="shadow-lg">
                                        <Statistic
                                            title="Diện tích TB"
                                            value={dashboardSummary.avg_area || 0}
                                            suffix="m²"
                                            formatter={(value) => {
                                                const num = Number(value);
                                                return isNaN(num) ? '0' : num.toFixed(1);
                                            }}
                                        />
                                    </Card>
                                </Col>
                                <Col xs={24} sm={12} lg={6}>
                                    <Card className="shadow-lg">
                                        <Statistic
                                            title="Số tỉnh/thành"
                                            value={dashboardSummary.provinces_count || 0}
                                            prefix={<EnvironmentOutlined />}
                                        />
                                    </Card>
                                </Col>
                            </Row>
                        </div>
                    )}

                    {/* Additional Overview Cards */}
                    {dashboardSummary && dashboardSummary.top_provinces && (
                        <Row gutter={[24, 24]} className="mb-6">
                            <Col xs={24} lg={12}>
                                <Card
                                    title={
                                        <Space>
                                            <TrophyOutlined />
                                            Top tỉnh/thành
                                        </Space>
                                    }
                                    className="shadow-lg"
                                >
                                    <List
                                        dataSource={dashboardSummary.top_provinces}
                                        renderItem={(item: any, index: number) => (
                                            <List.Item>
                                                <List.Item.Meta
                                                    avatar={<Tag color={COLORS[index % COLORS.length]}>{index + 1}</Tag>}
                                                    title={item.province}
                                                    description={`${formatNumber(item.count)} bất động sản`}
                                                />
                                            </List.Item>
                                        )}
                                    />
                                </Card>
                            </Col>
                            <Col xs={24} lg={12}>
                                <Card
                                    title={
                                        <Space>
                                            <HomeOutlined />
                                            Top loại BDS
                                        </Space>
                                    }
                                    className="shadow-lg"
                                >
                                    <List
                                        dataSource={dashboardSummary.top_property_types}
                                        renderItem={(item: any, index: number) => (
                                            <List.Item>
                                                <List.Item.Meta
                                                    avatar={<Tag color={COLORS[index % COLORS.length]}>{index + 1}</Tag>}
                                                    title={item.type}
                                                    description={`${formatNumber(item.count)} bất động sản`}
                                                />
                                            </List.Item>
                                        )}
                                    />
                                </Card>
                            </Col>
                        </Row>
                    )}

                    {/* Filters */}
                    <Card className="mb-6 shadow-lg">
                        <Row gutter={16} align="middle">
                            <Col>
                                <Text strong>Bộ lọc: </Text>
                            </Col>
                            <Col>
                                <Space>
                                    <Select
                                        placeholder="Tất cả tỉnh/thành"
                                        value={selectedProvince}
                                        onChange={setSelectedProvince}
                                        style={{ width: 200 }}
                                        loading={addressLoading}
                                    >
                                        <Option value="">Tất cả tỉnh/thành</Option>
                                        {getProvinceOptions()?.map(province => (
                                            <Option key={province.value} value={province.value}>
                                                {province.label}
                                            </Option>
                                        ))}
                                    </Select>

                                    <Select
                                        placeholder="Tất cả quận/huyện"
                                        value={selectedDistrict}
                                        onChange={setSelectedDistrict}
                                        style={{ width: 200 }}
                                        loading={addressLoading}
                                        disabled={!selectedProvince}
                                    >
                                        <Option value="">Tất cả quận/huyện</Option>
                                        {getDistrictOptions(selectedProvince)?.map(district => (
                                            <Option key={district.value} value={district.value}>
                                                {district.label}
                                            </Option>
                                        ))}
                                    </Select>

                                    <Select
                                        value={selectedTimeRange}
                                        onChange={setSelectedTimeRange}
                                        style={{ width: 150 }}
                                    >
                                        <Option value="1month">1 tháng</Option>
                                        <Option value="3months">3 tháng</Option>
                                        <Option value="6months">6 tháng</Option>
                                        <Option value="1year">1 năm</Option>
                                    </Select>
                                </Space>
                            </Col>
                        </Row>
                    </Card>

                    {/* Charts */}
                    <div className="text-center mb-6">
                        <Title level={4} className="mb-1">📈 Thống kê chi tiết</Title>
                        <Text type="secondary">
                            Dữ liệu được lọc theo {selectedProvince ?
                                (getProvinceOptions()?.find(p => p.value === selectedProvince)?.label || 'tỉnh/thành đã chọn') :
                                'toàn quốc'}
                            {selectedDistrict &&
                                ` - ${getDistrictOptions(selectedProvince)?.find(d => d.value === selectedDistrict)?.label || 'quận/huyện đã chọn'}`
                            }
                        </Text>
                    </div>
                    <Row gutter={[24, 24]}>
                        {/* Price Distribution */}
                        <Col xs={24} lg={12}>
                            <Card
                                title={
                                    <Space>
                                        <PieChartOutlined />
                                        Phân bổ giá bán
                                    </Space>
                                }
                                className="shadow-lg"
                            >
                                {priceDistribution.length > 0 ? (
                                    <div className="space-y-4">
                                        {priceDistribution.map((item, index) => (
                                            <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                                <div>
                                                    <Text strong>{item.range}</Text>
                                                    <br />
                                                    <Text type="secondary">{item.avg_price_formatted}</Text>
                                                </div>
                                                <div className="flex items-center space-x-2">
                                                    <Progress
                                                        percent={Math.round((item.count / Math.max(...priceDistribution.map(p => p.count))) * 100)}
                                                        showInfo={false}
                                                        strokeColor={COLORS[index % COLORS.length]}
                                                        style={{ width: 100 }}
                                                    />
                                                    <Text strong>{formatNumber(item.count)}</Text>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <Empty description="Không có dữ liệu phân bổ giá" />
                                )}
                            </Card>
                        </Col>

                        {/* Property Type Stats */}
                        <Col xs={24} lg={12}>
                            <Card
                                title={
                                    <Space>
                                        <BarChartOutlined />
                                        Thống kê theo loại BDS
                                    </Space>
                                }
                                className="shadow-lg"
                            >
                                {propertyTypeStats.length > 0 ? (
                                    <div className="space-y-4">
                                        {propertyTypeStats.map((item, index) => (
                                            <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                                <div>
                                                    <Text strong>{item.type}</Text>
                                                    <br />
                                                    <Text type="secondary">{item.avg_price_formatted || 'N/A'}</Text>
                                                </div>
                                                <div className="flex items-center space-x-2">
                                                    <Progress
                                                        percent={Math.round((item.count / Math.max(...propertyTypeStats.map(p => p.count))) * 100)}
                                                        showInfo={false}
                                                        strokeColor={COLORS[index % COLORS.length]}
                                                        style={{ width: 100 }}
                                                    />
                                                    <Text strong>{formatNumber(item.count)}</Text>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <Empty description="Không có dữ liệu loại BDS" />
                                )}
                            </Card>
                        </Col>

                        {/* Price Trends */}
                        {/* <Col xs={24}>
                            <Card
                                title={
                                    <Space>
                                        <LineChartOutlined />
                                        Xu hướng giá theo thời gian
                                    </Space>
                                }
                                className="shadow-lg"
                            >
                                {priceTrends.length > 0 ? (
                                    <div className="space-y-4">
                                        <Row gutter={16}>
                                            {priceTrends.slice(0, 6).map((item, index) => (
                                                <Col xs={24} sm={12} md={8} lg={4} key={index}>
                                                    <Card size="small" className="text-center">
                                                        <Statistic
                                                            title={item.period}
                                                            value={item.avg_price}
                                                            formatter={(value) => formatPrice(Number(value))}
                                                            suffix="VND"
                                                            valueStyle={{ fontSize: '14px' }}
                                                        />
                                                    </Card>
                                                </Col>
                                            ))}
                                        </Row>
                                        {priceTrends.length > 6 && (
                                            <div className="text-center">
                                                <Text type="secondary">và {priceTrends.length - 6} thời kỳ khác...</Text>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <Empty description="Không có dữ liệu xu hướng giá" />
                                )}
                            </Card>
                        </Col> */}

                        {/* Area Distribution */}
                        <Col xs={24} lg={12}>
                            <Card
                                title={
                                    <Space>
                                        <BarChartOutlined />
                                        Phân bổ diện tích
                                    </Space>
                                }
                                className="shadow-lg"
                            >
                                {areaDistribution.length > 0 ? (
                                    <div className="space-y-4">
                                        {areaDistribution.map((item, index) => (
                                            <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                                <div>
                                                    <Text strong>{item.range}</Text>
                                                    <br />
                                                    <Text type="secondary">{item.avg_area ? `TB: ${item.avg_area}m²` : 'N/A'}</Text>
                                                </div>
                                                <div className="flex items-center space-x-2">
                                                    <Progress
                                                        percent={Math.round((item.count / Math.max(...areaDistribution.map(p => p.count))) * 100)}
                                                        showInfo={false}
                                                        strokeColor="#00C49F"
                                                        style={{ width: 100 }}
                                                    />
                                                    <Text strong>{formatNumber(item.count)}</Text>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <Empty description="Không có dữ liệu phân bổ diện tích" />
                                )}
                            </Card>
                        </Col>

                        {/* Location Stats Table - Temporarily disabled to fix toFixed error */}
                        {/* <Col xs={24} lg={12}>
                            <Card
                                title={
                                    <Space>
                                        <TrophyOutlined />
                                        Top khu vực
                                    </Space>
                                }
                                className="shadow-lg"
                            >

                                <div className="text-center p-4 bg-blue-50 rounded mb-4">
                                    <Text type="secondary">
                                        💡 Để hiển thị biểu đồ đẹp hơn, cần cài đặt thư viện recharts: <code>npm install recharts</code>
                                    </Text>
                                </div>
                                Temporarily commented out to fix toFixed error
                                {locationStats.length > 0 ? (
                                    <Table
                                        dataSource={locationStats}
                                        columns={locationColumns}
                                        pagination={{ pageSize: 5 }}
                                        size="small"
                                        rowKey="location"
                                    />
                                ) : (
                                    <Empty description="Không có dữ liệu khu vực" />
                                )}
                                <div className="p-4 text-center">
                                    <Text type="secondary">
                                        Tính năng đang được phát triển... Tạm thời tắt để tránh lỗi toFixed.
                                    </Text>
                                </div>
                            </Card>
                        </Col> */}
                    </Row>
                </Spin>
            </div>
        </div>
    );
}
