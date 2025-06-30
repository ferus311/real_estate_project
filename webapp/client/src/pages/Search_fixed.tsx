import { useState, useEffect } from 'react';
import {
    Card,
    Form,
    Input,
    InputNumber,
    Select,
    Button,
    Row,
    Col,
    List,
    Spin,
    Tag,
    Divider,
    Empty,
    Pagination,
    Space,
    Slider,
    notification,
    Typography,
    Modal,
    Tooltip,
    Statistic,
} from 'antd';
import {
    SearchOutlined,
    HomeOutlined,
    EnvironmentOutlined,
    DollarOutlined,
    ExpandOutlined,
    TeamOutlined,
    FilterOutlined,
    ClearOutlined,
    EyeOutlined,
    LinkOutlined,
    CopyOutlined,
} from '@ant-design/icons';
import { useAddressData } from '../hooks/useAddressData';
import realEstateAPI, { SearchFilters, Property, getPropertyAddress, getPropertyShortAddress } from '../services/api';

const { Option } = Select;
const { Title, Text } = Typography;

export default function Search() {
    const [form] = Form.useForm();
    const [loading, setLoading] = useState(false);
    const [properties, setProperties] = useState<Property[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize] = useState(10);

    const {
        getProvinceOptions,
        getDistrictOptions,
        getWardOptions,
        getStreetOptions,
        loading: addressLoading,
    } = useAddressData();

    // Form state
    const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');
    const [selectedWard, setSelectedWard] = useState<string>('');
    const [selectedProperty, setSelectedProperty] = useState<Property | null>(null);
    const [propertyModalVisible, setPropertyModalVisible] = useState(false);

    // Price and area ranges
    const [priceRange, setPriceRange] = useState<[number, number]>([0, 50000000000]); // 0 - 50 tỷ
    const [areaRange, setAreaRange] = useState<[number, number]>([0, 1000]); // 0 - 1000m2

    // Utility functions
    const formatPrice = (price: number): string => {
        if (price >= 1_000_000_000) {
            return `${(price / 1_000_000_000).toFixed(1)} tỷ`;
        } else if (price >= 1_000_000) {
            return `${(price / 1_000_000).toFixed(0)} triệu`;
        }
        return price.toLocaleString();
    };

    const copyUrl = (url: string) => {
        navigator.clipboard.writeText(url).then(() => {
            notification.success({
                message: '✅ Đã copy URL',
                description: 'URL đã được copy vào clipboard',
            });
        }).catch(() => {
            notification.error({
                message: '❌ Copy thất bại',
                description: 'Không thể copy URL',
            });
        });
    };

    // Handle property detail modal
    const handlePropertyDetail = (property: Property) => {
        setSelectedProperty(property);
        setPropertyModalVisible(true);
    };

    // Handle search
    const handleSearch = async (values: any = {}) => {
        setLoading(true);
        try {
            const searchData: SearchFilters = {
                ...values,
                province: selectedProvince ? getProvinceOptions()?.find(p => p.value === selectedProvince)?.label : undefined,
                district: selectedDistrict ? getDistrictOptions(selectedProvince)?.find(d => d.value === selectedDistrict)?.label : undefined,
                ward: selectedWard ? getWardOptions(selectedProvince, selectedDistrict)?.find(w => w.value === selectedWard)?.label : undefined,
                street: values.street || undefined,
                price_min: priceRange[0] || undefined,
                price_max: priceRange[1] || undefined,
                area_min: areaRange[0] || undefined,
                area_max: areaRange[1] || undefined,
                limit: pageSize,
                offset: (currentPage - 1) * pageSize,
            };

            // Remove undefined values
            Object.keys(searchData).forEach(key => {
                if (searchData[key as keyof SearchFilters] === undefined) {
                    delete searchData[key as keyof SearchFilters];
                }
            });

            // Add pagination to search data
            searchData.page = currentPage;
            searchData.page_size = pageSize;

            console.log('🔍 Searching with filters:', searchData);

            const response = await realEstateAPI.search.advanced(searchData);
            console.log('🔍 Search response:', response);

            setProperties(response.results || []);
            // Backend trả về total_count trong pagination object
            const totalCount = response.pagination?.total_count || response.count || 0;
            setTotal(totalCount);

            notification.success({
                message: '🔍 Tìm kiếm thành công',
                description: `Tìm thấy ${totalCount} bất động sản (trang ${currentPage})`,
            });
        } catch (error) {
            console.error('Search failed:', error);
            notification.error({
                message: '❌ Tìm kiếm thất bại',
                description: 'Có lỗi xảy ra khi tìm kiếm bất động sản',
            });
        } finally {
            setLoading(false);
        }
    };

    // Clear filters
    const handleClearFilters = () => {
        form.resetFields();
        setSelectedProvince('');
        setSelectedDistrict('');
        setSelectedWard('');
        setPriceRange([0, 50000000000]);
        setAreaRange([0, 1000]);
        setCurrentPage(1);
        setProperties([]);
        setTotal(0);
    };

    // Handle pagination
    const handlePageChange = (page: number) => {
        setCurrentPage(page);
    };

    // Search when page changes
    useEffect(() => {
        if (currentPage > 1) {
            handleSearch(form.getFieldsValue());
        }
    }, [currentPage]);

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 p-6">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <div className="text-center mb-8">
                    <Title level={2} className="mb-2">
                        <SearchOutlined className="mr-3 text-blue-600" />
                        Tìm Kiếm Bất Động Sản
                    </Title>
                    <Text type="secondary" className="text-lg">
                        Tìm kiếm và lọc bất động sản theo tiêu chí của bạn
                    </Text>
                </div>

                <Row gutter={[24, 24]}>
                    {/* Search Filters */}
                    <Col xs={24} lg={8}>
                        <Card
                            title={
                                <Space>
                                    <FilterOutlined />
                                    Bộ Lọc Tìm Kiếm
                                </Space>
                            }
                            extra={
                                <Button
                                    type="link"
                                    icon={<ClearOutlined />}
                                    onClick={handleClearFilters}
                                    size="small"
                                >
                                    Xóa bộ lọc
                                </Button>
                            }
                            className="shadow-lg"
                        >
                            <Form
                                form={form}
                                layout="vertical"
                                onFinish={handleSearch}
                                initialValues={{
                                    property_type: '',
                                    listing_type: '',
                                }}
                            >
                                {/* Keyword Search */}
                                <Form.Item name="keyword" label="Từ khóa">
                                    <Input
                                        placeholder="Nhập từ khóa tìm kiếm..."
                                        prefix={<SearchOutlined />}
                                    />
                                </Form.Item>

                                {/* Location */}
                                <Form.Item name="province_id" label="Tỉnh/Thành phố">
                                    <Select
                                        placeholder="Chọn tỉnh/thành phố"
                                        value={selectedProvince}
                                        onChange={(value) => {
                                            setSelectedProvince(value);
                                            setSelectedDistrict('');
                                            setSelectedWard('');
                                            form.setFieldsValue({
                                                district_id: undefined,
                                                ward_id: undefined,
                                            });
                                        }}
                                        loading={addressLoading}
                                        showSearch
                                        filterOption={(input, option) =>
                                            String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                                        }
                                    >
                                        <Option value="">Tất cả</Option>
                                        {getProvinceOptions()?.map(province => (
                                            <Option key={province.value} value={province.value}>
                                                {province.label}
                                            </Option>
                                        ))}
                                    </Select>
                                </Form.Item>

                                <Form.Item name="district_id" label="Quận/Huyện">
                                    <Select
                                        placeholder="Chọn quận/huyện"
                                        value={selectedDistrict}
                                        onChange={(value) => {
                                            setSelectedDistrict(value);
                                            setSelectedWard('');
                                            form.setFieldValue('ward_id', undefined);
                                        }}
                                        disabled={!selectedProvince}
                                        showSearch
                                        filterOption={(input, option) =>
                                            String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                                        }
                                    >
                                        <Option value="">Tất cả</Option>
                                        {getDistrictOptions(selectedProvince)?.map(district => (
                                            <Option key={district.value} value={district.value}>
                                                {district.label}
                                            </Option>
                                        ))}
                                    </Select>
                                </Form.Item>

                                <Form.Item name="ward_id" label="Phường/Xã">
                                    <Select
                                        placeholder="Chọn phường/xã"
                                        value={selectedWard}
                                        onChange={setSelectedWard}
                                        disabled={!selectedDistrict}
                                        showSearch
                                        filterOption={(input, option) =>
                                            String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                                        }
                                    >
                                        <Option value="">Tất cả</Option>
                                        {getWardOptions(selectedProvince, selectedDistrict)?.map(ward => (
                                            <Option key={ward.value} value={ward.value}>
                                                {ward.label}
                                            </Option>
                                        ))}
                                    </Select>
                                </Form.Item>

                                <Form.Item name="street" label="Đường/Địa chỉ cụ thể">
                                    <Input
                                        placeholder="Nhập tên đường hoặc địa chỉ cụ thể..."
                                        prefix={<EnvironmentOutlined />}
                                    />
                                </Form.Item>

                                {/* Property Type - Disabled */}
                                <Form.Item name="property_type" label="Loại bất động sản">
                                    <Select
                                        placeholder="Tất cả loại bất động sản"
                                        disabled
                                        value=""
                                    >
                                        <Option value="">Tất cả</Option>
                                        <Option value="house">Nhà ở</Option>
                                        <Option value="apartment">Chung cư</Option>
                                        <Option value="land">Đất nền</Option>
                                        <Option value="commercial">Thương mại</Option>
                                    </Select>
                                </Form.Item>

                                {/* Listing Type - Disabled */}
                                <Form.Item name="listing_type" label="Hình thức">
                                    <Select
                                        placeholder="Tất cả hình thức"
                                        disabled
                                        value=""
                                    >
                                        <Option value="">Tất cả</Option>
                                        <Option value="sale">Bán</Option>
                                        <Option value="rent">Cho thuê</Option>
                                    </Select>
                                </Form.Item>

                                {/* Price Range */}
                                <Form.Item label="Khoảng giá (VND)">
                                    <Slider
                                        range
                                        min={0}
                                        max={50000000000}
                                        step={100000000}
                                        value={priceRange}
                                        onChange={(value) => setPriceRange(value as [number, number])}
                                        tooltip={{
                                            formatter: (value) => formatPrice(value || 0)
                                        }}
                                    />
                                    <div className="flex justify-between text-xs text-gray-500 mt-1">
                                        <span>{formatPrice(priceRange[0])}</span>
                                        <span>{formatPrice(priceRange[1])}</span>
                                    </div>
                                </Form.Item>

                                {/* Area Range */}
                                <Form.Item label="Diện tích (m²)">
                                    <Slider
                                        range
                                        min={0}
                                        max={1000}
                                        step={10}
                                        value={areaRange}
                                        onChange={(value) => setAreaRange(value as [number, number])}
                                    />
                                    <div className="flex justify-between text-xs text-gray-500 mt-1">
                                        <span>{areaRange[0]}m²</span>
                                        <span>{areaRange[1]}m²</span>
                                    </div>
                                </Form.Item>

                                {/* Bedrooms */}
                                <Form.Item name="bedroom" label="Số phòng ngủ">
                                    <Select placeholder="Tất cả">
                                        <Option value="">Tất cả</Option>
                                        <Option value={1}>1 phòng</Option>
                                        <Option value={2}>2 phòng</Option>
                                        <Option value={3}>3 phòng</Option>
                                        <Option value={4}>4 phòng</Option>
                                        <Option value={5}>5+ phòng</Option>
                                    </Select>
                                </Form.Item>

                                {/* Search Button */}
                                <Form.Item>
                                    <Button
                                        type="primary"
                                        htmlType="submit"
                                        loading={loading}
                                        icon={<SearchOutlined />}
                                        size="large"
                                        className="w-full"
                                    >
                                        Tìm kiếm
                                    </Button>
                                </Form.Item>
                            </Form>
                        </Card>
                    </Col>

                    {/* Search Results */}
                    <Col xs={24} lg={16}>
                        <Card
                            title={
                                <Space>
                                    <HomeOutlined />
                                    Kết Quả Tìm Kiếm
                                    <Tag color="blue">{total} kết quả</Tag>
                                </Space>
                            }
                            className="shadow-lg"
                        >
                            <Spin spinning={loading}>
                                {properties.length > 0 ? (
                                    <>
                                        <List
                                            dataSource={properties}
                                            renderItem={(property) => (
                                                <List.Item
                                                    key={property.id}
                                                    actions={[
                                                        <Button
                                                            type="link"
                                                            icon={<EyeOutlined />}
                                                            onClick={() => handlePropertyDetail(property)}
                                                        >
                                                            Xem chi tiết
                                                        </Button>
                                                    ]}
                                                >
                                                    <List.Item.Meta
                                                        title={
                                                            <Space direction="vertical" size="small" className="w-full">
                                                                <Text strong className="text-lg">
                                                                    {property.title || 'Bất động sản'}
                                                                </Text>
                                                                <Space wrap>
                                                                    <Tag color="green" icon={<DollarOutlined />}>
                                                                        {formatPrice(property.price || 0)} VND
                                                                    </Tag>
                                                                    <Tag color="blue" icon={<ExpandOutlined />}>
                                                                        {property.area || 0}m²
                                                                    </Tag>
                                                                    {property.bedroom && (
                                                                        <Tag color="purple" icon={<TeamOutlined />}>
                                                                            {property.bedroom} PN
                                                                        </Tag>
                                                                    )}
                                                                </Space>
                                                            </Space>
                                                        }
                                                        description={
                                                            <Space direction="vertical" size="small" style={{ width: '100%' }}>
                                                                <Text>
                                                                    <EnvironmentOutlined className="mr-1" />
                                                                    {(property as any).location || getPropertyAddress(property) || 'Không có địa chỉ'}
                                                                </Text>
                                                                {property.description && (
                                                                    <div style={{ width: '100%', maxWidth: '500px' }}>
                                                                        <Text
                                                                            type="secondary"
                                                                            ellipsis={{ tooltip: property.description }}
                                                                            style={{
                                                                                display: 'block',
                                                                                maxWidth: '100%'
                                                                            }}
                                                                        >
                                                                            {property.description.length > 100
                                                                                ? `${property.description.substring(0, 100)}...`
                                                                                : property.description
                                                                            }
                                                                        </Text>
                                                                    </div>
                                                                )}
                                                            </Space>
                                                        }
                                                    />
                                                </List.Item>
                                            )}
                                        />

                                        {/* Pagination */}
                                        {total > pageSize && (
                                            <>
                                                <Divider />
                                                <div className="text-center">
                                                    <Pagination
                                                        current={currentPage}
                                                        total={total}
                                                        pageSize={pageSize}
                                                        onChange={handlePageChange}
                                                        showSizeChanger={false}
                                                        showQuickJumper
                                                        showTotal={(total, range) =>
                                                            `${range[0]}-${range[1]} của ${total} kết quả`
                                                        }
                                                    />
                                                </div>
                                            </>
                                        )}
                                    </>
                                ) : (
                                    <Empty
                                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                                        description="Không tìm thấy bất động sản nào phù hợp"
                                    />
                                )}
                            </Spin>
                        </Card>
                    </Col>
                </Row>

                {/* Property Detail Modal */}
                <Modal
                    title="Chi tiết bất động sản"
                    open={propertyModalVisible}
                    onCancel={() => setPropertyModalVisible(false)}
                    footer={null}
                    width={800}
                >
                    {selectedProperty && (
                        <div className="space-y-4">
                            {/* Price and Area */}
                            <Row gutter={[16, 16]}>
                                <Col span={12}>
                                    <Card size="small" className="text-center">
                                        <Statistic
                                            title="Giá bán"
                                            value={selectedProperty.price || 0}
                                            formatter={(value) => formatPrice(Number(value))}
                                            suffix="VND"
                                            prefix="💰"
                                            valueStyle={{ color: '#f56a00' }}
                                        />
                                    </Card>
                                </Col>
                                <Col span={12}>
                                    <Card size="small" className="text-center">
                                        <Statistic
                                            title="Diện tích"
                                            value={selectedProperty.area || 0}
                                            suffix="m²"
                                            prefix="📐"
                                            valueStyle={{ color: '#1890ff' }}
                                        />
                                    </Card>
                                </Col>
                            </Row>

                            {/* Address */}
                            <div>
                                <Text strong className="block mb-2">📍 Địa chỉ:</Text>
                                <Card className="bg-gray-50">
                                    <Text>
                                        {(selectedProperty as any).location || getPropertyAddress(selectedProperty) || '(Không có địa chỉ)'}
                                    </Text>
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

                            {/* Property Details */}
                            <Row gutter={[16, 16]}>
                                {selectedProperty.bedroom && (
                                    <Col span={8}>
                                        <div className="text-center">
                                            <Text strong>🛏️ Phòng ngủ:</Text>
                                            <div>{selectedProperty.bedroom}</div>
                                        </div>
                                    </Col>
                                )}
                                {selectedProperty.bathroom && (
                                    <Col span={8}>
                                        <div className="text-center">
                                            <Text strong>🚿 Phòng tắm:</Text>
                                            <div>{selectedProperty.bathroom}</div>
                                        </div>
                                    </Col>
                                )}
                                {(selectedProperty as any).floor_count && (
                                    <Col span={8}>
                                        <div className="text-center">
                                            <Text strong>🏢 Số tầng:</Text>
                                            <div>{(selectedProperty as any).floor_count}</div>
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
            </div>
        </div>
    );
}
