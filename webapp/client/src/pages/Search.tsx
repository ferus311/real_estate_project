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
} from '@ant-design/icons';
import { useAddressData } from '../hooks/useAddressData';
import realEstateAPI, { SearchFilters, Property, getPropertyAddress } from '../services/api';

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
        loading: addressLoading,
    } = useAddressData();

    // Form state
    const [selectedProvince, setSelectedProvince] = useState<string>('');
    const [selectedDistrict, setSelectedDistrict] = useState<string>('');

    // Price and area ranges
    const [priceRange, setPriceRange] = useState<[number, number]>([0, 50000000000]); // 0 - 50 tỷ
    const [areaRange, setAreaRange] = useState<[number, number]>([0, 1000]); // 0 - 1000m2

    // Handle search
    const handleSearch = async (values: any = {}) => {
        setLoading(true);
        try {
            const searchData: SearchFilters = {
                ...values,
                province: selectedProvince ? getProvinceOptions()?.find(p => p.value === selectedProvince)?.label : undefined,
                district: selectedDistrict ? getDistrictOptions(selectedProvince)?.find(d => d.value === selectedDistrict)?.label : undefined,
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
                description: 'Có lỗi xảy ra khi tìm kiếm. Vui lòng thử lại.',
            });
        } finally {
            setLoading(false);
        }
    };

    // Handle clear filters
    const handleClearFilters = () => {
        form.resetFields();
        setSelectedProvince('');
        setSelectedDistrict('');
        setPriceRange([0, 50000000000]);
        setAreaRange([0, 1000]);
        setCurrentPage(1);
        setProperties([]);
        setTotal(0);
    };

    // Handle pagination
    const handlePageChange = (page: number) => {
        setCurrentPage(page);
        // Thêm page vào form data
        const formData = form.getFieldsValue();
        formData.page = page;
        handleSearch(formData);
    };

    // Format price
    const formatPrice = (price: number) => {
        if (price >= 1000000000) {
            return `${(price / 1000000000).toFixed(1)} tỷ`;
        } else if (price >= 1000000) {
            return `${(price / 1000000).toFixed(0)} triệu`;
        }
        return `${price.toLocaleString()}`;
    };

    // Initial search on mount
    useEffect(() => {
        handleSearch();
    }, []);

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
                                            form.setFieldValue('district_id', undefined);
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
                                        onChange={setSelectedDistrict}
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
                                        tooltip={{
                                            formatter: (value) => `${value}m²`
                                        }}
                                    />
                                    <div className="flex justify-between text-xs text-gray-500 mt-1">
                                        <span>{areaRange[0]}m²</span>
                                        <span>{areaRange[1]}m²</span>
                                    </div>
                                </Form.Item>

                                {/* Bedrooms */}
                                <Row gutter={12}>
                                    <Col span={12}>
                                        <Form.Item name="bedroom_min" label="Phòng ngủ tối thiểu">
                                            <InputNumber
                                                min={0}
                                                max={10}
                                                placeholder="0"
                                                className="w-full"
                                            />
                                        </Form.Item>
                                    </Col>
                                    <Col span={12}>
                                        <Form.Item name="bedroom_max" label="Phòng ngủ tối đa">
                                            <InputNumber
                                                min={0}
                                                max={10}
                                                placeholder="10"
                                                className="w-full"
                                            />
                                        </Form.Item>
                                    </Col>
                                </Row>

                                {/* Search Button */}
                                <Form.Item>
                                    <Button
                                        type="primary"
                                        htmlType="submit"
                                        loading={loading}
                                        icon={<SearchOutlined />}
                                        size="large"
                                        className="w-full bg-gradient-to-r from-blue-500 to-purple-600 border-0 shadow-lg hover:shadow-xl"
                                    >
                                        Tìm Kiếm
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
                                                            icon={<EnvironmentOutlined />}
                                                            onClick={() => {
                                                                notification.info({
                                                                    message: 'Chi tiết bất động sản',
                                                                    description: `ID: ${property.id}`,
                                                                });
                                                            }}
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
                                                                    </Tag>                                                    {property.bedroom && (
                                                                        <Tag color="purple" icon={<TeamOutlined />}>
                                                                            {property.bedroom} PN
                                                                        </Tag>
                                                                    )}
                                                                </Space>
                                                            </Space>
                                                        }
                                                        description={<Space direction="vertical" size="small" style={{ width: '100%' }}>
                                                            <Text>
                                                                <EnvironmentOutlined className="mr-1" />
                                                                {getPropertyAddress(property) || 'Không có địa chỉ'}
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
            </div>
        </div>
    );
}
