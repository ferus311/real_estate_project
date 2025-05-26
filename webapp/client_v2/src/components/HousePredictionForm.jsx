import { useState } from "react";
import {
  Form,
  Input,
  InputNumber,
  Button,
  Card,
  Row,
  Col,
  Radio,
  Divider,
  Tooltip,
} from "antd";
import {
  HomeOutlined,
  EnvironmentOutlined,
  PartitionOutlined,
  DollarOutlined,
  RocketOutlined,
  TeamOutlined,
  ColumnWidthOutlined,
  BuildOutlined,
  AimOutlined,
  GlobalOutlined,
  CompassOutlined,
  InfoCircleOutlined,
} from "@ant-design/icons";
import ClickableMap from "./ClickableMap";
import "../styles/HousePredictionForm.css";

const HousePredictionForm = ({ onSubmit }) => {
  const [form] = Form.useForm();
  const [useCoordinates, setUseCoordinates] = useState(false);
  const [latLng, setLatLng] = useState({
    latitude: 21.0738016,
    longitude: 105.8231588,
  });

  const handleSubmit = (values) => {
    onSubmit(values);
  };

  const handleLocationTypeChange = (e) => {
    const isCoordinates = e.target.value === "coordinates";
    setUseCoordinates(isCoordinates);

    if (isCoordinates) {
      form.setFieldsValue({
        latitude: latLng.latitude,
        longitude: latLng.longitude,
        address: undefined,
      });
    } else {
      form.setFieldsValue({
        address: "Hà nội",
        latitude: undefined,
        longitude: undefined,
      });
    }
  };

  // Handle location selection from map
  const handleLocationSelect = (latitude, longitude) => {
    setLatLng({ latitude, longitude });
    form.setFieldsValue({
      latitude,
      longitude,
    });
  };

  return (
    <Card className="house-form-card">
      <div className="form-header-compact">
        <h3>
          <HomeOutlined /> Thông tin bất động sản
        </h3>
        <div className="required-hint">
          <InfoCircleOutlined /> <span>Vui lòng điền đầy đủ thông tin</span>
        </div>
      </div>

      <Form
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        initialValues={{
          bedroom: 3,
          bathroom: 2,
          facade_width: 5,
          floor_count: 4,
          area_m2: 75,
          address: "Hà nội",
          price_per_m2: 66666667,
          latitude: 21.0738016,
          longitude: 105.8231588,
        }}
        className="compact-form"
      >
        <div className="location-toggle">
          <span>Loại vị trí:</span>
          <Radio.Group
            value={useCoordinates ? "coordinates" : "address"}
            onChange={handleLocationTypeChange}
            optionType="button"
            buttonStyle="solid"
            size="small"
          >
            <Radio.Button value="address">
              <EnvironmentOutlined /> Địa chỉ
            </Radio.Button>
            <Radio.Button value="coordinates">
              <AimOutlined /> Tọa độ
            </Radio.Button>
          </Radio.Group>
        </div>

        {!useCoordinates ? (
          <Form.Item
            label={
              <span>
                <EnvironmentOutlined /> Địa chỉ (address)
              </span>
            }
            name="address"
            rules={[
              { required: !useCoordinates, message: "Vui lòng nhập địa chỉ" },
            ]}
            tooltip="Nhập địa chỉ của bất động sản"
          >
            <Input placeholder="Nhập địa chỉ của bất động sản" />
          </Form.Item>
        ) : (
          <>
            {/* Sử dụng ClickableMap */}
            <ClickableMap
              onLocationSelect={handleLocationSelect}
              initialLatitude={latLng.latitude}
              initialLongitude={latLng.longitude}
            />

            {/* Ẩn trường nhập tọa độ vì đã có trong bản đồ */}
            <Form.Item
              name="latitude"
              hidden={true}
              rules={[
                {
                  required: useCoordinates,
                  message: "Vui lòng chọn vị trí trên bản đồ",
                },
              ]}
            >
              <InputNumber />
            </Form.Item>

            <Form.Item
              name="longitude"
              hidden={true}
              rules={[
                {
                  required: useCoordinates,
                  message: "Vui lòng chọn vị trí trên bản đồ",
                },
              ]}
            >
              <InputNumber />
            </Form.Item>
          </>
        )}

        <Divider orientation="left" className="form-divider">
          <InfoCircleOutlined /> Thông số cơ bản
        </Divider>

        <Row gutter={[16, 8]}>
          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <PartitionOutlined /> Diện tích (area_m2)
                </span>
              }
              name="area_m2"
              rules={[{ required: true, message: "Vui lòng nhập diện tích" }]}
              tooltip="Diện tích tổng thể của bất động sản"
            >
              <InputNumber
                min={1}
                style={{ width: "100%" }}
                placeholder="75"
                addonAfter="m²"
              />
            </Form.Item>
          </Col>

          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <DollarOutlined /> Giá trên m² (price_per_m2)
                </span>
              }
              name="price_per_m2"
              rules={[{ required: true, message: "Vui lòng nhập giá trên m²" }]}
              tooltip="Giá trên mỗi mét vuông (đơn vị: VND)"
            >
              <InputNumber
                min={0}
                style={{ width: "100%" }}
                placeholder="66666667"
                formatter={(value) =>
                  `${value}`.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                }
                parser={(value) => value.replace(/\$\s?|(,*)/g, "")}
                addonAfter="VND"
              />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={[16, 8]}>
          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <TeamOutlined /> Số phòng ngủ (bedroom)
                </span>
              }
              name="bedroom"
              rules={[
                { required: true, message: "Vui lòng nhập số phòng ngủ" },
              ]}
              tooltip="Số lượng phòng ngủ của bất động sản"
            >
              <InputNumber min={0} style={{ width: "100%" }} placeholder="3" />
            </Form.Item>
          </Col>

          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <TeamOutlined /> Số phòng tắm (bathroom)
                </span>
              }
              name="bathroom"
              rules={[
                { required: true, message: "Vui lòng nhập số phòng tắm" },
              ]}
              tooltip="Số lượng phòng tắm của bất động sản"
            >
              <InputNumber min={0} style={{ width: "100%" }} placeholder="2" />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={[16, 8]}>
          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <BuildOutlined /> Số tầng (floor_count)
                </span>
              }
              name="floor_count"
              rules={[{ required: true, message: "Vui lòng nhập số tầng" }]}
              tooltip="Số tầng của bất động sản"
            >
              <InputNumber min={1} style={{ width: "100%" }} placeholder="4" />
            </Form.Item>
          </Col>

          <Col xs={24} sm={12}>
            <Form.Item
              label={
                <span>
                  <ColumnWidthOutlined /> Chiều rộng mặt tiền (facade_width)
                </span>
              }
              name="facade_width"
              rules={[
                {
                  required: true,
                  message: "Vui lòng nhập chiều rộng mặt tiền",
                },
              ]}
              tooltip="Chiều rộng phần mặt tiền của bất động sản"
            >
              <InputNumber
                min={0}
                step={0.1}
                style={{ width: "100%" }}
                placeholder="5"
                addonAfter="m"
              />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item className="submit-container-compact">
          <Button
            type="primary"
            htmlType="submit"
            icon={<RocketOutlined />}
            className="submit-button-compact"
          >
            Dự đoán giá nhà
          </Button>
        </Form.Item>
      </Form>
    </Card>
  );
};

export default HousePredictionForm;
