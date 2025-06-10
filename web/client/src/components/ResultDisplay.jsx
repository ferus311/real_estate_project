import { Button, Alert, Statistic, Card, Row, Col, Badge, Tooltip } from "antd";
import {
  HomeOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
  DollarOutlined,
  BarChartOutlined,
  CheckCircleOutlined,
} from "@ant-design/icons";
import { formatPrice } from "../services/apiService";
import "../styles/ResultDisplay.css";

const ResultDisplay = ({ result, error, onReset, emptyState }) => {
  const predictedPrice = result?.predicted_price || 0;

  if (emptyState) {
    return (
      <div className="result-card-mini empty-result">
        <div className="empty-icon">
          <BarChartOutlined />
        </div>
        <h3>Chưa có kết quả</h3>
        <p>Vui lòng nhập thông tin để dự đoán</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="result-card-mini error-result">
        <Alert
          message="Có lỗi xảy ra"
          description={error}
          type="error"
          showIcon
          action={
            <Button onClick={onReset} size="small" danger>
              Thử lại
            </Button>
          }
        />
      </div>
    );
  }

  return (
    <div className="result-card-mini">
      <div className="result-header">
        <Badge status="success" text="Dự đoán thành công" />
        <Button
          type="text"
          icon={<ReloadOutlined />}
          onClick={onReset}
          size="small"
          className="reset-icon-button"
        />
      </div>

      <div className="price-result">
        <div className="price-label-mini">
          <DollarOutlined /> Giá nhà dự đoán
        </div>
        <Statistic
          value={predictedPrice}
          formatter={(value) => formatPrice(value)}
          valueStyle={{
            color: "#2c6bed",
            fontSize: "24px",
            fontWeight: "700",
          }}
        />
      </div>
    </div>
  );
};

export default ResultDisplay;
