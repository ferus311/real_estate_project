import { useState } from "react";
import SimpleLayout from "./components/SimpleLayout";
import { predictHousePrice } from "./services/apiService";
import HousePredictionForm from "./components/HousePredictionForm";
import ResultDisplay from "./components/ResultDisplay";
import { Typography, Spin, ConfigProvider, theme, Row, Col } from "antd";
import "./App.css";

const { Title } = Typography;

function App() {
  const [predictionResult, setPredictionResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (formData) => {
    setLoading(true);
    setError(null);
    try {
      // Real API call - uncomment in production
      const result = await predictHousePrice(formData);
      setPredictionResult(result);
      setLoading(false);
    } catch (err) {
      setError(err.message || "Có lỗi xảy ra khi dự đoán giá nhà");
      setLoading(false);
    }
  };

  const handleReset = () => {
    setPredictionResult(null);
    setError(null);
  };

  return (
    <ConfigProvider
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: "#C61E22",
          borderRadius: 8,
          fontFamily: "Montserrat, sans-serif",
          colorBgContainer: "#ffffff",
        },
      }}
    >
      <SimpleLayout appName="Dự Đoán Giá Nhà BĐS">
        <div className="main-container-fullscreen">
          <Row gutter={[16, 0]} className="content-row">
            <Col xs={24} lg={16} className="form-side">
              <HousePredictionForm onSubmit={handleSubmit} />
            </Col>

            <Col xs={24} lg={8} className="result-side">
              {loading ? (
                <div className="loading-overlay">
                  <Spin size="large" />
                  <p>Đang tính toán...</p>
                </div>
              ) : (
                <ResultDisplay
                  result={predictionResult}
                  error={error}
                  onReset={handleReset}
                  emptyState={!predictionResult && !error}
                />
              )}
            </Col>
          </Row>
        </div>
      </SimpleLayout>
    </ConfigProvider>
  );
}

export default App;
