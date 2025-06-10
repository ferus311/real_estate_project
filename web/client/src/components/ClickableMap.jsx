import { useEffect, useRef, useState } from "react";
import { Button, Tooltip, Row, Col } from "antd";
import { InfoCircleOutlined, AimOutlined } from "@ant-design/icons";
import "../styles/ClickableMap.css";

const ClickableMap = ({
  onLocationSelect,
  initialLatitude,
  initialLongitude,
}) => {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const markerRef = useRef(null);
  const [position, setPosition] = useState({
    lat: initialLatitude || 21.0738016,
    lng: initialLongitude || 105.8231588,
  });

  // Khởi tạo bản đồ Leaflet khi component mount
  useEffect(() => {
    // Thêm CSS của Leaflet
    const leafletCSS = document.createElement("link");
    leafletCSS.rel = "stylesheet";
    leafletCSS.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
    leafletCSS.integrity =
      "sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=";
    leafletCSS.crossOrigin = "";
    document.head.appendChild(leafletCSS);

    // Thêm JavaScript của Leaflet
    const leafletScript = document.createElement("script");
    leafletScript.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    leafletScript.integrity =
      "sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=";
    leafletScript.crossOrigin = "";
    document.head.appendChild(leafletScript);

    // Đợi Leaflet load xong
    leafletScript.onload = () => {
      // Khởi tạo bản đồ nếu chưa có
      if (!mapRef.current && mapContainerRef.current) {
        const L = window.L;

        // Tạo bản đồ
        const map = L.map(mapContainerRef.current).setView(
          [position.lat, position.lng],
          13
        );
        mapRef.current = map;

        // Thêm layer bản đồ
        L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
          maxZoom: 19,
          attribution:
            '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        }).addTo(map);

        // Thêm marker ban đầu
        const marker = L.marker([position.lat, position.lng], {
          draggable: true,
        }).addTo(map);
        markerRef.current = marker;

        // Xử lý sự kiện click trên bản đồ
        map.on("click", function (e) {
          const { lat, lng } = e.latlng;
          marker.setLatLng([lat, lng]);
          updatePosition(lat, lng);
        });

        // Xử lý sự kiện kéo marker
        marker.on("dragend", function () {
          const position = marker.getLatLng();
          updatePosition(position.lat, position.lng);
        });
      }
    };

    // Cleanup khi unmount
    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
      if (document.head.contains(leafletCSS)) {
        document.head.removeChild(leafletCSS);
      }
    };
  }, []);

  // Cập nhật vị trí khi props thay đổi
  useEffect(() => {
    if (
      mapRef.current &&
      markerRef.current &&
      initialLatitude &&
      initialLongitude
    ) {
      const L = window.L;
      const newLatLng = [initialLatitude, initialLongitude];

      markerRef.current.setLatLng(newLatLng);
      mapRef.current.setView(newLatLng, mapRef.current.getZoom());

      setPosition({
        lat: initialLatitude,
        lng: initialLongitude,
      });
    }
  }, [initialLatitude, initialLongitude]);

  // Cập nhật vị trí và gọi callback
  const updatePosition = (lat, lng) => {
    setPosition({ lat, lng });
    onLocationSelect(lat, lng);
  };

  // Hàm trợ giúp để tìm vị trí hiện tại
  const getCurrentLocation = () => {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          const { latitude, longitude } = position.coords;

          if (mapRef.current && markerRef.current) {
            const L = window.L;
            const newLatLng = [latitude, longitude];

            markerRef.current.setLatLng(newLatLng);
            mapRef.current.setView(newLatLng, 15);

            updatePosition(latitude, longitude);
          }
        },
        (error) => {
          console.error("Lỗi khi lấy vị trí:", error);
          alert(
            "Không thể lấy vị trí hiện tại. Vui lòng chọn vị trí trên bản đồ."
          );
        }
      );
    } else {
      alert("Trình duyệt của bạn không hỗ trợ định vị vị trí.");
    }
  };

  return (
    <div className="clickable-map-container">
      <div ref={mapContainerRef} className="map-content"></div>

      <div className="map-coordinates">
        <Row gutter={[8, 8]}>
          <Col span={24}>
            <div className="coordinate-display">
              <div>
                <strong>Vĩ độ:</strong> {position.lat.toFixed(7)}
              </div>
              <div>
                <strong>Kinh độ:</strong> {position.lng.toFixed(7)}
              </div>
            </div>
          </Col>
        </Row>
      </div>

      <div className="map-controls">
        <Tooltip title="Sử dụng vị trí hiện tại của bạn">
          <Button
            type="primary"
            shape="circle"
            icon={<AimOutlined />}
            onClick={getCurrentLocation}
            className="location-button"
          />
        </Tooltip>
      </div>

      <div className="map-instruction">
        <InfoCircleOutlined /> Nhấp chuột vào bản đồ để chọn vị trí
      </div>
    </div>
  );
};

export default ClickableMap;
