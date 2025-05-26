// Định nghĩa URL API - lấy từ biến môi trường hoặc sử dụng mặc định
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";

// Hàm dự đoán giá nhà
export const predictHousePrice = async (formData) => {
  try {
    const response = await fetch(`${API_URL}/predict_house_price/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(formData),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        errorData.message || "Không thể dự đoán giá nhà. Vui lòng thử lại!"
      );
    }

    return await response.json();
  } catch (error) {
    console.error("API error:", error);
    throw error;
  }
};

// Hàm format giá tiền theo định dạng Việt Nam
export const formatPrice = (price) => {
  return new Intl.NumberFormat("vi-VN", {
    style: "currency",
    currency: "VND",
    maximumFractionDigits: 0,
  }).format(price);
};
