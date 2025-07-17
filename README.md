# Hệ thống Dự đoán Giá Bất động sản

**Bạn có bao giờ thắc mắc liệu căn nhà mình muốn mua có phù hợp với số tiền mình bỏ ra hay không?**

Dự án này được sinh ra từ một nhu cầu thực tế: giúp người mua bất động sản đưa ra quyết định thông minh hơn dựa trên dữ liệu thực tế từ thị trường. Thay vì dựa vào cảm tính hay kinh nghiệm, bạn sẽ có một "cố vấn AI" dựa trên phân tích hàng trăm ngàn tin đăng thực tế.

## 🎯 Mục tiêu của dự án

**Thu thập** → **Xử lý"** → **Phân tích** → **Dự đoán** → **Hỗ trợ quyết định**

-   **Thu thập dữ liệu thông minh**: Tự động lấy thông tin từ các sàn BDS lớn (Batdongsan.com.vn, Chotot.vn) 24/7
-   **Dự đoán giá chính xác**: Sử dụng Gradient Boosting để đưa ra mức giá hợp lý dựa trên vị trí, diện tích, và đặc điểm bất động sản
-   **Thông tin minh bạch**: Cung cấp dashboard trực quan với biểu đồ, xu hướng giá theo khu vực và thời gian
-   **Hỗ trợ quyết định**: Giúp người dùng biết được một căn nhà có đắt hay rẻ so với thị trường

## 🏗️ Kiến trúc hệ thống

### Pipeline tự động hoàn chỉnh

```
📊 Nguồn dữ liệu → 🕷️ Crawler → 🔄 Airflow → 📦 Hadoop/HDFS → ⚡ Spark → 🤖 ML Model → 🌐 Website
```

**1. Thu thập dữ liệu (Data Collection)**

-   Crawler tự động chạy định kỳ với Apache Airflow
-   Thu thập từ Batdongsan và Chotot với khả năng mở rộng
-   Lưu trữ an toàn trên Hadoop HDFS

**2. Xử lý dữ liệu (Data Processing)**

-   Làm sạch và chuẩn hóa dữ liệu với Apache Spark
-   Xử lý địa chỉ, diện tích, giá cả từ nhiều định dạng khác nhau
-   Tạo features engineering cho machine learning

**3. Machine Learning**

-   Sử dụng Gradient Boosting để dự đoán giá
-   Liên tục cập nhật mô hình với dữ liệu mới
-   Đánh giá độ chính xác và tối ưu hóa

**4. Giao diện người dùng**

-   Website tương tác thân thiện
-   Nhập thông tin → Nhận dự đoán giá
-   Dashboard thống kê thị trường real-time

## 💡 Giá trị thực tiếng

**Cho người mua nhà:**

-   Biết được mức giá hợp lý trước khi đàm phán
-   Tránh bị "chặt chém" bởi các môi giới
-   So sánh giá giữa các khu vực

**Cho nhà đầu tư:**

-   Phát hiện cơ hội đầu tư tiềm năng
-   Theo dõi xu hướng giá theo thời gian
-   Đánh giá ROI của các khu vực

**Cho người bán:**

-   Định giá nhà đất một cách khách quan
-   Thời điểm tốt nhất để bán

## 🛠️ Công nghệ sử dụng

**Thu thập, Xử lý và Lưu trữ dữ liệu:**

-   **Apache Airflow**: Điều phối pipeline tự động
-   **Kafka**: Message queue cho real-time processing
-   **Apache Hadoop/HDFS**: Lưu trữ đáp ứng với lượng dữ liệu khổng lồ
-   **Apache Spark**: Xử lý dữ liệu quy mô lớn
-   **Playwright**: Crawler nhanh chóng, phù hợp với trang web động
-   **Gradient Boosting**: Thuật toán dự đoán chính

**Website & Deployment:**
-   **React.js**: Giao diện website modern
-   **Django**: Cung cấp api tương thích mạnh mẽ với các thư viện python
-   **Docker**: Containerization toàn bộ hệ thống

## 📈 Dữ liệu thu thập được

Mỗi bất động sản được lưu trữ với thông tin chi tiết:

-   **Vị trí**: Tỉnh/thành, quận/huyện, phường/xã
-   **Đặc điểm**: Diện tích, số phòng, hướng nhà
-   **Giá cả**: Giá rao bán, giá/m²
-   **Thời gian**: Ngày đăng, xu hướng giá
-   **Mô tả**: Chi tiết về tình trạng, nội thất

## 🎮 Trải nghiệm người dùng

1. **Nhập thông tin căn nhà** bạn quan tâm
2. **Nhận dự đoán giá** dựa trên AI trong vài giây
3. **Xem biểu đồ so sánh** với khu vực lân cận
4. **Theo dõi xu hướng** giá cả theo thời gian
5. **Đưa ra quyết định** mua/bán thông minh hơn

---

