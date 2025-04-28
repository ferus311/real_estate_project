# Base image: Python 3.10 slim
FROM python:3.10-slim

# Cài đặt các gói hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    wget curl gnupg ca-certificates fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 \
    libnspr4 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libxss1 libxtst6 \
    libglib2.0-0 libu2f-udev libvulkan1 xdg-utils libdrm2 \
    libexpat1 libxfixes3 libgbm1 libxkbcommon0 libpango-1.0-0 libcairo2 \
    libfontconfig1 libxrender1 libxext6 libx11-6 libxcb1 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt Playwright và trình duyệt Chromium
RUN pip install --no-cache-dir playwright pandas pyarrow hdfs && \
    playwright install chromium

# Set working directory
WORKDIR /app

# Copy toàn bộ mã nguồn vào container
COPY . .

# Lệnh khởi chạy container
# CMD ["python", "crawl_realestate.py"]
