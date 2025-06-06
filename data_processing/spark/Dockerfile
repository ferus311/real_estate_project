FROM bitnami/spark:latest

USER root

# Cài đặt các thư viện Python cần thiết
RUN apt-get update && apt-get install -y python3-pip

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Tạo thư mục cho ứng dụng và logs
WORKDIR /app
RUN mkdir -p /app/logs


# Tạo biến môi trường cho cấu hình
ENV PYTHONPATH=/app:${PYTHONPATH}
ENV SPARK_LOG_DIR=/app/logs
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

# Sao chép mã nguồn Spark processing
COPY ./common /app/common
COPY ./jobs /app/jobs
COPY ./pipelines /app/pipelines
