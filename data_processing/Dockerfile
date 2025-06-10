FROM bitnami/spark:latest

USER root

# Cài đặt các thư viện Python cần thiết
RUN apt-get update && apt-get install -y python3-pip wget

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Download PostgreSQL JDBC driver
RUN wget -O /opt/bitnami/spark/jars/postgresql-42.7.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.0.jar

# Tạo thư mục cho ứng dụng và logs
WORKDIR /app
RUN mkdir -p /app/logs

# Tạo biến môi trường cho cấu hình
ENV PYTHONPATH=/app:${PYTHONPATH}
ENV SPARK_LOG_DIR=/app/logs
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

# Sao chép mã nguồn Spark processing
COPY ./spark/common /app/common
COPY ./spark/jobs /app/jobs
COPY ./spark/pipelines /app/pipelines
COPY ./ml /app/ml
