FROM bitnami/spark:latest

USER root

# Cài đặt các thư viện Python cần thiết
RUN apt-get update && apt-get install -y python3-pip
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

# Cài đặt các công cụ để giúp debug và monitoring
RUN apt-get install -y procps net-tools iputils-ping wget

# Tạo thư mục cho ứng dụng và logs
WORKDIR /app
RUN mkdir -p /app/logs


# Tạo biến môi trường cho cấu hình
ENV PYTHONPATH=/app:${PYTHONPATH}
ENV SPARK_LOG_DIR=/app/logs
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

# Tải và cài đặt Delta Lake JAR
ENV DELTA_CORE_VERSION=2.1.0
RUN mkdir -p /opt/bitnami/spark/jars/
RUN wget -q -O /opt/bitnami/spark/jars/delta-core_2.12-${DELTA_CORE_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_CORE_VERSION}/delta-core_2.12-${DELTA_CORE_VERSION}.jar

# Sao chép mã nguồn Spark processing
COPY ./common /app/common
COPY ./jobs /app/jobs
COPY ./pipelines /app/pipelines

# Script wrapper để xử lý các tham số dòng lệnh và làm entrypoint chuyên nghiệp hơn
COPY ./entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Quay trở lại user không phải root để tăng tính bảo mật
USER root
