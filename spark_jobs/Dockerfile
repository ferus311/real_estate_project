FROM bitnami/spark:3.4.1

WORKDIR /app

COPY ./spark_jobs/ /app/

RUN pip install --no-cache-dir pandas

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/app/preview_hdfs_data.py"]
