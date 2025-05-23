FROM jupyter/scipy-notebook:latest

LABEL maintainer="Real Estate Data Team"

USER root

# Cài đặt Java và các dependencies hệ thống
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    libsnappy-dev \
    libatlas-base-dev \
    graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Cài đặt các thư viện Python cần thiết
RUN pip install --no-cache-dir \
    pandas==2.1.1 \
    numpy==1.26.0 \
    matplotlib==3.8.0 \
    seaborn==0.13.0 \
    plotly==5.17.0 \
    folium==0.14.0 \
    scikit-learn==1.3.2 \
    xgboost==2.0.0 \
    lightgbm==4.1.0 \
    pyarrow==10.0.1 \
    snappy==3.1.1 \
    hdfs==2.7.0 \
    fastavro==1.8.3 \
    geopandas==0.14.0 \
    pyspark==3.4.1 \
    findspark==2.0.1 \
    jupyter_contrib_nbextensions==0.7.0 \
    notebook==6.5.0

# Tạo thư mục runtime và cấp quyền cho thư mục .local (sử dụng UID và GID 1000)
RUN mkdir -p /home/jovyan/.local/share/jupyter/runtime && \
    chown -R 1000:1000 /home/jovyan/.local

# Cài đặt Jupyter extensions
RUN jupyter contrib nbextension install --user && \
    jupyter nbextension enable toc2/main

# Cấu hình mặc định cho PySpark (nếu chạy local)
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8888 --ip=0.0.0.0 --allow-root'

# Thư mục làm việc
RUN mkdir -p /home/jovyan/work/data
WORKDIR /home/jovyan/work

# Mở cổng notebook
EXPOSE 8888

# Lệnh mặc định
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
