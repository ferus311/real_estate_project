FROM python:3.10-slim

# Cài đặt Chrome và ChromeDriver đã kiểm nghiệm (v124)
RUN apt-get update && apt-get install -y \
    wget unzip gnupg curl ca-certificates \
    fonts-liberation libappindicator3-1 libasound2 libnss3 \
    libx11-xcb1 libxss1 libxcomposite1 libxcursor1 libxdamage1 libxi6 libxtst6 \
    libglib2.0-0 libu2f-udev && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    wget -q -O /tmp/chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/135.0.7049.84/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/chromedriver-linux64/chromedriver && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip google-chrome-stable_current_amd64.deb /usr/local/bin/chromedriver-linux64

# Cài đặt thư viện Python
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir selenium hdfs

CMD ["python", "run.py"]
