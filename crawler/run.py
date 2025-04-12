from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from hdfs import InsecureClient
import time
from datetime import datetime  # Import thêm datetime

# Cấu hình headless Chrome
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Trỏ đến chromedriver đã cài trong Dockerfile
service = Service("/usr/local/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)

# Crawl
url = "https://example.com"
driver.get(url)
time.sleep(2)
html = driver.page_source
driver.quit()

print("✅ Crawl thành công:", url)

# Ghi lên HDFS
client = InsecureClient("http://namenode:9870", user="hdfs")

# Lấy ngày giờ hiện tại và tạo đường dẫn thư mục
current_time = datetime.now().strftime("%Y-%m-%d/%H-%M-%S")  # Định dạng: YYYY-MM-DD/HH-MM-SS
hdfs_path = f"/raw/{current_time}/example.html"  # Đường dẫn lưu file

# Tạo file và ghi nội dung
with client.write(hdfs_path, encoding="utf-8", overwrite=True) as f:
    f.write(html)

print(f"✅ Ghi lên HDFS thành công: {hdfs_path}")
