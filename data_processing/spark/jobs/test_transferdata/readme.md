Nếu bạn muốn **lấy nhiều file `.parquet` từ HDFS về máy local** bằng cách sử dụng Docker (cụ thể là `hdfs dfs -get` + `docker cp`), bạn có thể làm theo **2 bước đơn giản sau**:

---

## ✅ **Bước 1: Vào container `namenode` và dùng `hdfs dfs -get` tải nhiều file về thư mục tạm**

```bash
docker exec -it namenode bash
```

Trong terminal container:

```bash
# Tạo thư mục tạm để chứa file
mkdir -p /tmp/exported

# Copy tất cả file parquet từ HDFS vào thư mục này
hdfs dfs -get /data/realestate/chotot/*.parquet /tmp/exported/
```

> Khi xong, thư mục `/tmp/exported/` sẽ chứa tất cả file `.parquet` bạn cần.

---

## ✅ **Bước 2: Từ máy local, dùng `docker cp` để copy thư mục về**

Từ terminal **trên máy bạn (host)**:

```bash
docker cp namenode:/tmp/exported ./parquet_files
```

→ Kết quả: Tất cả file `.parquet` từ HDFS được lưu vào thư mục `./parquet_files` trên máy bạn.

---

## ✅ Tóm tắt nhanh

| Việc cần làm       | Lệnh                                                |
| ------------------ | --------------------------------------------------- |
| Mở shell container | `docker exec -it namenode bash`                     |
| Lấy file từ HDFS   | `hdfs dfs -get /hdfs/path/*.parquet /tmp/exported/` |
| Copy về local      | `docker cp namenode:/tmp/exported ./parquet_files`  |

---

## 🧠 Mẹo thêm

* Nếu bạn cần dùng thường xuyên, hãy mount sẵn volume từ `namenode` ra local, như:

```yaml
namenode:
  volumes:
    - ./parquet_out:/output
```

→ Sau đó trong container: `hdfs dfs -get ... /output`

---

Bạn muốn mình tạo một script `.sh` tự động hóa cả quá trình này không?
