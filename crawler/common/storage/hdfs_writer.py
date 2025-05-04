import os, pyarrow.parquet as pq, pyarrow as pa
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime

HDFS_URL = "http://namenode:9870"
HDFS_DIR = "/data/test/raw_data"
LOCAL_DIR = "./data_output"
hdfs_client = InsecureClient(HDFS_URL, user="airflow")

async def write_to_parquet(data, prefix="data"):
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    fname = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    local_file = os.path.join(LOCAL_DIR, fname)

    os.makedirs(LOCAL_DIR, exist_ok=True)
    pq.write_table(table, local_file)

    try:
        hdfs_path = f"{HDFS_DIR}/{fname}"
        hdfs_client.upload(hdfs_path, local_file, overwrite=True)
        print(f"✅ Uploaded to HDFS: {hdfs_path}")
    except Exception as e:
        print(f"❌ HDFS upload failed: {e}")
    finally:
        os.remove(local_file)

def log_error_to_hdfs(msg):
    print(f"[ERROR] {msg}")
