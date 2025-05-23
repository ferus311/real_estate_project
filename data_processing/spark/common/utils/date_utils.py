from datetime import datetime, timedelta
import os


def get_date_format(date_str=None, days_ago=0, format="%Y-%m-%d"):
    """
    Trả về chuỗi ngày theo định dạng yêu cầu

    Args:
        date_str (str, optional): Chuỗi ngày đầu vào. Mặc định là None.
        days_ago (int, optional): Số ngày trước hiện tại. Mặc định là 0.
        format (str, optional): Định dạng ngày. Mặc định là "%Y-%m-%d".

    Returns:
        str: Chuỗi ngày theo định dạng
    """
    if date_str:
        return datetime.strptime(date_str, format).strftime(format)
    else:
        return (datetime.now() - timedelta(days=days_ago)).strftime(format)


def get_hdfs_path(
    base_path,
    data_source,
    property_type,
    date=None,
    days_ago=0,
    partition_format="%Y/%m/%d",
):
    """
    Trả về đường dẫn HDFS đầy đủ với phân vùng theo ngày

    Args:
        base_path (str): Đường dẫn cơ sở trên HDFS
        data_source (str): Nguồn dữ liệu (batdongsan, chotot,...)
        property_type (str): Loại bất động sản (house, other,...)
        date (str, optional): Ngày cụ thể. Mặc định là None.
        days_ago (int, optional): Số ngày trước hiện tại. Mặc định là 0.
        partition_format (str, optional): Định dạng phân vùng ngày. Mặc định là "%Y/%m/%d".

    Returns:
        str: Đường dẫn HDFS đầy đủ
    """
    if date:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
    else:
        date_obj = datetime.now() - timedelta(days=days_ago)

    partition = date_obj.strftime(partition_format)
    return os.path.join(base_path, data_source, property_type, partition)


def generate_processing_id(job_name):
    """
    Tạo ID xử lý duy nhất cho batch

    Args:
        job_name (str): Tên job xử lý

    Returns:
        str: ID xử lý duy nhất
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{job_name}_{timestamp}"
