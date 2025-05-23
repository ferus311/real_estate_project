"""
Tiện ích logging và ghi nhật ký
"""

import logging
from datetime import datetime


def setup_logger(name, log_level=logging.INFO):
    """
    Cấu hình logger

    Args:
        name (str): Tên của logger
        log_level (int, optional): Mức độ log. Mặc định là logging.INFO.

    Returns:
        Logger: Logger đã được cấu hình
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Kiểm tra nếu logger đã có handler
    if not logger.handlers:
        # Tạo console handler
        ch = logging.StreamHandler()
        ch.setLevel(log_level)

        # Tạo formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch.setFormatter(formatter)

        # Thêm handler vào logger
        logger.addHandler(ch)

    return logger


class SparkJobLogger:
    """
    Logger cho các Spark job với tính năng ghi nhật ký thực thi
    """

    def __init__(self, job_name):
        """
        Khởi tạo SparkJobLogger

        Args:
            job_name (str): Tên của job
        """
        self.job_name = job_name
        self.logger = setup_logger(job_name)
        self.start_time = None
        self.metrics = {}

    def start_job(self, params=None):
        """
        Ghi log bắt đầu job

        Args:
            params (dict, optional): Tham số của job. Mặc định là None.
        """
        self.start_time = datetime.now()
        self.logger.info(f"Job {self.job_name} bắt đầu lúc {self.start_time}")
        if params:
            self.logger.info(f"Tham số: {params}")

    def end_job(self):
        """
        Ghi log kết thúc job
        """
        end_time = datetime.now()
        duration = (
            (end_time - self.start_time).total_seconds() if self.start_time else 0
        )
        self.logger.info(f"Job {self.job_name} kết thúc lúc {end_time}")
        self.logger.info(f"Thời gian thực thi: {duration:.2f} giây")

        if self.metrics:
            self.logger.info("Chỉ số thực thi:")
            for key, value in self.metrics.items():
                self.logger.info(f"  - {key}: {value}")

    def log_dataframe_info(self, df, stage_name=""):
        """
        Ghi log thông tin về DataFrame

        Args:
            df: DataFrame Spark
            stage_name (str, optional): Tên của giai đoạn. Mặc định là "".
        """
        prefix = f"{stage_name} - " if stage_name else ""
        count = df.count()
        self.logger.info(f"{prefix}Số lượng bản ghi: {count}")
        self.metrics[f"{prefix}record_count"] = count

        # Log schema
        self.logger.info(f"{prefix}Schema:")
        for field in df.schema.fields:
            self.logger.info(f"  - {field.name}: {field.dataType}")

    def log_error(self, error_message, exception=None):
        """
        Ghi log lỗi

        Args:
            error_message (str): Thông báo lỗi
            exception (Exception, optional): Exception. Mặc định là None.
        """
        if exception:
            self.logger.error(f"{error_message}: {str(exception)}")
        else:
            self.logger.error(error_message)
