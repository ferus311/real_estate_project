import os
import time
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional
import threading
from contextlib import contextmanager


class CheckpointManager:
    """
    SQLite checkpoint manager - Đơn giản và hiệu quả
    """

    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, source: str, base_path: str = "/app/checkpoint"):
        with cls._lock:
            key = f"{source}_{base_path}"
            if key not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[key] = instance
            return cls._instances[key]

    def __init__(self, source: str, base_path: str = "/app/checkpoint"):
        if hasattr(self, "_initialized"):
            return

        self.source = source
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

        self.db_path = os.path.join(base_path, f"{source}_checkpoint.db")
        self._local = threading.local()
        self._initialized = True

        self._init_database()

    def _init_database(self):
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                    identifier TEXT PRIMARY KEY,
                    success BOOLEAN NOT NULL DEFAULT 1,
                    timestamp REAL NOT NULL,
                    datetime TEXT NOT NULL
                )
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_timestamp
                ON checkpoints(timestamp)
            """
            )

    @contextmanager
    def _get_connection(self):
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(
                self.db_path, timeout=30.0, check_same_thread=False
            )
            # Simple optimizations
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.execute("PRAGMA synchronous=NORMAL")

        try:
            yield self._local.connection
        finally:
            self._local.connection.commit()

    def save_checkpoint(self, identifier: str, success: bool = True):
        """Lưu checkpoint"""
        current_time = time.time()
        current_datetime = datetime.now().isoformat()

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints
                (identifier, success, timestamp, datetime)
                VALUES (?, ?, ?, ?)
            """,
                (str(identifier), success, current_time, current_datetime),
            )

    def is_crawled(self, identifier: str) -> bool:
        """Kiểm tra xem item đã được crawl thành công chưa"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 1 FROM checkpoints
                WHERE identifier = ? AND success = 1
                LIMIT 1
            """,
                (str(identifier),),
            )

            return cursor.fetchone() is not None

    def should_force_crawl(
        self, identifier: str, force_crawl_interval_hours: float
    ) -> bool:
        """Kiểm tra có nên force crawl không"""
        if force_crawl_interval_hours <= 0:
            return False

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT timestamp FROM checkpoints
                WHERE identifier = ? AND success = 1
                LIMIT 1
            """,
                (str(identifier),),
            )

            row = cursor.fetchone()
            if not row:
                return True

            last_crawl_time = row[0]
            current_time = time.time()
            hours_since_last_crawl = (current_time - last_crawl_time) / 3600

            return hours_since_last_crawl >= force_crawl_interval_hours

    def get_stats(self) -> Dict[str, int]:
        """Thống kê checkpoint"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed_count
                FROM checkpoints
            """
            )

            row = cursor.fetchone()
            return {
                "total": row[0] if row else 0,
                "success": row[1] if row else 0,
                "failed": row[2] if row else 0,
            }


def get_checkpoint_manager(source: str) -> CheckpointManager:
    """Factory function để tạo CheckpointManager"""
    return CheckpointManager(source)
