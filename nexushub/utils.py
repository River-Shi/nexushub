import sys
import time
from pathlib import Path
from typing import Optional
from loguru import logger
from datetime import timezone, datetime

def safe_timestamp(ts: int | float) -> datetime:
    ts = int(ts)
    # 判断时间戳的数量级
    if ts > 1e18:  # 纳秒
        ts = ts / 1e9
    elif ts > 1e15:  # 微秒
        ts = ts / 1e6
    elif ts > 1e12:  # 毫秒
        ts = ts / 1e3
    # else: 认为是秒
    return datetime.fromtimestamp(ts, tz=timezone.utc)

class LiveClock:
    def __init__(self):
        pass

    def timestamp(self):
        return time.time()

    def timestamp_ms(self):
        return time.time_ns() // 1_000_000

    def timestamp_ns(self):
        return time.time_ns()

    def utc_now(self):
        return datetime.now(timezone.utc)

    def iso_now(self, timespec="milliseconds"):
        return self.utc_now().isoformat(timespec=timespec).replace("+00:00", "Z")


class Log:
    _initialized = False

    @staticmethod
    def setup_logger(
        log_path: Optional[str] = None,
        log_level: str = "INFO",
        rotation: str = "20 MB",
        retention: str = "10 days",
    ):
        if Log._initialized:
            return

        logger.remove()

        # 添加标准输出处理器，使用带颜色的自定义格式
        logger.add(
            sink=sys.stdout,
            level=log_level,
            colorize=True,
            enqueue=True,
        )

        if log_path:
            log_dir = Path(log_path)
            log_dir.mkdir(parents=True, exist_ok=True)

            # 文件输出不需要颜色标签
            logger.add(
                sink=str(log_dir / "app.log"),
                level=log_level,
                rotation=rotation,
                retention=retention,
                compression="zip",
                enqueue=True,
                colorize=False,
            )

        Log._initialized = True

    @staticmethod
    def get_logger():
        """获取带有类名上下文的logger"""
        if not Log._initialized:
            Log.setup_logger()
        return logger


if __name__ == "__main__":
    logger = Log.get_logger()
    logger.info("Logger initialized.")
    logger.debug("This is a debug message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.success("This is a success message.")
    logger.critical("This is a critical message.")
