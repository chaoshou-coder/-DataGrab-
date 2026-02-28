import logging


DEFAULT_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
ALLOWED_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}


def configure_logging(level: str = "INFO") -> None:
    normalized = (level or "INFO").strip().upper()
    if normalized not in ALLOWED_LEVELS:
        raise ValueError(f"unsupported log level: {level}")
    logging.basicConfig(level=normalized, format=DEFAULT_FORMAT)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
