import logging


DEFAULT_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level.upper(), format=DEFAULT_FORMAT)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
