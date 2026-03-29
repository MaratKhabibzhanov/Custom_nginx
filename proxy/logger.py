import logging
import sys

from proxy.config import config


level_dict = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

logger = logging.getLogger("proxy_logger")
logger.setLevel(level_dict[config.LOG_LEVEL])


console_handler = logging.StreamHandler(stream=sys.stdout)
file_handler = logging.FileHandler("proxy_logger.log", mode='w')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


warn_logger = logging.getLogger("warn_proxy_logger")
warn_logger.setLevel(level_dict[config.LOG_LEVEL])

warn_file_handler = logging.FileHandler("warn_proxy_logger.log", mode='w')

warn_file_handler.setFormatter(formatter)

warn_logger.addHandler(warn_file_handler)
warn_logger.addHandler(console_handler)
