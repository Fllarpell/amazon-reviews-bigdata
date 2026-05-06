import logging
import sys

from config.constants import LOG_DATEFMT, LOG_FORMAT, LOG_LEVEL


def setup_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        stream=sys.stdout,
    )
