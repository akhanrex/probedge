import logging
import sys

# Simple project-wide logger utility.
# Any module can do: from probedge.infra.logger import get_logger

def get_logger(name: str = "probedge") -> logging.Logger:
    logger = logging.getLogger(name)

    # Only add handler once to avoid duplicate logs
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    # Default level – you can tweak to DEBUG if needed
    logger.setLevel(logging.INFO)
    return logger
