import logging
import os
import sys


def setup_logging(name: str | None = None) -> logging.Logger:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    # bớt log ồn từ thư viện
    for noisy in ("urllib3", "elastic_transport", "py4j", "botocore", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(name or "sportslake")
