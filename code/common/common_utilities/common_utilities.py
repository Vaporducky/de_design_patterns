import logging
import sys
import time


def setup_logging(name="SparkApp"):
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=f"[{name}] [%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
