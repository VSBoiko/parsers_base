import logging

import settings


logging.basicConfig(
    level=logging.INFO,
    # filename=settings.LOG_FILE_PATH,
    # filemode="w",
    # format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d): %(message)s",
    format="%(asctime)s - [%(levelname)s]: %(message)s",
)

# logging.debug("This is a debug message")
# logging.info("This is an info message")
# logging.warning("This is a warning message")
# logging.error("This is an error message")
# logging.critical("This is a critical message")
