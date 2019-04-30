from pkg_resources import Requirement, resource_string
import json
from typing import Optional
from logging import Logger
import logging.config
import multiprocessing_logging

__pkg__ = "pypedream"

def getLogger(log_name: str = "", log_path: Optional[str] = None) -> Logger:
    config_str = resource_string(Requirement.parse(__pkg__), f"{__pkg__}/logger-conf.json")
    config = json.loads(config_str)
    if log_path is not None:
        config["handlers"]["file"]["filename"] = log_path
    logging.config.dictConfig(config)
    logger = logging.getLogger(log_name)
    multiprocessing_logging.install_mp_handler(logger)
    return logger
