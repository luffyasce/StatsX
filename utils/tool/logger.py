import os
import platform
import logging
from utils.tool.configer import Config


def log(filename, log_name, warning_only: bool = True):
    global logger
    conf = Config()
    log_path = os.path.join(conf.path, os.sep.join(['logs', f"{log_name}.txt"]))
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)
    try:
        filing = logging.FileHandler(filename=log_path)
    except FileNotFoundError:
        if platform.system().lower() == "windows":
            os.mkdir(r"\\".join(log_path.split('\\')[:-1]))
        else:
            os.mkdir("/".join(log_path.split('/')[:-1]))
        filing = logging.FileHandler(filename=log_path)
    else:
        pass
    filing.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    filing.setFormatter(formatter)
    logger.addHandler(filing)
    if not warning_only:
        level_s = logging.INFO
    else:
        level_s = logging.WARNING
    streaming = logging.StreamHandler()
    streaming.setLevel(level_s)
    streaming.setFormatter(formatter)
    logger.addHandler(streaming)
    return logger


