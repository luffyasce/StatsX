"""
To initialize data api
"""
import sys
import os
from threading import Lock, Semaphore
from utils.tool.configer import Config
from utils.tool.logger import log

logger = log(__file__, "infra")


class DataApi:
    def __init__(self):
        pth = os.path.dirname(__file__)
        sys.path.append(pth)
        self.conf = Config().get_conf


class THS(DataApi):
    def __init__(self):
        super().__init__()
        self.api_name = "THS"
        try:
            import iFinDPy as ifdpy
        except ModuleNotFoundError:
            logger.error(f"{self.api_name} api not exists.")
        else:
            self.mod = ifdpy

    def __enter__(self):
        sem = Semaphore(5)  # 此变量用于控制最大并发数
        dllock = Lock()  # 此变量用来控制实时行情推送中落数据到本地的锁

        ths_user, ths_key = self.conf.get("THS", "acc"), self.conf.get("THS", "key")

        thsLogin = self.mod.THS_iFinDLogin(ths_user, ths_key)
        if thsLogin == 0:
            logger.info("Login succeeded.")
        else:
            logger.warning("Login failed.")
        return self.mod

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"{self.api_name} exited: {exc_type} || {exc_val} || {exc_tb}")
        self.mod.THS_iFinDLogout()


class RQ(DataApi):
    def __init__(self):
        super().__init__()
        self.api_name = "RQ"
        try:
            import rqdatac as rq
        except ModuleNotFoundError:
            logger.error(f"{self.api_name} api not exists.")
        else:
            self.mod = rq
        self.status = None

    def __enter__(self):
        _user, _key = self.conf.get("RQ", "acc"), self.conf.get("RQ", "key")
        try:
            quota = self.mod.user.get_quota()
        except Exception as err:
            if 'not initialized' in err.args[0]:
                self.mod.init(_user, _key)
            else:
                logger.error(err.args[0])
        else:
            used_ = quota['bytes_used']
            limit_ = quota['bytes_limit']
            remain_days = quota['remaining_days']
            if limit_ - used_ > 0 and remain_days > 0:
                pass
            else:
                logger.error(
                    f"RQ API insufficient quota. remaining bytes: {limit_ - used_} remaining days: {remain_days}"
                )
            self.status = (limit_ - used_, remain_days)
        return self.mod

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"{self.api_name} exited: {exc_type} || {exc_val} || {exc_tb} || status: {self.status}")


class BaoStock(DataApi):
    def __init__(self):
        super().__init__()
        self.api_name = "BS"
        try:
            import baostock as bs
        except ModuleNotFoundError:
            logger.error(f"{self.api_name} api not exists.")
        else:
            self.mod = bs
        self.status = None

    def __enter__(self):
        lg = self.mod.login()
        logger.info(f"baostock api: {lg.error_msg}")
        return self.mod

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"{self.api_name} exited: {exc_type} || {exc_val} || {exc_tb}")
        self.mod.logout()
