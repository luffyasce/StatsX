import os
import configparser
from datetime import datetime, timedelta


class Config:
    def __init__(self):
        _fp = os.path.dirname(__file__)
        self.path = os.path.realpath(os.path.join(_fp,  os.sep.join(['..', '..'])))

    @property
    def get_conf(self):
        _conf_path = os.path.join(self.path, 'config.ini')
        config = configparser.ConfigParser()
        config.read(_conf_path)
        return config

    @property
    def get_private_conf(self):
        _conf_path = os.path.join(self.path, 'private_config.ini')
        config = configparser.ConfigParser()
        config.read(_conf_path)
        return config

    @property
    def get_trade_conf(self):
        _conf_path = os.path.join(self.path, 'trade.ini')
        config = configparser.ConfigParser()
        config.read(_conf_path)
        return config

    @property
    def project_max_history(self):
        """the max history period to trace back history data"""
        return 255

    @property
    def exchange_list(self):
        return ['SHFE', 'DCE', 'CZCE', 'GFEX']

    @property
    def project_start_date(self):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-self.project_max_history)

