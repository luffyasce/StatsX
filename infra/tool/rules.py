"""
Naming conventions and rules for each exchange.
"""
import json
import os
from time import sleep
from datetime import datetime, time, timedelta
from utils.tool.configer import Config
from utils.database.unified_db_control import UnifiedControl


class TradeRules:
    def __init__(self):
        _trade_settings = os.path.join(Config().path, "trade_settings.json")
        with open(_trade_settings, mode='r+', encoding='utf8') as f:
            self.settings = json.load(f)

        self.db = UnifiedControl(db_type='base')

        self.exchange_list = ['SHFE', 'DCE', 'CZCE', 'GFEX']

    @property
    def targets(self):
        ls = []
        for e in self.exchange_list:
            r = self.db.read_dataframe(
                "processed_option_cn_meta_data",
                f"spec_info_{e}"
            )
            rls = r['symbol'].tolist()
            ls += rls
        return ls

    @classmethod
    def standard_contract_to_trade_code(cls, contract: str, exchange: str):
        if len(contract) <= 6:
            if exchange.lower() in ['shfe', 'ine', 'dce']:
                return contract.lower()
            elif exchange.lower() == 'cffex':
                return contract.upper()
            elif exchange.lower() == 'czce':
                return contract[:-4].upper() + contract[-3:]
            elif exchange.lower() == 'gfex':
                return contract.lower()
            else:
                raise ValueError(f"Please check your exchange input, got {exchange}. ")
        else:
            if exchange.lower() in ['shfe', 'ine']:
                c_ = contract.split('-')
                return f"{c_[0].lower()}{c_[1].upper()}{c_[2]}"
            elif exchange.lower() == 'dce':
                c_ = contract.split('-')
                return f"{c_[0].lower()}-{c_[1].upper()}-{c_[2]}"
            elif exchange.lower() == 'cffex':
                c_ = contract.split('-')
                return f"{c_[0].upper()}-{c_[1].upper()}-{c_[2]}"
            elif exchange.lower() == 'czce':
                c_ = contract.split('-')
                return f"{c_[0][:-4].upper()}{c_[0][-3:]}{c_[1].upper()}{c_[2]}"
            elif exchange.lower() == 'gfex':
                c_ = contract.split('-')
                return f"{c_[0].lower()}-{c_[1].upper()}-{c_[2]}"
            else:
                raise ValueError(f"Please check your exchange input, got {exchange}. ")

    @classmethod
    def request_id_generator(cls):
        sleep(0.3)
        return int(datetime.now().timestamp())
    
    def is_trading(self, e_type: str, t: datetime = datetime.now()):
        nts = self.settings.get("non_tradable_time").get(e_type)
        for time_range in nts:
            if time.fromisoformat(min(time_range)) <= t.time() <= time.fromisoformat(max(time_range)):
                return False
        return True

    def api_exit_signal(self, t: datetime, timeout: int = 120):
        """
        t: default datetime.now(), current trade time
        timeout: max seconds waited after trading session ends.
        return: True if process should exit
        """
        nts = self.settings.get("non_tradable_time").get('generalDerivatives')
        for time_range in nts:
            t_min = time.fromisoformat(min(time_range))
            t_max = time.fromisoformat(max(time_range))
            if t_min <= t.time() <=t_max :
                td = (t - t.replace(hour=t_min.hour, minute=t_min.minute, second=t_min.second)).seconds
                if td >= timeout:
                    return True
                else:
                    return False
        return False
