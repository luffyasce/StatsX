from datetime import datetime
from time import sleep
import pandas as pd
import numpy as np
from infra.trade.api.ctp_trade import *
from utils.tool.logger import log
from utils.database.unified_db_control import UnifiedControl
from utils.tool.configer import Config
from utils.tool.beep import Beep

config = Config()
trade_conf = config.get_trade_conf

logger = log(__file__, "infra", warning_only=False)

rules = rules.TradeRules()


class AccStats:
    def __init__(self, trader: CtpTrade):
        self.trader = trader
        self.origin = UnifiedControl(db_type='origin')

        self.hist_rec = pd.DataFrame()

    def query_account(self):
        req_id = self.trader.query_account()
        while req_id != self.trader.trade_spi.account_detail.get("nRequestID", 0):
            sleep(0.01)
        return self.trader.trade_spi.account_detail

    def query_position(self, ctp_contract: str = None):
        req_id = self.trader.query_position(ctp_contract)
        while not self.trader.trade_spi.req_position_record.get(req_id, False):
            sleep(0.01)
        return pd.DataFrame(self.trader.trade_spi.req_position_detail)

    def get_trades(self):
        cur_rec = pd.DataFrame.from_dict(self.trader.trade_spi.trade_rtn_detail, orient='index')
        if self.hist_rec.empty:
            new_rec = cur_rec
        else:
            new_rec = cur_rec[~cur_rec.set_index(['OrderSysID', 'ExchangeID']).index.isin(self.hist_rec.set_index(['OrderSysID', 'ExchangeID']).index)]
        self.hist_rec = cur_rec
        return new_rec.assign(RecDatetime=datetime.now()) if not new_rec.empty else new_rec

    def save_trades(self, df: pd.DataFrame):
        self.origin.insert_dataframe(
            df, "origin_future_cn_trade_data", "trade_record_DIY",
            set_index=['BrokerID', 'InvestorID', 'InstrumentID', 'OrderSysID', 'ExchangeID']
        )


def run_account_recorder(broker: str):
    trader = CtpTrade(
        front_addr=trade_conf.get(broker, "trade_front_addr"),
        broker_id=trade_conf.get(broker, "broker_id"),
        investor_id=trade_conf.get(broker, "investor_id"),
        pwd=trade_conf.get(broker, "pwd"),
        app_id=trade_conf.get(broker, "app_id"),
        auth_code=trade_conf.get(broker, "auth_code"),
        user_product_info=trade_conf.get(broker, "product_info"),
        mode=trade_conf.getint(broker, "login_mode"),
    )
    with trader as tr:
        acc_stat = AccStats(tr)
        while not rules.api_exit_signal(datetime.now()):
            trade_records = acc_stat.get_trades()
            if not trade_records.empty:
                acc_stat.save_trades(trade_records)
            print(
                f"\r{broker} trade records {len(acc_stat.hist_rec)} saving {len(trade_records)} -- {datetime.now()}.",
                end="", flush=True
            )
        else:
            print(
                f"\r{broker} trade records {len(trade_records)} -- "
                f"Conn Expired -- {datetime.now()} ",
                end="", flush=True
            )
