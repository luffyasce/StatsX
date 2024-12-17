import pandas as pd
import numpy as np
from datetime import datetime
from model.live.live_data_source.source import MDO
from model.tool.technicals import technical_indicators as ti
from infra.tool.rules import TradeRules
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import RedisMsg
from utils.tool.logger import log

logger = log(__file__, 'model')

# CONFIG
OPT_CAP_LIMIT = 1e4
FUT_CAP_LIMIT = 1e6
IR_LIMIT = 0.1


class OrderTracking:
    def __init__(self):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')

        self.hist = None

        self.rule = TradeRules()

        self.__del_hist_records__()

    def __del_hist_records__(self, record_date_max: int = 90):
        if "filtered_orders_DIY" not in self.origin.get_table_names("origin_future_cn_model_data"):
            return

        rec_dts = self.origin.read_dataframe(
            sql_str="select DISTINCT(`trading_date`) from origin_future_cn_model_data.filtered_orders_DIY "
                    "order by `trading_date` DESC"
        )
        if len(rec_dts) < record_date_max:
            return
        else:
            earliest_dt = rec_dts['trading_date'].iloc[record_date_max - 1]
            self.origin.del_row(
                db_name="origin_future_cn_model_data",
                tb_name="filtered_orders_DIY",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    def save_result(self, result: pd.DataFrame):
        self.origin.insert_dataframe(
            result,
            "origin_future_cn_model_data",
            "filtered_orders_DIY",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def filter_orders(self):
        for md in self.live_source.md_snapshot():
            if self.hist is None:
                self.hist = md
                continue
            if self.rule.api_exit_signal(datetime.now()):
                logger.warning("Tracker exited.")
                break
            sample_df = pd.concat([md, self.hist], axis=0).drop_duplicates(subset=['contract', 'datetime'])
            cnt_s = sample_df.groupby('contract')['datetime'].count()
            ts = cnt_s[cnt_s > 1].index.tolist()
            sample_df = sample_df[sample_df['contract'].isin(ts)].copy()

            result = pd.DataFrame()
            for contract, v_df in sample_df.groupby('contract'):
                v_df.sort_values(by='datetime', ascending=True, inplace=True)
                CAP_LIMIT = FUT_CAP_LIMIT if '-' not in contract else OPT_CAP_LIMIT
                orders = ti.filter_orders(
                    v_df.iloc[0]['volume'], v_df.iloc[1]['volume'],
                    v_df.iloc[0]['open_interest'], v_df.iloc[1]['open_interest'],
                    v_df.iloc[1]['last'], v_df.iloc[0]['last'],
                    v_df.iloc[1]['multiplier'],
                    cap_threshold=CAP_LIMIT, ir_threshold=IR_LIMIT
                )
                if orders:
                    res_i = pd.DataFrame.from_dict(orders, orient='index').T.assign(
                        contract=contract, datetime=v_df.iloc[1]['datetime'],
                        last=v_df.iloc[1]['last'],
                        trading_date=v_df.iloc[1]['trading_date']
                    )
                    msg = f"Order {contract}: ΔCAP {round(orders['money_delta'], 2)} | " \
                          f"IR {round(orders['information_ratio'], 2)} @ " \
                          f"{v_df.iloc[1]['datetime'].strftime('%Y-%m-%d %H:%M:%S.%f')}"
                    print(msg)
                    result = pd.concat([result, res_i], axis=0)
            self.save_result(result)
            self.hist = md


if __name__ == "__main__":
    o = OrderTracking()
    o.filter_orders()