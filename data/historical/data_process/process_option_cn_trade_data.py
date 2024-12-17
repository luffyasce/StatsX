from datetime import datetime
import re
import pandas as pd
import numpy as np
from typing import Union
import utils.database.unified_db_control as udc
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ProcessOptionCnTrade:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')
        conf = Config()
        self.exchange_ls = conf.exchange_list

    def process_option_net_position_data(self, exchange: str, by: str):
        last_update = self.base.read_dataframe(
            "processed_option_cn_trade_data",
            f"net_position_by_{by}_{exchange}",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        if last_update.empty:
            last_date = None
        else:
            last_date = last_update['trading_date'][0]

        filt_ = None if last_date is None else {"trading_date": {'gte': last_date.strftime("%Y-%m-%d")}}

        df = self.base.read_dataframe(
            "pretreated_option_cn_trade_data",
            f"position_rank_by_{by}_{exchange}",
            filter_datetime=filt_
        )
        if not df.empty:
            for t, v in df.groupby('trading_date'):
                for d, vd in v.groupby('direction'):
                    for c, vx in vd.groupby(by):
                        vx = vx.replace(to_replace='nan', value=np.nan)
                        ll = vx[['broker_long', 'long', 'long_chg']].copy().dropna(subset=['broker_long']).set_index(
                            'broker_long')
                        ss = vx[['broker_short', 'short', 'short_chg']].copy().dropna(subset=['broker_short']).set_index(
                            'broker_short')
                        df_c = pd.concat([ll, ss], axis=1).fillna(0)
                        df_c = df_c.assign(
                            net_pos=df_c['long'] - df_c['short'],
                            net_chg=df_c['long_chg'] - df_c['short_chg']
                        ).drop(
                            columns=['long', 'long_chg', 'short', 'short_chg']
                        ).reset_index(drop=False, names=['broker']).assign(
                            trading_date=t,
                            direction=d,
                        )
                        df_c[by] = c
                        yield df_c

    def save_option_net_position_data(self, df: pd.DataFrame, exchange: str, by: str):
        self.base.insert_dataframe(
            df,
            "processed_option_cn_trade_data",
            f"net_position_by_{by}_{exchange}",
            set_index=["trading_date", "broker", "direction", by]
        )


if __name__ == "__main__":
    pro = ProcessOptionCnTrade()
    for e in ['GFEX']:
        for s in ['symbol', 'contract']:
            for r in pro.process_option_net_position_data(e, s):
                pro.save_option_net_position_data(r, e, s)