import pandas as pd
import numpy as np
from typing import Union
from datetime import datetime, time, timedelta, date
from utils.database.unified_db_control import UnifiedControl
from data.data_utils.data_standardization import logarithm_change
from model.tool.technicals.technical_indicators import historical_volitility, realized_volitility
from utils.tool.logger import log

logger = log(__file__, "data")

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ProcessFutureCnTradeData:
    def __init__(self):
        self.base = UnifiedControl(db_type='base')

    def process_future_net_position_data(self, exchange: str, by: str):
        last_update = self.base.read_dataframe(
            "processed_future_cn_trade_data",
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
            "pretreated_future_cn_trade_data",
            f"position_rank_by_{by}_{exchange}",
            filter_datetime=filt_
        )
        if not df.empty:
            for t, v in df.groupby('trading_date'):
                for c, vx in v.groupby(by):
                    vx = vx.replace(to_replace='nan', value=np.nan)
                    ll = vx[['broker_long', 'long', 'long_chg']].copy().dropna(subset=['broker_long']).set_index('broker_long')
                    ll.drop_duplicates(inplace=True)
                    ss = vx[['broker_short', 'short', 'short_chg']].copy().dropna(subset=['broker_short']).set_index('broker_short')
                    ss.drop_duplicates(inplace=True)

                    df_c = pd.concat([ll, ss], axis=1).fillna(0)

                    df_c = df_c.assign(
                        net_pos=df_c['long'] - df_c['short'],
                        net_chg=df_c['long_chg'] - df_c['short_chg']
                    ).drop(
                        columns=['long', 'long_chg', 'short', 'short_chg']
                    ).reset_index(drop=False, names=['broker']).assign(
                        trading_date=t
                    )
                    df_c[by] = c
                    yield df_c

    def save_future_net_position_data(self, df: pd.DataFrame, exchange: str, by: str):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_trade_data",
            f"net_position_by_{by}_{exchange}",
            set_index=["trading_date", "broker", by]
        )

    def process_future_net_position_by_symbol(self, exchange: str):
        if exchange == 'CZCE':
            for r in self.process_future_net_position_data(exchange, "symbol"):
                yield r
        else:
            last_update = self.base.read_dataframe(
                "processed_future_cn_trade_data",
                f"net_position_by_symbol_{exchange}",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            if last_update.empty:
                last_date = None
            else:
                last_date = last_update['trading_date'][0]

            filt_ = None if last_date is None else {"trading_date": {'gte': last_date.strftime("%Y-%m-%d")}}

            df = self.base.read_dataframe(
                "pretreated_future_cn_trade_data",
                f"position_rank_by_contract_{exchange}",
                filter_datetime=filt_
            )
            if not df.empty:
                for t, v in df.groupby('trading_date'):
                    v['symbol'] = v['contract'].apply(lambda x: x[:-4])
                    v = v.replace(to_replace='nan', value=np.nan)
                    for s, vx in v.groupby("symbol"):
                        ll = vx[['broker_long', 'long', 'long_chg']].copy().dropna(
                            subset=['broker_long']
                        ).groupby('broker_long')[['long', 'long_chg']].sum()
                        ss = vx[['broker_short', 'short', 'short_chg']].copy().dropna(
                            subset=['broker_short']
                        ).groupby('broker_short')[['short', 'short_chg']].sum()
                        df_c = pd.concat([ll, ss], axis=1).fillna(0)
                        df_c = df_c.assign(
                            net_pos=df_c['long'] - df_c['short'],
                            net_chg=df_c['long_chg'] - df_c['short_chg']
                        ).drop(
                            columns=['long', 'long_chg', 'short', 'short_chg']
                        ).reset_index(drop=False, names=['broker']).assign(
                            trading_date=t
                        )
                        df_c['symbol'] = s
                        yield df_c

    def process_historical_volatility(self, exchange: str):
        hv_period_ls = [20, 40, 60]
        last_update = self.base.read_dataframe(
            "processed_future_cn_trade_data",
            f"historical_volatility_{exchange}",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        if last_update.empty:
            last_date = None
            start_date = None
        else:
            last_date = last_update['trading_date'][0]
            start_date = last_date + timedelta(days=-max(hv_period_ls))

        filt_ = None if start_date is None else {"trading_date": {'gte': start_date.strftime("%Y-%m-%d")}}

        df = self.base.read_dataframe(
            "pretreated_future_cn_md_data",
            f"all_1d_{exchange}",
            filter_datetime=filt_
        )
        if not df.empty:
            df = df.set_index('trading_date').sort_index(ascending=True)
            for c, v in df.groupby('contract'):
                ret_v = logarithm_change(v, "close")
                res_df = pd.DataFrame()
                for p in hv_period_ls:
                    if len(ret_v) < p:
                        continue
                    hv = ret_v.rolling(p, min_periods=p).apply(historical_volitility)
                    rv = ret_v.rolling(p, min_periods=p).apply(realized_volitility)
                    res = pd.concat([hv.rename(f"hv{p}"), rv.rename(f"rv{p}")], axis=1)
                    res = res if last_date is None else res[res.index >= last_date].copy()
                    res = res.dropna(axis=0, how='all')
                    res_df = pd.concat([res_df, res], axis=1)
                res_df = res_df.assign(
                    contract=c,
                    symbol=c[:-4]
                ).reset_index(drop=False, names=['trading_date'])
                if res_df.empty:
                    continue
                yield res_df

    def save_historical_volatility_data(self, df: pd.DataFrame, exchange: str):
        self.base.insert_dataframe(
            df, "processed_future_cn_trade_data", f"historical_volatility_{exchange}",
            set_index=['trading_date', 'contract', 'symbol'], partition=['trading_date']
        )



if __name__ == "__main__":
    ptr = ProcessFutureCnTradeData()
    for e in ['GFEX']:
        for r in ptr.process_future_net_position_data(e, "contract"):
            ptr.save_future_net_position_data(r, e, "contract")
        for r in ptr.process_future_net_position_by_symbol(e):
            ptr.save_future_net_position_data(r, e, "symbol")
        for r in ptr.process_historical_volatility(e):
            ptr.save_historical_volatility_data(r, e)
