import pandas as pd
import numpy as np
from datetime import datetime
from model.live.live_data_source.source import MDO
from model.tool.technicals import technical_indicators as ti
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from utils.buffer.redis_handle import RedisMsg
from utils.tool.logger import log

logger = log(__file__, 'model')


class OrderArchive:
    """
    tick订单拆分归档，区分并分析买开/卖平，卖开/买平
    基于volume和oi均为单向计数
    """
    def __init__(self):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')
        self.rds = Redis()

        self.hist = None

        self.ls_dict = {
            1: 'long',
            0: 'neut',
            -1: 'short'
        }
        self.oi_dict = {
            1: 'open',
            0: 'swap',
            -1: 'close'
        }

        self.__del_hist_archive__()

    def __del_hist_archive__(self, record_date_max: int = 5):
        if "archived_orders_DIY" not in self.origin.get_table_names("origin_future_cn_model_data"):
            return
        rec_dts = self.origin.read_dataframe(
            sql_str="select DISTINCT(`trading_date`) from origin_future_cn_model_data.archived_orders_DIY "
                    "order by `trading_date` DESC"
        )
        if len(rec_dts) < record_date_max:
            return
        else:
            earliest_dt = rec_dts['trading_date'].iloc[record_date_max - 1]
            self.origin.del_row(
                db_name="origin_future_cn_model_data",
                tb_name="archived_orders_DIY",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    def save_result(self, result: pd.DataFrame):
        self.origin.insert_dataframe(
            result,
            "origin_future_cn_model_data",
            "archived_orders_DIY",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def read_dataframe_from_rds(self, k):
        return self.rds.decode_dataframe(self.rds.get_key(db=1, k=k, decode=False))

    def save_dataframe_to_rds(self, k: str, v: pd.DataFrame):
        self.rds.set_key(db=1, k=k, v=self.rds.encode_dataframe(v))

    def archive_orders(self):
        for md in self.live_source.md_snapshot():
            underlying_md = md[~md['contract'].str.contains('-')].copy()
            und_performance = ((underlying_md['last'] / underlying_md['open']) - 1) * 100
            bins = [-float('inf'), -3, -2, -1, 0, 1, 2, 3, float('inf')]
            labels = ['<=-3%', '(-3%, -2%]', '(-2%, -1%]', '(-1%, 0%]', '(0%, 1%)', '[1%, 2%)', '[2%, 3%)', '>=3%']
            categorized_data = pd.cut(und_performance, bins=bins, labels=labels)
            res_cnt = categorized_data.value_counts(sort=False)
            res_cnt = pd.DataFrame(res_cnt.rename('cnt'))
            self.save_dataframe_to_rds(k='today_pnl_distribute', v=res_cnt)

            if self.hist is None:
                self.hist = md
                continue
            sample_df = pd.concat([md, self.hist], axis=0).drop_duplicates(subset=['contract', 'datetime'])
            cnt_s = sample_df.groupby('contract')['datetime'].count()
            ts = cnt_s[cnt_s > 1].index.tolist()
            sample_df = sample_df[sample_df['contract'].isin(ts)].copy()

            result = pd.DataFrame()
            for contract, v_df in sample_df.groupby('contract'):
                v_df.set_index('datetime', inplace=True)
                prev_s = v_df.loc[v_df.index.min()]
                curr_s = v_df.loc[v_df.index.max()]
                vol_delta = curr_s['volume'] - prev_s['volume']
                oi_delta = curr_s['open_interest'] - prev_s['open_interest']
                if vol_delta < 0:
                    continue
                vol_delta = abs(oi_delta) if vol_delta < abs(oi_delta) else vol_delta
                p_delta = curr_s['last'] - prev_s['last']
                ls_initiator = self.ls_dict.get(np.sign(p_delta))
                oi_initiator = self.oi_dict.get(np.sign(oi_delta))
                open_vol = abs(oi_delta)
                swap_vol = vol_delta - abs(oi_delta)

                res_i = pd.DataFrame.from_dict(
                    {
                        "contract": contract,
                        "datetime": v_df.index.max(),
                        "trading_date": v_df.iloc[0]['trading_date'],
                        "act_vol": open_vol,
                        "swap_vol": swap_vol,
                        "indicator": f"{ls_initiator}-{oi_initiator}"
                    },
                    orient='index'
                ).T

                msg = f"Order {contract}: {ls_initiator} {oi_initiator} {open_vol} + swap {swap_vol}" \
                      f" @ {v_df.index.max()}"
                print(msg)
                result = pd.concat([result, res_i], axis=0)
            self.save_result(result)
            self.hist = md

    def get_archived_order_data(self, contract: str):
        cols = [f"{ls}-{oc}" for ls in self.ls_dict.values() for oc in self.oi_dict.values()]
        sorted_cols = sorted(cols)
        df = self.live_source.archived_orders(self.live_source.min_trade_date(), contract)
        if df.empty:
            return
        adf = df[['act_vol', 'indicator', 'datetime']].copy()
        sdf = df[['swap_vol', 'indicator', 'datetime']].copy()
        adf = adf.pivot(index='datetime', columns='indicator', values='act_vol').apply(pd.to_numeric, errors='coerce').fillna(0)
        adf['datetime_minute'] = adf.index.to_series().dt.ceil('1min')
        adf = adf.groupby('datetime_minute').sum().cumsum().sort_index(ascending=True)
        for x in [i for i in cols if i not in adf.columns]:
            adf[x] = 0
        sdf = sdf.pivot(index='datetime', columns='indicator', values='swap_vol').apply(pd.to_numeric, errors='coerce').fillna(0)
        sdf['datetime_minute'] = sdf.index.to_series().dt.ceil('1min')
        sdf = sdf.groupby('datetime_minute').sum().cumsum().sort_index(ascending=True)
        for x in [i for i in cols if i not in sdf.columns]:
            sdf[x] = 0
        return adf[sorted_cols].reset_index(drop=False), sdf[sorted_cols].reset_index(drop=False)


if __name__ == "__main__":
    oa = OrderArchive()
    oa.archive_orders()