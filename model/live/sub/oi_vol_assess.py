import os
import traceback
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.fft import fft, fftfreq
from datetime import datetime, timedelta
from model.live.live_data_source.source import MDO
from model.tool.technicals import technical_indicators as ti
from infra.tool.rules import TradeRules
from utils.database.unified_db_control import UnifiedControl
from data.historical.data_pretreat.pretreat_data_from_local import PretreatLocal
from utils.buffer.redis_handle import Redis
from utils.tool.logger import log
from utils.tool.configer import Config

logger = log(__file__, 'model')

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class OiVolAssess:
    def __init__(self, options_only: bool = True):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')
        self.base = UnifiedControl('base')
        self.rds = Redis()
        config = Config()
        self.exchange_list = config.exchange_list

        self.rule = TradeRules()

        self.opt_only = options_only
        self.targets = self.rule.targets

    @property
    def multiplier(self):
        res = pd.DataFrame()
        for e in self.rule.exchange_list:
            df = self.base.read_dataframe(
                "processed_future_cn_meta_data", f"spec_info_{e}"
            )
            m = df.set_index('symbol')[['trade_unit']]
            res = pd.concat([res, m], axis=0)
        return res['trade_unit']

    @property
    def prev_trading_date(self):
        t = self.base.read_dataframe(
            "processed_future_cn_meta_data",
            "hist_trading_date_DIY",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        if t.empty:
            return None
        else:
            tx = t['trading_date'].sort_values(ascending=False)[0]
            return tx

    @property
    def oi_tick_sample(self):
        prev_t = self.prev_trading_date.strftime('%Y-%m-%d') if self.prev_trading_date is not None else None
        filt_t = "" if prev_t is None else f"`trading_date` >= '{prev_t}' and "
        target_ls_str = '[' + ', '.join([f"'{i}'" for i in self.targets]) + ']'
        t = self.live_source.realtime_data.get_stored(
            sql_str=f"select DISTINCT `symbol`, `contract`, `open_interest`, `datetime_minute`, `volume`, `trading_date` "
                    f"from origin_future_cn_md_data.all_tick_CTP "
                    f"where {filt_t}"
                    f"`contract` NOT LIKE '%-%' "
                    f"and `symbol` in {target_ls_str} "
                    f"and `ask_vol1` > 0 and `bid_vol1` > 0 and `open` > 0 and `high` > 0 and `low` > 0 "
                    f"and `last` > 0 and `open_interest` > 0 and `volume` > 0"
        )
        t = t.groupby('contract').apply(
            lambda x: x.sort_values(by=['datetime_minute', 'volume'], ascending=True).drop_duplicates(subset=['datetime_minute'], keep='last')
        ).reset_index(drop=True)
        return t

    @property
    def option_oi_tick_sample(self):
        curr_t = self.live_source.this_trading_date.strftime('%Y-%m-%d')
        filt_t = f"`trading_date` == '{curr_t}' and "
        t = self.live_source.realtime_data.get_stored(
            sql_str=f"select DISTINCT `symbol`, `contract`, `open_interest`, `datetime_minute`, `volume`, `trading_date`, `pre_open_interest`, `average_price`, `exchange` "
                    f"from origin_future_cn_md_data.all_tick_CTP "
                    f"where {filt_t}"
                    f"`contract` LIKE '%-%' "
                    f"and `ask_vol1` > 0 and `bid_vol1` > 0 and `open` > 0 and `high` > 0 and `low` > 0 "
                    f"and `last` > 0 and `open_interest` > 0 and `volume` > 0"
        )
        t = t.groupby('contract').apply(
            lambda x: x.sort_values(by=['datetime_minute', 'volume'], ascending=True).drop_duplicates(
                subset=['contract'], keep='last')
        ).reset_index(drop=True)
        return t

    # @staticmethod
    # def process_volume_data(df: pd.DataFrame):
    #     df = df.sort_values(by='datetime_minute', ascending=True)
    #     df['vol_'] = df.groupby(['contract', 'trading_date'])['volume'].diff()
    #     df['volume'] = np.where(
    #         np.isnan(df['vol_']), df['volume'], df['vol_']
    #     )
    #     df.drop(columns=['vol_'], inplace=True)
    #     return df

    # @staticmethod
    # def calc_oi_neg_chg_pctg(df: pd.DataFrame):
    #     df = df.sort_values(by='datetime_minute', ascending=True)
    #     df['oi_chg'] = df.groupby('contract')['open_interest'].diff().fillna(0)
    #     df['oi_chg'] = np.where(df['oi_chg'] < 0, df['oi_chg'].abs(), 0)
    #     rdf = df.groupby('contract')[['oi_chg']].sum()
    #     cdf = df.groupby('contract').apply(
    #         lambda x: x[x['datetime_minute'] == x['datetime_minute'].max()].iloc[0]['open_interest'] -
    #                   x[x['datetime_minute'] == x['datetime_minute'].min()].iloc[0]['open_interest']
    #     )
    #     rdf['base_oi'] = cdf.loc[cdf.index.intersection(rdf.index)].reindex(rdf.index)
    #     rdf = rdf[rdf['base_oi'] > 0].copy()        # 仅将增仓合约纳入样本范畴，原因依然是：只有增仓才有明确信息，减仓没有明确信息
    #     rdf['contract_month'] = rdf.index.to_series().str[-4:]
    #     rdf['symbol'] = rdf.index.to_series().str[:-4]
    #     rdf['oi_chg_pctg_by_contract'] = (rdf['oi_chg'] / rdf['base_oi']) * 100
    #     oi_chg_pctg_by_symbol = (rdf.groupby('symbol')['oi_chg'].sum() / rdf.groupby('symbol')['base_oi'].sum()) * 100
    #     res = rdf.pivot(index='symbol', values='oi_chg_pctg_by_contract', columns='contract_month').assign(
    #         oi_chg_pctg_by_symbol=oi_chg_pctg_by_symbol
    #     ).sort_values(by='oi_chg_pctg_by_symbol', ascending=True)
    #     return res

    # @staticmethod
    # def roll_window_std_pikes(vol_data):
    #     # 设置滑动窗口的大小
    #     window_size = 60
    #
    #     # 计算滚动窗口内的平均值和标准差
    #     rolling_mean = pd.Series(vol_data).rolling(window=window_size).mean()
    #     rolling_std = pd.Series(vol_data).rolling(window=window_size).std()
    #
    #     # 设定检测阈值：均值 + 2倍标准差
    #     threshold = rolling_mean + 2 * rolling_std
    #
    #     # 检测成交量是否超过阈值，标记出局部峰值
    #     spikes = vol_data > threshold
    #
    #     # 输出检测到的峰值点
    #     peak_indices = np.where(spikes)[0]
    #     return peak_indices

    # def unusual_vol_inspector(self, df: pd.DataFrame):
    #     df = self.process_volume_data(df)
    #     res = pd.DataFrame(columns=['t_delta', 'volume_quantile'])
    #     for c, v in df.groupby('contract'):
    #         sample_full = v.set_index('datetime_minute').sort_index(ascending=True)['volume']
    #         sample = sample_full.iloc[-(60 * 4):]
    #         idx = self.roll_window_std_pikes(np.array(sample.values))
    #         t = sample.index.to_series().iloc[idx].max()
    #         try:
    #             v = sample.loc[t]
    #         except KeyError:
    #             continue
    #         else:
    #             t_delta = (sample.index.max() - t).seconds
    #             vol_quantile = len(sample_full[sample_full <= v]) / len(sample_full)
    #             res.loc[c] = {'t_delta': t_delta, 'volume_quantile': vol_quantile}
    #         """
    #         这里想做的是：可视化，把所有合约通过一个个方块可视化出来，也可以做成表格形式：column：合约月份，index：symbol，然后
    #         value值就是最近一次vol异常值距离当前的时间差值（可以是分钟可以是秒），然后低于5分钟的就把这个方块标红。直观看到当前哪些合约
    #         出现异常巨量成交。
    #         """
    #     res = res.assign(symbol=res.index.to_series().str[:-4]).reset_index(drop=False, names=['contract'])
    #     return res

    def unusual_option_whales(self):
        option_oi_data = self.option_oi_tick_sample
        option_oi_data['oi_chg'] = option_oi_data['open_interest'] - option_oi_data['pre_open_interest']
        option_oi_data['underlying'] = option_oi_data['contract'].str.split('-').str.get(0)
        option_oi_data['mul'] = [self.multiplier.loc[i] for i in option_oi_data['symbol']]
        option_oi_data['average_price'] = np.where(
            option_oi_data['exchange'] == 'CZCE',
            option_oi_data['average_price'],
            option_oi_data['average_price'] / option_oi_data['mul']
        )
        option_oi_data['oi_cap_chg'] = (option_oi_data['oi_chg'] * option_oi_data['mul'] * option_oi_data['average_price']).round(2)
        option_oi_data['oi_chg_pctg'] = (option_oi_data['open_interest'] / (option_oi_data['pre_open_interest'] + 1) - 1).round(2)
        option_oi_data = option_oi_data[(option_oi_data['oi_cap_chg'].abs() > 100_000) & (option_oi_data['oi_chg_pctg'].abs() > 0.5)].copy()
        option_oi_data = option_oi_data[['symbol', 'underlying', 'contract', 'oi_cap_chg', 'oi_chg_pctg', 'open_interest', 'pre_open_interest']].copy()
        return option_oi_data

    def read_dataframe_from_rds(self, k):
        return self.rds.decode_dataframe(self.rds.get_key(db=1, k=k, decode=False))

    def save_dataframe_to_rds(self, k: str, v: pd.DataFrame):
        self.rds.set_key(db=1, k=k, v=self.rds.encode_dataframe(v))

    def run(self):
        while not self.rule.api_exit_signal(datetime.now()):
            df = self.oi_tick_sample.copy()

            res_option_whales = self.unusual_option_whales()
            self.save_dataframe_to_rds(k='unusual_option_whales', v=res_option_whales)

            # res = self.calc_oi_neg_chg_pctg(df)
            # self.save_dataframe_to_rds(k='oi_neg_chg_rank', v=res)

            # vol_res = self.unusual_vol_inspector(df)
            # self.save_dataframe_to_rds(k='unusual_volume', v=vol_res)

            os.system('cls')
            print(f"{self.__class__.__name__} update @ {datetime.now()}")
        else:
            logger.warning("Model exited outside trading time.")


if __name__ == "__main__":
    o = OiVolAssess()
    x = o.unusual_option_whales()
    print(x)