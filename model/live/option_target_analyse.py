import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from model.tool.technicals import technical_indicators as ti
from model.live.sub.option_greeks import OptionGreeks
from model.live.sub.md_assess import MdAssess
from model.live.sub.stake_mapping import StakeMap
from utils.buffer.redis_handle import Redis
from utils.database.unified_db_control import UnifiedControl
from utils.tool.logger import log
from utils.tool.configer import Config

logger = log(__file__, 'model')


REQ_RATIO_LIMIT = 2.5
REQ_MAX_LOSS = 1000
REQ_MIN_HLD_VAL = 100_000
REQ_EXPIRY = 1


class OptTargetAnalyse(OptionGreeks):
    def __init__(self):
        super().__init__()
        self.rds = Redis()
        self.udc = UnifiedControl(db_type='base')

        self.origin = UnifiedControl(db_type='origin')

        config = Config()
        self.exchange_list = config.exchange_list

        self.hist_prices = self.init_all_hist_price_data()

        self.md_assess = MdAssess()
        self.smap = StakeMap()

    def get_all_hist_price_data(self, exchange: str, md_type: str):
        dt_o = self.origin.read_dataframe(
            "origin_future_cn_md_data", "all_tick_CTP",
            sql_str="select MIN(`trading_date`) from origin_future_cn_md_data.all_tick_CTP"
        )
        if dt_o.empty:
            raise ValueError("EMPTY TICK DATA.")
        start_dt = dt_o.iloc[0, 0]
        df = self.udc.read_dataframe(
            f"pretreated_{md_type}_cn_md_data", f"all_1d_{exchange}",
            filter_datetime={'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}
        )
        if df.empty:
            return None
        else:
            df.sort_values(by='trading_date', ascending=False, inplace=True)
            res = pd.concat(
                [
                    df.groupby('contract')['high'].max(),
                    df.groupby('contract')['low'].min()
                ],
                axis=1
            )
            return res

    def init_all_hist_price_data(self):
        res_df = pd.DataFrame()
        for e in self.exchange_list:
            for t in ['future', 'option']:
                r = self.get_all_hist_price_data(e, t)
                if r is not None:
                    res_df = pd.concat([res_df, r], axis=0)
        return res_df

    @property
    def consistency(self):
        c = self.rds.get_hash(
            db=1, name='consistency', decode=True
        )
        return pd.Series(c).astype(float)

    def big_order_snap(self):
        df = self.smap.current_big_order_snap()
        self.save_result("raw_bo", df)
        return df

    @staticmethod
    def organise_data(df: pd.DataFrame):
        df[['md_top', 'md_bot', 'oi_top', 'oi_bot']] = df[['md_top', 'md_bot', 'oi_top', 'oi_bot']].round(2).astype(str)
        df['mdx'] = df['md_top'] + '/' + df['md_bot']
        df['oix'] = df['oi_top'] + '/' + df['oi_bot']

        df[['und_hist_price_position', 'und_hist_price_delta', 'hist_price_position', 'hist_price_delta']] = df[[
            'und_hist_price_position', 'und_hist_price_delta', 'hist_price_position', 'hist_price_delta'
        ]].applymap(lambda x: x * 100).round(2).astype(str)

        df['und_hpp/delta'] = df['und_hist_price_position'] + "/" + df['und_hist_price_delta']
        df['hpp/delta'] = df['hist_price_position'] + "/" + df['hist_price_delta']

        df = df[[
            'exchange', 'symbol', 'underlying', 'contract', 'X', 'mdx', 'oix',
            'und_last', 'und_price_chg', 'und_hpp/delta', 'und_oi_chg',
            'last', 'price_chg', 'hpp/delta', 'oi_chg',
            'aim', 'mid_price', 'theo_p', 'stop',
            'POT.PROFIT', 'POT.LOSS', 'current_ratio', 'premium', 'iv', 'hv20', 'daily_theta_decay',
            'holding_value', 'OTM_pctg', 'underlying_l/s_dif', 'option_l/s_dif',
            'mover_und', 'mover_opt', 'days_before_expire', 'rm1', 'rm2', 'rmu1', 'rmu2', 'iv_ratio'
        ]].copy()
        df[[
            'und_price_chg', 'price_chg', 'oi_chg', 'und_oi_chg', 'premium', 'iv', 'hv20', 'OTM_pctg',
            'underlying_l/s_dif', 'option_l/s_dif'
        ]] = df[[
            'und_price_chg', 'price_chg', 'oi_chg', 'und_oi_chg', 'premium', 'iv', 'hv20', 'OTM_pctg',
            'underlying_l/s_dif', 'option_l/s_dif'
        ]].applymap(lambda x: x * 100)
        df['OTM_pctg'] = df['OTM_pctg'].round(2).astype(str) + '%'

        df = df.rename(
            columns={
                'und_last': 'und_p',
                'last': 'p',
                'und_price_chg': 'und_p_delta',
                'price_chg': 'p_delta',
                'und_oi_chg': 'und_oi_delta',
                'oi_chg': 'oi_delta',
                'current_ratio': 'RATIO',
                'POT.PROFIT': 'Est.PROFIT',
                'POT.LOSS': 'Est.LOSS',
                'daily_theta_decay': 'THETA_DECAY',
                'holding_value': 'Mkt.Value',
                'OTM_pctg': 'OTM',
                'days_before_expire': 'expiry',
                'underlying_l/s_dif': 'UND_L/S',
                'option_l/s_dif': 'OPT_L/S',
                'mover_und': 'MOV_UND',
                'mover_opt': 'MOV_OPT',
                'mid_price': 'mid',
                'theo_p': 'theo',
            }
        )
        return df

    def save_result(self, k: str, v: pd.DataFrame):
        self.rds.set_key(db=1, k=k, v=self.rds.encode_dataframe(v))

    def save_iv_records_from_raw_df(self, df: pd.DataFrame):
        df = df[['contract', 'underlying', 'trading_date', 'datetime', 'datetime_minute', 'iv']].copy()
        self.origin.insert_dataframe(
            df,
            "origin_future_cn_md_data",
            "iv_records_by_option_contract_DIY",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def read_dataframe_from_rds(self, k):
        return self.rds.decode_dataframe(self.rds.get_key(db=1, k=k, decode=False))

    def filter_out_possible_targets(self):
        for odf in self.run_calc():
            bo_snap = self.big_order_snap()

            # cheap_options_df = odf.sort_values(by='premium', ascending=True).reset_index(drop=True)
            # cheap_options_df = cheap_options_df[cheap_options_df['premium'] < 0].copy()
            # cheap_options_df = cheap_options_df.groupby(['underlying', 'direction'])['premium'].mean()
            # print(cheap_options_df)
            # exit()

            odf.set_index('symbol', drop=False, inplace=True)
            odf['X'] = self.consistency.loc[
                self.consistency.index.intersection(odf.index)
            ].reindex(odf.index)

            odf.set_index('contract', drop=False, inplace=True)

            odf[['rm1', 'rm2']] = np.nan if bo_snap.empty else bo_snap.loc[
                bo_snap.index.intersection(odf.index)
            ][['rm1', 'rm2']].reindex(odf.index)

            odf[['hhigh', 'hlow']] = np.nan if self.hist_prices.empty else self.hist_prices.loc[
                self.hist_prices.index.intersection(odf.index)
            ][['high', 'low']].reindex(odf.index)
            odf['hist_price_position'] = (odf['last'] - odf['hlow']) / (odf['hhigh'] - odf['hlow'])
            odf['hist_price_delta'] = (odf['last'] / odf['hlow']) - 1

            self.save_result("target_underlying", odf.loc[:, ['underlying']].drop_duplicates(subset=['underlying']))

            bo_und, bo_opt, mover_s = self.md_assess.big_order_sum_up()
            odf.set_index('underlying', drop=False, inplace=True)

            odf[['rmu1', 'rmu2']] = np.nan if bo_snap.empty else bo_snap.loc[
                bo_snap.index.intersection(odf.index)
            ][['rm1', 'rm2']].reindex(odf.index)

            odf[['uhhigh', 'uhlow']] = np.nan if self.hist_prices.empty else self.hist_prices.loc[
                self.hist_prices.index.intersection(odf.index)
            ][['high', 'low']].reindex(odf.index)
            odf['und_hist_price_position'] = (odf['und_last'] - odf['uhlow']) / (odf['uhhigh'] - odf['uhlow'])
            odf['und_hist_price_delta'] = np.where(
                odf['X'] >= 0, (odf['und_last'] / odf['uhlow']) - 1, (odf['und_last'] / odf['uhhigh']) - 1
            )

            dows_df = self.read_dataframe_from_rds('underlying_dows_value')
            odf[['md_top', 'md_bot', 'oi_top', 'oi_bot']] = np.nan if dows_df.empty else dows_df.loc[
                dows_df.index.intersection(odf.index)
            ].reindex(odf.index)

            odf['underlying_l/s_dif'] = bo_und.loc[bo_und.index.intersection(odf.index)].reindex(odf.index)
            odf['option_l/s_dif'] = bo_opt.loc[bo_opt.index.intersection(odf.index)].reindex(odf.index)
            odf['mover_und'] = mover_s.loc[mover_s.index.intersection(odf.index)].reindex(odf.index)
            odf.set_index('contract', drop=False, inplace=True)
            odf['mover_opt'] = mover_s.loc[mover_s.index.intersection(odf.index)].reindex(odf.index)

            self.save_iv_records_from_raw_df(odf)

            raw_df = self.organise_data(odf.copy())

            self.save_result("raw_options", raw_df)

            odf = odf[odf['days_before_expire'] >= REQ_EXPIRY].copy()
            odf = odf[odf['direc'] == odf['X'].apply(np.sign)].copy()
            odf = odf[odf['OTM_pctg'] <= odf['possible_range']].copy()
            odf = odf[odf['holding_value'] >= REQ_MIN_HLD_VAL].copy()
            odf = odf[odf['current_ratio'] >= REQ_RATIO_LIMIT].copy()
            odf = odf[odf['POT.LOSS'] <= REQ_MAX_LOSS].copy()

            odf = self.organise_data(odf)

            odf = odf.set_index(['exchange', 'symbol', 'underlying', 'contract']).sort_index(
                ascending=[False, True, True, True]
            )

            self.save_result("option_targets", odf)

            os.system('cls')
            print(f"{self.__class__.__name__} update @ {datetime.now()}")


if __name__ == "__main__":
    o = OptTargetAnalyse()
    o.filter_out_possible_targets()
