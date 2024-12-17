import pandas as pd
import numpy as np
from datetime import datetime
from model.live.live_data_source.source import MDO
import infra.math.option_calc as oc
import model.tool.technicals.technical_indicators as ti
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import RedisMsg
from utils.tool.logger import log

logger = log(__file__, 'model')

RISK_FREE_RATE = 0.02
OBSERVE_DAYS = 5
MAX_POSSIBLE_RANGE = 0.1
IV_RANGE_SURPLUS = 1


class OptionGreeks:
    def __init__(self):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')

    def run_calc(self):
        for md in self.live_source.md_snapshot():
            md['mid_price'] = ti.calculate_orderbook_vwap(
                md['ask1'], md['ask_vol1'], md['bid1'], md['bid_vol1']
            )

            und_md = md[~md['contract'].str.contains('-')].copy()
            und_md.set_index('contract', inplace=True, drop=False)
            und_md['oi_chg'] = und_md['open_interest'] / und_md['pre_open_interest'] - 1
            opt_md = md[md['contract'].str.contains('-')].copy()
            opt_md['oi_chg'] = opt_md['open_interest'] / opt_md['pre_open_interest'] - 1
            opt_md['holding_value'] = opt_md['open_interest'] * opt_md['average_price'] * opt_md['multiplier']
            opt_md.set_index('contract', inplace=True, drop=False)
            opt_md['days_before_expire'] = self.live_source.option_contract['days_before_expire'].loc[
                self.live_source.option_contract.index.intersection(opt_md.index)
            ].reindex(opt_md.index)
            opt_md.reset_index(drop=True, inplace=True)
            extracts_df = opt_md['contract'].str.extract(
                "(?P<underlying>[A-Z]+[0-9]{4})-(?P<direction>[A-Z])-(?P<strike>[0-9]+)"
            )
            opt_md = pd.concat([opt_md, extracts_df], axis=1).set_index('underlying', drop=False)
            opt_md['strike'] = opt_md['strike'].astype(float)
            opt_md['direc'] = np.where(opt_md['direction'] == 'C', 1, -1)
            opt_md['und_last'] = und_md['last'].loc[und_md.index.intersection(opt_md.index)].reindex(opt_md.index)
            opt_md['und_open'] = und_md['open'].loc[und_md.index.intersection(opt_md.index)].reindex(opt_md.index)
            opt_md['und_pre_close'] = und_md['pre_close'].loc[und_md.index.intersection(opt_md.index)].reindex(opt_md.index)
            opt_md['und_oi_chg'] = und_md['oi_chg'].loc[und_md.index.intersection(opt_md.index)].reindex(opt_md.index)
            opt_md['OTM_pctg'] = opt_md['direc'] * (opt_md['strike'] / opt_md['und_last'] - 1)
            opt_md['OTM'] = np.where(opt_md['OTM_pctg'] > 0, 'Y', 'N')
            opt_md = opt_md[opt_md['OTM'] == 'Y'].copy()         # only calc OTM options
            opt_md['OTM_dist'] = opt_md['OTM_pctg'].abs() * 100

            atm_df = opt_md.reset_index(drop=True).sort_values(
                by='OTM_dist', ascending=True
            ).groupby('underlying').agg(
                {'strike': 'first', 'mid_price': 'first'}
            )

            opt_md['ATM_strike'] = atm_df['strike'].loc[atm_df.index.intersection(opt_md.index)].reindex(opt_md.index)
            opt_md['ATM_mid'] = atm_df['mid_price'].loc[atm_df.index.intersection(opt_md.index)].reindex(opt_md.index)

            opt_md['hv20'] = self.live_source.hv["hv20"].loc[
                self.live_source.hv.index.intersection(opt_md.index)
            ].reindex(opt_md.index)
            opt_md['rv20'] = self.live_source.hv["rv20"].loc[
                self.live_source.hv.index.intersection(opt_md.index)
            ].reindex(opt_md.index)
            opt_md['iv'] = [
                oc.baw_iv_call(v['und_last'], v['strike'], v['days_before_expire'], RISK_FREE_RATE, v['mid_price'], 0) if v['direc'] == 1 else
                oc.baw_iv_put(v['und_last'], v['strike'], v['days_before_expire'], RISK_FREE_RATE, v['mid_price'], 0) for _, v in opt_md.iterrows()
            ]
            opt_md['iv_o'] = [
                oc.baw_iv_call(v['und_open'], v['strike'], v['days_before_expire'], RISK_FREE_RATE, v['open'], 0) if v['direc'] == 1 else
                oc.baw_iv_put(v['und_open'], v['strike'], v['days_before_expire'], RISK_FREE_RATE, v['open'], 0) for _, v in opt_md.iterrows()
            ]
            opt_md['theo_p'] = [
                oc.baw_price_call(v['und_last'], v['strike'], v['days_before_expire'], v['hv20'], RISK_FREE_RATE, 0) if v['direc'] == 1 else
                oc.baw_price_put(v['und_last'], v['strike'], v['days_before_expire'], v['hv20'], RISK_FREE_RATE, 0) for _, v in opt_md.iterrows()
            ]
            opt_md['theta_annual'] = [
                oc.bs_theta_call(v['und_last'], v['strike'], v['days_before_expire'], v['iv'], RISK_FREE_RATE, 0) if v['direc'] == 1 else
                oc.bs_theta_put(v['und_last'], v['strike'], v['days_before_expire'], v['iv'], RISK_FREE_RATE, 0) for _, v in opt_md.iterrows()
            ]
            opt_md['daily_theta_decay'] = opt_md['theta_annual'] / 365

            opt_md['avail_days'] = np.where(
                opt_md['days_before_expire'] >= OBSERVE_DAYS,
                OBSERVE_DAYS,
                opt_md['days_before_expire']
            )

            opt_md['possible_range'] = ((((opt_md['iv'] / np.sqrt(255)) * IV_RANGE_SURPLUS) + 1) ** opt_md['avail_days']) - 1  # 估算iv indicated 最多OBSERVE_DAYS日涨幅
            # 最远只取距离atm MAX_POSSIBLE_RANGE的期权
            opt_md['possible_range'] = np.where(
                opt_md['possible_range'] >= MAX_POSSIBLE_RANGE,
                MAX_POSSIBLE_RANGE,
                opt_md['possible_range']
            )

            opt_md['und_stop'] = opt_md['und_last'] * (1 - opt_md['OTM_pctg'] * opt_md['direc'])  # 计算对应的标的止损价
            opt_md['und_stop_opt_p'] = [
                oc.baw_price_call(v['und_stop'], v['strike'], v['days_before_expire'], v['iv'], RISK_FREE_RATE, 0) if v['direc'] == 1 else
                oc.baw_price_put(v['und_stop'], v['strike'], v['days_before_expire'], v['iv'], RISK_FREE_RATE, 0) for _, v in opt_md.iterrows()
            ]  # 计算对应的标的止损价对应的期权价格，估算

            # 目标价采取当前目标ATM价减去有效期内的theta损失
            opt_md['aim'] = opt_md['ATM_mid'] + opt_md['daily_theta_decay'] * opt_md['avail_days']  # 这里daily_theta_decay是负值，所以用+
            opt_md['stop'] = opt_md['und_stop_opt_p'] + opt_md['daily_theta_decay'] * opt_md['avail_days']  # 止损价是标的止损价对应期权价减去theta损失

            opt_md['stop'] = np.where(
                opt_md['stop'] >= 0,
                opt_md['stop'],
                0
            )  # 要么损失avail_days的theta（对应远期）+ 反向波动的价格，要么归零（对应近期）

            opt_md['current_ratio'] = (opt_md['aim'] - opt_md['mid_price']) / (opt_md['mid_price'] - opt_md['stop'])

            opt_md['POT.PROFIT'] = (opt_md['aim'] - opt_md['mid_price']) * opt_md['multiplier']
            opt_md['POT.LOSS'] = (opt_md['mid_price'] - opt_md['stop']) * opt_md['multiplier']

            opt_md['premium'] = opt_md['mid_price'] / opt_md['theo_p'] - 1
            opt_md['price_chg'] = opt_md['last'] / opt_md['pre_close'] - 1
            opt_md['und_price_chg'] = opt_md['und_last'] / opt_md['und_pre_close'] - 1

            cp_iv_ratio = pd.Series()
            for und, udf in opt_md.groupby(opt_md.index):
                call_df = udf[udf['direction'] == 'C'].copy()
                call_u = np.nan if call_df.empty else call_df['iv'].mean() - call_df[call_df['strike'] == call_df['strike'].min()].iloc[0]['iv']
                call_u = np.nan if call_u <= 0 else call_u

                put_df = udf[udf['direction'] == 'P'].copy()
                put_u = np.nan if put_df.empty else put_df['iv'].mean() - put_df[put_df['strike'] == put_df['strike'].max()].iloc[0]['iv']
                put_u = np.nan if put_u <= 0 else put_u

                res_u = call_u / put_u
                res_u_log = np.log(res_u, where=res_u > 0)
                cp_iv_ratio[und] = res_u_log

            opt_md['iv_ratio'] = cp_iv_ratio.loc[cp_iv_ratio.index.intersection(opt_md.index)].reindex(opt_md.index)

            yield opt_md
