from datetime import datetime
import pandas as pd
import numpy as np
from model.live.live_data_source.source import MDO
from utils.database.unified_db_control import UnifiedControl
from utils.tool.logger import log

logger = log(__file__, 'model')


class StakeMap:
    def __init__(self):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')

    def get_position_main_contract(self, symbol: str, start_date: datetime):
        df = self.live_source.udc.read_dataframe(
            "processed_future_cn_model_data", "valid_position_by_main_contract_SUM",
            filter_datetime={
                'trading_date': {'gte': start_date.strftime('%Y-%m-%d')}
            },
            filter_keyword={
                'symbol': {'eq': symbol}
            }
        )
        return df

    def get_position_symbol(self, symbol: str, start_date: datetime):
        df = self.live_source.udc.read_dataframe(
            "processed_future_cn_model_data", "valid_position_by_symbol_SUM",
            filter_datetime={
                'trading_date': {'gte': start_date.strftime('%Y-%m-%d')}
            },
            filter_keyword={
                'symbol': {'eq': symbol}
            }
        )
        return df

    def get_md(self, contract: str):
        md_df = self.live_source.tick_md_data(contract)
        if md_df.empty:
            return
        md_df = md_df.sort_values(by='datetime', ascending=True).assign(
            vol_chg=md_df['volume'].diff(),
            oi_chg=md_df['open_interest'].diff()
        )
        md_df['vol_chg'] = np.where(md_df['vol_chg'] < 0, np.nan, md_df['vol_chg'])
        return md_df

    def md_stake_map(self, contract: str):
        md_df = self.get_md(contract)
        md_last = md_df.groupby('datetime_minute')[['last', 'open_interest']].last()
        stake_df = md_df[['last', 'vol_chg', 'oi_chg']].copy().groupby('last').sum()
        return md_last, stake_df

    def big_order_map(self, contract: str):
        df = self.live_source.big_orders(self.live_source.min_trade_date(), contract)
        if df.empty:
            return
        df['valid_money_delta'] = df['money_delta'] * df['information_ratio'].abs()
        df['abs_valid_money_delta'] = (df['money_delta'] * df['information_ratio']).abs()
        stake_df = df.groupby('last')[['valid_money_delta', 'abs_valid_money_delta']].sum() / 1e6
        return stake_df

    def big_order_cumsum(self, contract: str):
        order_df = self.live_source.big_orders(self.live_source.min_trade_date(), contract=contract)

        order_df['real_money'] = order_df['money_delta'] * order_df['information_ratio'].abs()
        # 按照多头开仓、多头平仓、空头开仓、空头平仓的逻辑来统计多头和空头的大单，然后累加，形成多头/空头大单的走势图
        order_df['rm_long_inc'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_long_dec'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_short_inc'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_short_dec'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_long'] = order_df['rm_long_inc'] + order_df['rm_long_dec']
        order_df['rm_short'] = order_df['rm_short_inc'] + order_df['rm_short_dec']

        if order_df.empty:
            return order_df

        order_df['datetime_minute'] = order_df['datetime'].dt.ceil("1min")
        big_order_minute = order_df.groupby('datetime_minute')[['rm_long', 'rm_short']].sum()
        res_ = big_order_minute.sort_index(ascending=True).cumsum()
        res_['rm_delta'] = res_['rm_long'] - res_['rm_short']
        return res_

    def big_order_daily_sum(self, contract: str, start_date: datetime):
        order_df = self.live_source.big_orders(contract=contract)

        order_df['real_money'] = order_df['money_delta'] * order_df['information_ratio'].abs()
        order_df['rm_long_inc'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_long_dec'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_short_inc'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_short_dec'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_long'] = order_df['rm_long_inc'] + order_df['rm_long_dec']
        order_df['rm_short'] = order_df['rm_short_inc'] + order_df['rm_short_dec']

        if order_df.empty:
            return order_df
        df = order_df.groupby('trading_date')[['rm_long', 'rm_short']].sum()
        vp_by_main = self.get_position_main_contract(contract[:-4], start_date)
        if not vp_by_main.empty:
            vp_by_main_s = vp_by_main.set_index('trading_date')['valid_net_pos']
            df = pd.concat([df, vp_by_main_s.rename('vp_main')], axis=1)
        else:
            df['vp_main'] = np.nan
        vp_by_sym = self.get_position_symbol(contract[:-4], start_date)
        if not vp_by_sym.empty:
            vp_by_sym_s = vp_by_sym.set_index('trading_date')['valid_net_pos']
            df = pd.concat([df, vp_by_sym_s.rename('vp_symbol')], axis=1)
        else:
            df['vp_symbol'] = np.nan
        return df

    def current_big_order_snap(self):
        order_df = self.live_source.big_orders(self.live_source.this_trading_date)
        # rm1按照多 空 开 平 计算多头和空头各自的大单
        order_df['rm1'] = order_df['money_delta'] * order_df['information_ratio'].abs()
        order_df['rm_long_inc'] = np.where(((order_df['price_chg'] > 0) & (order_df['rm1'] > 0)), order_df['rm1'], 0)
        order_df['rm_long_dec'] = np.where(((order_df['price_chg'] < 0) & (order_df['rm1'] < 0)), order_df['rm1'], 0)
        order_df['rm_short_inc'] = np.where(((order_df['price_chg'] < 0) & (order_df['rm1'] > 0)), order_df['rm1'], 0)
        order_df['rm_short_dec'] = np.where(((order_df['price_chg'] > 0) & (order_df['rm1'] < 0)), order_df['rm1'], 0)
        order_df['rm1_long'] = order_df['rm_long_inc'] + order_df['rm_long_dec']
        order_df['rm1_short'] = order_df['rm_short_inc'] + order_df['rm_short_dec']

        # rm2按照对价格的推进计算rm绝对值，即累计推进大单量
        order_df['rm2'] = (order_df['money_delta'] * order_df['information_ratio']).abs()
        order_df['rm2_long'] = np.where(order_df['price_chg'] > 0, order_df['rm2'], 0)
        order_df['rm2_short'] = np.where(order_df['price_chg'] < 0, order_df['rm2'], 0)
        xdf = order_df.groupby('contract')[['rm1_long', 'rm1_short', 'rm2_long', 'rm2_short']].sum()
        res = pd.concat(
            [
                pd.Series(np.sign(xdf['rm1_long'] - xdf['rm1_short']), index=xdf.index, name='rm1'),
                pd.Series(np.sign(xdf['rm2_long'] - xdf['rm2_short']), index=xdf.index, name='rm2'),
            ],
            axis=1
        )
        return res

    def start_mapping(self, contract):
        md_last, stake_md = self.md_stake_map(contract)
        stake_big_orders = self.big_order_map(contract)

        stake_df = pd.concat(
            [stake_md, stake_big_orders],
            axis=1
        ).fillna(0).rename(
            columns={
                'vol_chg': 'volume_stake',
                'oi_chg': 'oi_stake',
                'valid_money_delta': 'filtered_order_stake',
                'abs_valid_money_delta': 'abs_big_order_stake'
            }
        )
        return md_last.sort_index(ascending=True), stake_df.sort_index(ascending=False)
