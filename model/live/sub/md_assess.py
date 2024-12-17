import os
import traceback

import pandas as pd
import numpy as np
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

# REQ OI HIGH
OI_TOP_REQ = 0.1
OI_BOT_REQ = 0.9
REQ_SETTLEMENT_SAMPLE_DAYS = 10


class MdAssess:
    def __init__(self):
        self.live_source = MDO()
        self.origin = UnifiedControl('origin')
        self.base = UnifiedControl('base')
        self.rds = Redis()
        config = Config()
        self.exchange_list = config.exchange_list

        self.rule = TradeRules()

        self.start = self.live_source.this_trading_date + timedelta(days=-90)

        self.all_mains = self.get_all_main_contracts()

        self.settle_price_start_sample_date = min(self.live_source.prev_n_trading_date(REQ_SETTLEMENT_SAMPLE_DAYS))

        self.__del_hist_extreme_price_cnt_archive__()
        self.__del_hist_extreme_position_cnt_archive__()

        self.hist_settlement = self.get_hist_settle_price()

    def get_hist_settle_price(self):
        hist_md = pd.DataFrame()
        for e in self.exchange_list:
            df = self.base.read_dataframe(
                "pretreated_future_cn_md_data",
                f"all_1d_{e}",
                filter_keyword={'contract': {'in': self.all_mains.values.tolist()}},
                filter_datetime={'trading_date': {'gte': self.settle_price_start_sample_date.strftime('%Y-%m-%d')}},
                filter_columns=['contract', 'settlement', 'volume', 'trading_date']
            )
            hist_md = pd.concat([hist_md, df], axis=0)
        hist_md['turnover'] = hist_md['settlement'] * hist_md['volume']
        price = hist_md.groupby('contract').apply(lambda x: x['turnover'].sum() / x['volume'].sum())
        return price

    def get_current_standard_spot(self):
        last_df = self.base.read_dataframe(
            "pretreated_spot_cn_md_data", "standard_price_100ppi",
            ascending=[('record_date', False)],
            filter_row_limit=1
        )
        if last_df.empty:
            return None, None
        date_ = last_df.iloc[0]['record_date'].strftime("%Y-%m-%d")
        filt_date = {'record_date': {'eq': date_}}
        raw_df = self.base.read_dataframe(
            db_name="pretreated_spot_cn_md_data",
            tb_name="standard_price_100ppi",
            filter_datetime=filt_date
        )
        return raw_df.set_index('symbol')['price'], date_

    def get_spot_hist_md(self, symbol: str):
        md = self.base.read_dataframe(
            "pretreated_spot_cn_md_data", "standard_price_100ppi",
            filter_keyword={'symbol': {'eq': symbol}},
            filter_datetime={'record_date': {'gte': self.start.strftime('%Y-%m-%d')}},
            filter_columns=['record_date', 'price']
        )
        if md.empty:
            return pd.Series(dtype=float)
        else:
            return md.set_index('record_date')['price']

    def get_hist_md(self, contract: str, exchange: str):
        underlying_contract = contract.split('-')[0]
        und_md = self.base.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{exchange}",
            filter_keyword={'contract': {'eq': underlying_contract}},
            filter_datetime={'trading_date': {'gte': self.start.strftime('%Y-%m-%d')}},
            filter_columns=['trading_date', 'close']
        ).rename(columns={'close': 'und_close'}).set_index('trading_date')
        opt_md = self.base.read_dataframe(
            "pretreated_option_cn_md_data", f"all_1d_{exchange}",
            filter_keyword={'contract': {'eq': contract}},
            filter_datetime={'trading_date': {'gte': self.start.strftime('%Y-%m-%d')}},
            filter_columns=['trading_date', 'close']
        ).rename(columns={'close': 'opt_close'}).set_index('trading_date')
        md = pd.concat([und_md, opt_md], axis=1)
        return md

    def get_und_hist_md(self, underlying_contract: str, exchange: str):
        und_md = self.base.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{exchange}",
            filter_keyword={'contract': {'eq': underlying_contract}},
            filter_datetime={'trading_date': {'gte': self.start.strftime('%Y-%m-%d')}},
            filter_columns=['trading_date', 'close']
        ).rename(columns={'close': 'und_close'}).set_index('trading_date')
        return und_md

    @property
    def option_symbols(self):
        res = []
        for e in self.exchange_list:
            df = self.base.read_dataframe(
                db_name="processed_option_cn_meta_data",
                tb_name=f"spec_info_{e}"
            )
            res += df['symbol'].tolist()
        return res

    @property
    def hist_iv_range_data(self):
        prev_n_ls = self.live_source.prev_n_trading_date(60)
        start_dt = None if len(prev_n_ls) == 0 else min(prev_n_ls)
        filt_ = None if start_dt is None else {'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}
        df = self.base.read_dataframe(
            "pretreated_option_cn_md_data",
            "all_1d_iv_range_DIY",
            filter_datetime=filt_
        )
        return df

    def get_tick_data(self, contract: str):
        return self.live_source.tick_md_data(contract)

    def get_derivatives_data(self, contract: str):
        return self.live_source.derivatives_tick_md_data(contract)

    @staticmethod
    def calculate_tick_return_skewness(tick_sample_df: pd.DataFrame, rolling_sample_cnt: int = 1200):
        """
        rolling_sample_cnt: 10 minutes period (approx.) = 10 * 60 sec * 2 tick/sec = 1200
        """
        tick_sample_df = tick_sample_df.set_index('datetime').sort_index(ascending=True)
        tick_sample_df['ret'] = round((tick_sample_df['last'] / tick_sample_df['last'].shift() - 1) * 1000, 2)
        tick_sample_df['sk'] = tick_sample_df['ret'].rolling(rolling_sample_cnt).skew()
        sk_df = tick_sample_df[['sk', 'datetime_minute']].copy().sort_index(ascending=True)
        """
        由于数据量太大，对于前后端交互效率有影响。试过对sk进行一定程度的数据统一，之后取关键节点数据进行传输，但这样画图出来很怪
        所以还是用平均的方式，将每分钟内的skew进行平均。
        """
        sk_df = sk_df.groupby('datetime_minute')[['sk']].mean()
        sk_df['sk'].iloc[: 10] = np.nan
        return sk_df.sort_index(ascending=True)

    def big_order_sum_up(self):
        order_df = self.live_source.big_orders(self.live_source.min_trade_date())
        order_df['real_money'] = order_df['money_delta'] * order_df['information_ratio'].abs()
        # 只算多头和空头的增仓部分，这部分数据用来计算MOVER值
        order_df['pos_long_mover'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['pos_neut_mover'] = np.where(((order_df['price_chg'] == 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['pos_short_mover'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        # 按照（多头增仓+空头减仓）计算多头绝对值，（空头增仓+多头减仓）计算空头绝对值，这部分数据用来计算多空比率
        order_df['abs_long_mover'] = np.where(order_df['price_chg'] > 0, order_df['real_money'].abs(), 0)
        order_df['abs_neut_mover'] = np.where(order_df['price_chg'] == 0, order_df['real_money'].abs(), 0)
        order_df['abs_short_mover'] = np.where(order_df['price_chg'] < 0, order_df['real_money'].abs(), 0)

        tot_long = pd.concat(
            [order_df.groupby('contract')['pos_long_mover'].sum(), order_df.groupby('contract')['abs_long_mover'].sum()],
            axis=1)
        tot_neut = pd.concat(
            [order_df.groupby('contract')['pos_neut_mover'].sum(), order_df.groupby('contract')['abs_neut_mover'].sum()],
            axis=1)
        tot_short = pd.concat(
            [order_df.groupby('contract')['pos_short_mover'].sum(), order_df.groupby('contract')['abs_short_mover'].sum()],
            axis=1)
        tot_df = pd.concat([tot_long, tot_neut, tot_short], axis=1)

        # mover: oi increasing real money diff
        mover_s = (tot_df['pos_long_mover'] - tot_df['pos_short_mover']) / 1e4

        tot_df.reset_index(drop=False, inplace=True)
        tot_df.drop(columns=['pos_long_mover', 'pos_neut_mover', 'pos_short_mover'], inplace=True)
        # result underlying l/s factor
        und_tot = tot_df[~tot_df['contract'].str.contains('-')].copy().set_index('contract')
        und_tot['long_f'] = und_tot['abs_long_mover'] / und_tot[
            ['abs_long_mover', 'abs_neut_mover', 'abs_short_mover']].sum(axis=1)
        und_tot['short_f'] = und_tot['abs_short_mover'] / und_tot[
            ['abs_long_mover', 'abs_neut_mover', 'abs_short_mover']].sum(axis=1)
        und_tot['neut_f'] = 1 - und_tot[['long_f', 'short_f']].sum(axis=1)
        res_und = und_tot['long_f'] - und_tot['short_f']

        # result option l/s factor
        opt_tot = tot_df[tot_df['contract'].str.contains('-')].copy()
        opt_splits = opt_tot['contract'].str.extract(
            "(?P<underlying>[A-Z]+[0-9]{4})-(?P<direction>[C, P])-([0-9]+)"
        )
        opt_tot = pd.concat([opt_tot, opt_splits[['underlying', 'direction']]], axis=1)
        call_opt_sum = opt_tot[opt_tot['direction'] == 'C'].groupby('underlying')[[
            'abs_long_mover', 'abs_neut_mover', 'abs_short_mover'
        ]].sum().fillna(0)
        put_opt_sum = opt_tot[opt_tot['direction'] == 'P'].groupby('underlying')[[
            'abs_long_mover', 'abs_neut_mover', 'abs_short_mover'
        ]].sum().fillna(0)
        cp_sum = pd.DataFrame(
            {
                'abs_long_mover': call_opt_sum['abs_long_mover'] + put_opt_sum['abs_short_mover'],
                'abs_neut_mover': call_opt_sum['abs_neut_mover'] + put_opt_sum['abs_neut_mover'],
                'abs_short_mover': call_opt_sum['abs_short_mover'] + put_opt_sum['abs_long_mover'],
            }
        )
        cp_sum['long_f'] = cp_sum['abs_long_mover'] / cp_sum[
            ['abs_long_mover', 'abs_neut_mover', 'abs_short_mover']].sum(axis=1)
        cp_sum['short_f'] = cp_sum['abs_short_mover'] / cp_sum[
            ['abs_long_mover', 'abs_neut_mover', 'abs_short_mover']].sum(axis=1)
        cp_sum['neut_f'] = 1 - cp_sum[['long_f', 'short_f']].sum(axis=1)
        res_opt = cp_sum['long_f'] - cp_sum['short_f']
        return res_und.rename('und'), res_opt.rename('opt'), mover_s.rename('mover')

    def call_put_cap(self, und_contract: str):
        symbol = und_contract[:-4]
        opt_md_df = self.get_derivatives_data(und_contract)
        mult = self.live_source.option_spec_info.loc[symbol]
        opt_md_df['mkt_v'] = opt_md_df['open_interest'] * opt_md_df['last'] * mult
        opt_md_df['direction'] = opt_md_df['contract'].str.split('-').str.get(1)
        opt_md_df.sort_values(by='datetime', ascending=True)

        df_ddup = opt_md_df.groupby(['contract', 'datetime_minute'])[['mkt_v', 'direction']].last().reset_index(drop=False)
        calls = df_ddup[df_ddup['direction'] == 'C'].groupby('datetime_minute')['mkt_v'].sum()
        puts = df_ddup[df_ddup['direction'] == 'P'].groupby('datetime_minute')['mkt_v'].sum()
        res_cp = calls / puts
        res_cp = pd.Series(
            np.where(
                abs(res_cp.diff()) >= 10, np.nan, np.log(res_cp)
            ),
            index=res_cp.index
        )
        res = pd.concat(
            [calls.rename('mkv_call'), puts.rename('mkv_put'), res_cp.rename('cp_ratio')],
            axis=1
        )
        return res

    @staticmethod
    def dows_assess(df: pd.DataFrame):
        xdf = ti.md_channel(df)
        xdf = xdf.diff().fillna(0)
        h_slice = xdf[xdf['tops'] < 0]
        if h_slice.empty:
            hs = 0
        else:
            hs = xdf.loc[h_slice.index.min():].shape[0] / xdf.shape[0]
        l_slice = xdf[xdf['bots'] < 0]
        if l_slice.empty:
            ls = 1
        else:
            ls = xdf.loc[l_slice.index.max():].shape[0] / xdf.shape[0]
        return {'top': hs, 'bot': ls}

    def run_dows_assessment(self, ts: pd.DataFrame):
        ts.sort_values(by='datetime', ascending=True, inplace=True)
        md = pd.concat(
            [
                ts.groupby('datetime_minute')['last'].max().rename('high'),
                ts.groupby('datetime_minute')['last'].min().rename('low'),
            ],
            axis=1
        )
        oi_df = pd.concat(
            [
                ts.groupby('datetime_minute')['open_interest'].max().rename('high'),
                ts.groupby('datetime_minute')['open_interest'].min().rename('low'),
            ],
            axis=1
        )
        res_md = self.dows_assess(md)
        res_oi = self.dows_assess(oi_df)
        return {
            'md_top': res_md['top'],
            'md_bot': res_md['bot'],
            'oi_top': res_oi['top'],
            'oi_bot': res_oi['bot']
        }

    def read_dataframe_from_rds(self, k):
        return self.rds.decode_dataframe(self.rds.get_key(db=1, k=k, decode=False))

    def save_dataframe_to_rds(self, k: str, v: pd.DataFrame):
        self.rds.set_key(db=1, k=k, v=self.rds.encode_dataframe(v))

    def mkt_val_chg_on_this_trading_date(self):
        data = self.live_source.get_tick_compressed_data(self.live_source.this_trading_date)
        iv_snap = self.live_source.get_bulk_tick_iv_data(self.live_source.this_trading_date)

        if data is None:
            return
        fut, call, put = data

        main_futs = fut[fut['contract'].isin(self.hist_settlement.index.tolist())].copy()
        main_futs = main_futs.sort_values(by='latest_datetime', ascending=True).drop_duplicates(subset=['symbol'], keep='last').set_index('contract')[['symbol', 'latest_last_price']]
        main_futs['hist_settle'] = self.hist_settlement.loc[self.hist_settlement.index.intersection(main_futs.index)].reindex(main_futs.index)
        main_futs['price_deviate'] = ((main_futs['latest_last_price'] / main_futs['hist_settle'] - 1) * 100).round(2)
        main_futs.set_index('symbol', inplace=True)

        oi_sum = fut.groupby('symbol')[['latest_open_interest', 'earliest_open_interest']].sum()
        oi_pctg = (100 * (oi_sum['latest_open_interest'] / oi_sum['earliest_open_interest'] - 1)).rename('fut_oi_pctg')
        fut_res = fut.groupby('symbol')['mkt_val_chg'].sum().rename('future_chg')
        call_res = call.groupby('symbol')['mkt_val_chg'].sum().rename('call_chg')
        put_res = put.groupby('symbol')['mkt_val_chg'].sum().rename('put_chg')
        cp_diff_factor = ((call_res - put_res) / (call_res.abs() + put_res.abs())) * ((call_res - put_res).abs())
        res = pd.concat([
            oi_pctg, fut_res, call_res, put_res, cp_diff_factor.rename('cp_factor'),
            main_futs['price_deviate']
        ], axis=1).sort_values(by='future_chg', ascending=False)
        res['average_future_chg'] = res['future_chg'].mean()
        return res

    def get_all_main_contracts(self):
        prev_trading_date = self.live_source.prev_trade_date(1)
        if prev_trading_date is None:
            prev_trading_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-20)
        main_contract_s = pd.Series(dtype=str)
        for t in self.live_source.udc.get_table_names("processed_future_cn_roll_data"):
            main_contracts_df = self.live_source.udc.read_dataframe(
                "processed_future_cn_roll_data", t,
                filter_datetime={'trading_date': {'gte': prev_trading_date.strftime('%Y-%m-%d')}}
            )
            if main_contracts_df.empty:
                continue
            main_s = main_contracts_df[main_contracts_df['trading_date'] == main_contracts_df['trading_date'].max()].set_index('symbol')['O_NM_N']
            main_contract_s = pd.concat([main_contract_s, main_s])
        return main_contract_s

    def get_all_rm_delta_from_recent_tick(self):
        main_contract_s = self.all_mains
        order_df = self.live_source.big_orders(self.live_source.min_trade_date(), contract_list=main_contract_s.tolist())
        if order_df.empty:
            return
        order_df['real_money'] = order_df['money_delta'] * order_df['information_ratio'].abs()
        # 按照多头开仓、多头平仓、空头开仓、空头平仓的逻辑来统计多头和空头的大单，然后累加，形成多头/空头大单的走势图
        order_df['rm_long_inc'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_long_dec'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_short_inc'] = np.where(((order_df['price_chg'] < 0) & (order_df['real_money'] > 0)), order_df['real_money'], 0)
        order_df['rm_short_dec'] = np.where(((order_df['price_chg'] > 0) & (order_df['real_money'] < 0)), order_df['real_money'], 0)
        order_df['rm_long'] = order_df['rm_long_inc'] + order_df['rm_long_dec']
        order_df['rm_short'] = order_df['rm_short_inc'] + order_df['rm_short_dec']

        order_df['rm_delta'] = order_df['rm_long'] - order_df['rm_short']
        res = order_df.groupby('contract')[['rm_delta']].sum().apply(np.sign)
        res.index = res.index.to_series().str[:-4]
        return res

    def price_positions(self):
        mains = self.all_mains
        contract_ls = ', '.join([f"'{i}'" for i in mains.values.tolist()])
        tick_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                        SELECT DISTINCT 
                            `exchange`, `symbol`, `contract`, `last`, `datetime`
                            FROM origin_future_cn_md_data.all_tick_CTP
                            WHERE `contract` IN ({contract_ls})
                            AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                            AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                            AND (`contract`, `datetime`) IN (
                                SELECT `contract`, MAX(`datetime`)
                                FROM origin_future_cn_md_data.all_tick_CTP
                                WHERE `contract` IN ({contract_ls})
                                AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                                AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                                GROUP BY `contract`
                            )
                    """
        )
        tick_df = tick_df.sort_values(by='datetime', ascending=True).drop_duplicates(subset=['contract'], keep='last')
        tick_s = tick_df.set_index('symbol')['last']
        dts = self.live_source.prev_n_trading_date(n=10)
        start_dt = None if len(dts) == 0 else min(dts).strftime('%Y-%m-%d')
        filt_ = {'trading_date': {'gte': start_dt}} if start_dt is not None else None
        mdf = pd.DataFrame()
        for e in self.exchange_list:
            mdd_ = self.base.read_dataframe(
                "processed_future_cn_md_data", f"all_1d_main_{e}",
                filter_datetime=filt_,
                filter_keyword={'process_type': {'eq': 'O_NM_N'}},
                filter_columns=['open', 'close', 'high', 'low', 'symbol']
            )
            rm_ = mdd_.groupby('symbol').apply(lambda x: pd.Series({'max': x.values.max(), 'min': x.values.min()}))
            mdf = pd.concat([mdf, rm_], axis=0)
        mdf['range'] = mdf['max'] / mdf['min'] - 1
        mdf['curr_p'] = tick_s.loc[tick_s.index.intersection(mdf.index)].reindex(mdf.index)
        mdf = mdf.assign(position=(mdf['curr_p'] - mdf['min']) / (mdf['max'] - mdf['min'])).dropna(axis=0, how='any')
        mdf = mdf[['range', 'position']].copy() * 100
        return mdf

    def extreme_price_days_count(self, max_days: int = 100):
        mains = self.all_mains
        contract_ls = ', '.join([f"'{i}'" for i in mains.values.tolist()])
        tick_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                                SELECT DISTINCT 
                                    `exchange`, `symbol`, `contract`, `last`, `datetime`, `high`, `low` 
                                    FROM origin_future_cn_md_data.all_tick_CTP
                                    WHERE `contract` IN ({contract_ls})
                                    AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                                    AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                                    AND (`contract`, `datetime`) IN (
                                        SELECT `contract`, MAX(`datetime`)
                                        FROM origin_future_cn_md_data.all_tick_CTP
                                        WHERE `contract` IN ({contract_ls})
                                        AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                                        AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                                        GROUP BY `contract`
                                    )
                            """
        )
        tick_df = tick_df.sort_values(by='datetime', ascending=True).drop_duplicates(subset=['contract'], keep='last')
        tick_df = tick_df.set_index('symbol')[['last', 'high', 'low']]
        dts = self.live_source.prev_n_trading_date(n=max_days)
        start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-(max_days * 1.5)) if len(dts) == 0 else min(dts).strftime('%Y-%m-%d')
        filt_ = {'trading_date': {'gte': start_dt}} if start_dt is not None else None
        df = pd.DataFrame()
        for e in self.exchange_list:
            mdd_ = self.base.read_dataframe(
                "pretreated_future_cn_md_data", f"all_1d_{e}",
                filter_datetime=filt_,
                filter_keyword={'contract': {'in': mains.values.tolist()}},
                filter_columns=['close', 'high', 'low', 'symbol', 'contract', 'trading_date']
            )
            df = pd.concat([df, mdd_], axis=0)
        res = pd.DataFrame(columns=['highs', 'lows'])
        for s, vdf in df.groupby('symbol'):
            if s in tick_df.index:
                vdf.set_index('trading_date', inplace=True)
                vdf = vdf.sort_index(ascending=False).drop(columns=['symbol'])
                vdf['cumhigh'] = vdf['high'].cummax()
                vdf['cumlow'] = vdf['low'].cummin()
                vdf['new_high'] = tick_df.loc[s, 'last'] > vdf['cumhigh']
                vdf['new_low'] = tick_df.loc[s, 'last'] < vdf['cumlow']
                rec_high = vdf[vdf.index > vdf[~vdf['new_high']].first_valid_index()].shape[0] if not vdf[~vdf['new_high']].empty else max_days
                rec_low = vdf[vdf.index > vdf[~vdf['new_low']].first_valid_index()].shape[0] if not vdf[~vdf['new_low']].empty else max_days
                res.loc[s] = pd.Series({'highs': rec_high, 'lows': rec_low})
            else:
                continue
        results = res['highs'] - res['lows']
        results = pd.DataFrame(results.rename('extreme_price_cnt')).assign(
            trading_date=self.live_source.this_trading_date,
            datetime=datetime.now().replace(second=0, microsecond=0)
        ).reset_index(names=['symbol'])
        return results

    def save_extreme_price_days_cnt(self, df: pd.DataFrame):
        self.origin.insert_dataframe(
            df,
            "origin_future_cn_model_data",
            "extreme_price_status_DIY",
            set_index=['symbol', 'datetime'],
            partition=['trading_date']
        )

    def __del_hist_extreme_price_cnt_archive__(self, record_date_max: int = 5):
        try:
            rec_dts = self.origin.read_dataframe(
                sql_str="select DISTINCT(`trading_date`) from origin_future_cn_model_data.extreme_price_status_DIY "
                        "order by `trading_date` DESC"
            )
        except:
            return
        if len(rec_dts) < record_date_max:
            return
        else:
            earliest_dt = rec_dts['trading_date'].iloc[record_date_max - 1]
            self.origin.del_row(
                db_name="origin_future_cn_model_data",
                tb_name="extreme_price_status_DIY",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    def extreme_position_days_count(self, max_days: int = 100):
        mains = self.all_mains
        contract_ls = ', '.join([f"'{i}'" for i in mains.values.tolist()])
        tick_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                SELECT DISTINCT 
                    `exchange`, `symbol`, `contract`, `datetime`, `open_interest` 
                    FROM origin_future_cn_md_data.all_tick_CTP
                    WHERE `contract` IN ({contract_ls})
                    AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                    AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                    AND (`contract`, `datetime`) IN (
                        SELECT `contract`, MAX(`datetime`)
                        FROM origin_future_cn_md_data.all_tick_CTP
                        WHERE `contract` IN ({contract_ls})
                        AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                        AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                        GROUP BY `contract`
                    )
            """
        )
        tick_df = tick_df.sort_values(by='datetime', ascending=True).drop_duplicates(subset=['contract'], keep='last')
        tick_df = tick_df.set_index('symbol')[['open_interest']]
        dts = self.live_source.prev_n_trading_date(n=max_days)
        start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-(max_days * 1.5)) if len(dts) == 0 else min(dts).strftime('%Y-%m-%d')
        filt_ = {'trading_date': {'gte': start_dt}} if start_dt is not None else None
        df = pd.DataFrame()
        for e in self.exchange_list:
            mdd_ = self.base.read_dataframe(
                "pretreated_future_cn_md_data", f"all_1d_{e}",
                filter_datetime=filt_,
                filter_keyword={'contract': {'in': mains.values.tolist()}},
                filter_columns=['open_interest', 'symbol', 'contract', 'trading_date']
            )
            df = pd.concat([df, mdd_], axis=0)
        res = pd.Series(dtype=int, name='extreme_position_cnt')

        for s, vdf in df.groupby('symbol'):
            if s in tick_df.index:
                vdf.set_index('trading_date', inplace=True)
                vdf = vdf.sort_index(ascending=False).drop(columns=['symbol'])
                vdf['cumhigh'] = vdf['open_interest'].cummax()
                vdf['new_high'] = tick_df.loc[s, 'open_interest'] > vdf['cumhigh']
                rec_high = vdf[vdf.index > vdf[~vdf['new_high']].first_valid_index()].shape[0] if not vdf[~vdf['new_high']].empty else max_days
                res.loc[s] = rec_high
            else:
                continue
        results = pd.DataFrame(res).assign(
            trading_date=self.live_source.this_trading_date,
            datetime=datetime.now().replace(second=0, microsecond=0)
        ).reset_index(names=['symbol'])
        return results

    def save_extreme_position_days_cnt(self, df: pd.DataFrame):
        self.origin.insert_dataframe(
            df,
            "origin_future_cn_model_data",
            "extreme_position_status_DIY",
            set_index=['symbol', 'datetime'],
            partition=['trading_date']
        )

    def __del_hist_extreme_position_cnt_archive__(self, record_date_max: int = 5):
        try:
            rec_dts = self.origin.read_dataframe(
                sql_str="select DISTINCT(`trading_date`) from origin_future_cn_model_data.extreme_position_status_DIY "
                        "order by `trading_date` DESC"
            )
        except:
            return
        if len(rec_dts) < record_date_max:
            return
        else:
            earliest_dt = rec_dts['trading_date'].iloc[record_date_max - 1]
            self.origin.del_row(
                db_name="origin_future_cn_model_data",
                tb_name="extreme_position_status_DIY",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    def run(self):
        while not self.rule.api_exit_signal(datetime.now()):
            und_df = self.read_dataframe_from_rds(k="target_underlying")
            dows_value = {u: self.run_dows_assessment(self.get_tick_data(u)) for u in und_df['underlying'].drop_duplicates().tolist()}
            res = pd.DataFrame.from_dict(dows_value).T
            self.save_dataframe_to_rds(k="underlying_dows_value", v=res)

            mkt_val_chg_df = self.mkt_val_chg_on_this_trading_date()
            self.save_dataframe_to_rds(k='mkt_val_chg_tod', v=mkt_val_chg_df)

            rm_status = self.get_all_rm_delta_from_recent_tick()
            if rm_status is not None:
                self.save_dataframe_to_rds(k='main_contract_rm_status', v=rm_status)

            extreme_price_days_cnt_result = self.extreme_price_days_count()
            self.save_extreme_price_days_cnt(extreme_price_days_cnt_result)

            extreme_position_days_cnt_result = self.extreme_position_days_count()
            self.save_extreme_position_days_cnt(extreme_position_days_cnt_result)

            os.system('cls')
            print(f"{self.__class__.__name__} update @ {datetime.now()}")
        else:
            logger.warning("Model exited outside trading time.")

    def this_mkt_val_chg(self):
        res = self.read_dataframe_from_rds(k='mkt_val_chg_tod')
        return res

    def analyse_future_targets(self, targets: pd.Series):
        sym_list = ', '.join([f"'{i}'" for i in targets.index.tolist()])
        tick_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                SELECT DISTINCT 
                    `exchange`, `symbol`, `contract`, `last`, 
                    `average_price`, `open_interest`, `volume`, `datetime`
                    FROM origin_future_cn_md_data.all_tick_CTP
                    WHERE `symbol` IN ({sym_list})
                    AND `contract` NOT LIKE '%-%'
                    AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                    AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                    AND (`contract`, `datetime`) IN (
                        SELECT `contract`, MAX(`datetime`)
                        FROM origin_future_cn_md_data.all_tick_CTP
                        WHERE `symbol` IN ({sym_list})
                        AND `contract` NOT LIKE '%-%'
                        AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                        AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0
                        GROUP BY `contract`
                    )
            """
        )
        tick_df = tick_df.sort_values(by='datetime', ascending=True).drop_duplicates(subset=['contract'], keep='last')
        # 取持仓量在同品种所有上市合约前30%的合约
        tick_df = tick_df.groupby('symbol').apply(lambda x: x[x['open_interest'] >= x['open_interest'].quantile(0.7)].copy())
        tick_df.set_index('symbol', drop=False, inplace=True)
        tick_df['multiplier'] = self.live_source.future_spec_info.loc[
            self.live_source.future_spec_info.index.intersection(tick_df.index)].reindex(tick_df.index)
        tick_df.reset_index(drop=True, inplace=True)
        tick_df['average_price'] = np.where(
            tick_df['exchange'] == 'CZCE',
            tick_df['average_price'],
            tick_df['average_price'] / tick_df['multiplier']
        )
        tick_df['mkt_val'] = tick_df['open_interest'] * tick_df['average_price'] * tick_df['multiplier']
        main_ls = tick_df.loc[tick_df.groupby('symbol')['open_interest'].idxmax()].set_index('symbol')['contract'].tolist()
        tick_df['code'] = tick_df['contract'].str.slice(-4)
        tick_df['data'] = tick_df['last'].astype(str) + '|' + tick_df['volume'].astype(str)

        tick_res = tick_df.pivot(index=['exchange', 'symbol'], columns='code', values='data').reset_index(drop=False).set_index('symbol')

        tick_res['X'] = targets.loc[targets.index.intersection(tick_res.index)].reindex(tick_res.index)
        tick_res.reset_index(drop=False, inplace=True)
        tick_res.insert(tick_res.columns.get_loc('exchange') + 1, 'X', tick_res.pop('X'))
        tick_res.insert(tick_res.columns.get_loc('X'), 'symbol', tick_res.pop('symbol'))
        return tick_res, main_ls

    def generate_call_put_iv_data(self, underlying_contract: str):
        current_df = self.live_source.get_tick_iv_data(underlying_contract, self.live_source.this_trading_date)
        if not current_df.empty:
            current_df = PretreatLocal.proc_daily_iv_range(current_df)
            current_dt = self.live_source.this_trading_date
        else:
            current_dt = None

        hist_df = self.base.read_dataframe(
            "pretreated_option_cn_md_data", "all_1d_iv_range_DIY",
            filter_keyword={'underlying_contract': {'eq': underlying_contract}},
            filter_datetime={'trading_date': {'gte': self.start.strftime('%Y-%m-%d')}}
        )
        if not hist_df.empty:
            hist_df = hist_df.drop(columns=['underlying_contract'])
            if current_dt is not None:
                hist_df = hist_df[hist_df['trading_date'] < current_dt].copy()
        res = pd.concat([current_df, hist_df], axis=0).sort_values(by='trading_date', ascending=True)
        return res

    # def get_oi_high_contracts(self, oi_limit: int = 1e4):
    #     res = self.live_source.realtime_data.get_stored(
    #         sql_str=f"""
    #             SELECT contract, open_interest
    #             FROM (
    #                 SELECT contract, MAX(open_interest) AS max_oi
    #                 FROM origin_future_cn_md_data.all_tick_CTP
    #                 GROUP BY contract
    #             ) AS max_oi_query
    #             JOIN (
    #                 SELECT contract, open_interest
    #                 FROM origin_future_cn_md_data.all_tick_CTP
    #                 WHERE (contract, datetime) IN (
    #                     SELECT contract, MAX(datetime)
    #                     FROM origin_future_cn_md_data.all_tick_CTP
    #                     GROUP BY contract
    #                 )
    #             ) AS current_oi_query
    #             ON max_oi_query.contract = current_oi_query.contract
    #             WHERE max_oi_query.max_oi <= current_oi_query.open_interest
    #             AND `contract` NOT LIKE '%-%' AND max_oi_query.max_oi >= {oi_limit}
    #         """
    #     )
    #     res['open_interest'] = 'L' + '-' + res['open_interest'].astype(str)
    #     sym_list = ', '.join([f"'{i}'" for i in res['contract'].str[:-4].drop_duplicates().tolist()])
    #     all_curr_oi_df = self.live_source.realtime_data.get_stored(
    #         sql_str=f"""
    #             SELECT `contract`, `open_interest`
    #             FROM origin_future_cn_md_data.all_tick_CTP
    #             WHERE (`contract`, `datetime`) IN (
    #                 SELECT `contract`, MAX(`datetime`)
    #                 FROM origin_future_cn_md_data.all_tick_CTP
    #                 GROUP BY contract
    #             )
    #             AND `contract` NOT LIKE '%-%'
    #         """
    #     )
    #     all_curr_oi_df = all_curr_oi_df.sort_values(by='open_interest', ascending=False).drop_duplicates(subset=['contract'], keep='last').set_index('contract')
    #     res = res.sort_values(by='open_interest', ascending=False).drop_duplicates(subset=['contract'], keep='last').set_index('contract')
    #     all_other_oi_df = all_curr_oi_df[~all_curr_oi_df.index.isin(res.index)].copy()
    #     res_df = pd.concat([res, all_other_oi_df], axis=0).sort_index()
    #
    #     oi_dow_df = self.read_dataframe_from_rds("underlying_dows_value")
    #     ls = oi_dow_df[(oi_dow_df['oi_top'] <= OI_TOP_REQ) & (oi_dow_df['oi_bot'] >= OI_BOT_REQ)].index.tolist()
    #
    #     res_df['open_interest'] = np.where(
    #         res_df.index.to_series().isin(ls),
    #         res_df['open_interest'].astype(str) + '-S',
    #         res_df['open_interest']
    #     )
    #     res_df.reset_index(drop=False, inplace=True)
    #
    #     res_df['symbol'] = res_df['contract'].str[:-4]
    #     res_df['contract'] = res_df['contract'].str[-4:]
    #     res_df['open_interest'] = res_df['open_interest'].astype(str)
    #
    #     res_df = res_df.pivot(index='symbol', columns='contract', values='open_interest')
    #     return res_df

    def get_extreme_price_cnt_data(self):
        df = self.origin.read_dataframe(
            db_name="origin_future_cn_model_data",
            tb_name="extreme_price_status_DIY",
        )
        df = df.groupby('symbol').apply(lambda x: x.drop_duplicates(subset=['datetime'], keep='last'))
        df['datetime'] = df['datetime'].dt.strftime('%m%d %H:%M')
        df = pd.pivot(df, columns='symbol', index='datetime', values='extreme_price_cnt')
        df.sort_index(axis=1, ascending=True, inplace=True)
        return df

    def get_extreme_position_cnt_data(self):
        df = self.origin.read_dataframe(
            db_name="origin_future_cn_model_data",
            tb_name="extreme_position_status_DIY",
        )
        df = df.groupby('symbol').apply(lambda x: x.drop_duplicates(subset=['datetime'], keep='last'))
        df['datetime'] = df['datetime'].dt.strftime('%m%d %H:%M')
        df = pd.pivot(df, columns='symbol', index='datetime', values='extreme_position_cnt')
        df.sort_index(axis=1, ascending=True, inplace=True)
        return df

    """全市场持仓手数加总 全市场持仓市值加总"""
    def hist_whole_market_size(self, hist_days: int = 20):
        prev_n_ls = self.live_source.prev_n_trading_date(hist_days)
        start_dt = None if len(prev_n_ls) == 0 else min(prev_n_ls)
        filt_ = None if start_dt is None else {'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}
        res_df = pd.DataFrame()
        for e in self.exchange_list:
            hist_df = self.base.read_dataframe(
                "pretreated_future_cn_md_data",
                f"all_1d_{e}",
                filter_datetime=filt_
            )
            if hist_df.empty:
                continue
            hist_df['mkt_cap'] = hist_df['open_interest'] * hist_df['close']
            daily_df = hist_df.groupby('trading_date')[['mkt_cap', 'open_interest', 'turnover']].sum()
            res_df = pd.concat([res_df, daily_df], axis=1)
        res_df = res_df.fillna(0).groupby(level=0, axis=1).sum()
        return res_df

    def live_whole_market_size(self):
        all_curr_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                SELECT `contract`, `open_interest`, `last`, `turnover`, `volume`, `average_price`, `symbol`, `exchange` 
                FROM origin_future_cn_md_data.all_tick_CTP
                WHERE (`contract`, `datetime`) IN (
                    SELECT `contract`, MAX(`datetime`)
                    FROM origin_future_cn_md_data.all_tick_CTP
                    GROUP BY contract
                )
                AND `contract` NOT LIKE '%-%'
            """
        )
        all_curr_df = all_curr_df.sort_values(by=['contract', 'open_interest'], ascending=True).drop_duplicates(subset=['contract'], keep='last')
        all_curr_df['mul'] = [self.live_source.future_spec_info.loc[i] for i in all_curr_df['symbol']]
        all_curr_df['average_price'] = np.where(
            all_curr_df['exchange'] == 'CZCE',
            all_curr_df['average_price'],
            all_curr_df['average_price'] / all_curr_df['mul']
        )
        all_curr_df['turnover'] = np.where(
            all_curr_df['turnover'].isna(), all_curr_df['volume'] * all_curr_df['average_price'], all_curr_df['turnover']
        )
        return {
            'mkt_cap': (all_curr_df['open_interest'] * all_curr_df['last']).sum(),
            'open_interest': all_curr_df['open_interest'].sum(),
            'turnover': all_curr_df['turnover'].sum()
        }

    """全市场持仓手数加总 全市场持仓市值加总"""
    def hist_sym_market_size(self, hist_days: int = 20):
        prev_n_ls = self.live_source.prev_n_trading_date(hist_days)
        start_dt = None if len(prev_n_ls) == 0 else min(prev_n_ls)
        filt_ = None if start_dt is None else {'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}
        res_df = pd.DataFrame()
        for e in self.exchange_list:
            hist_df = self.base.read_dataframe(
                "pretreated_future_cn_md_data",
                f"all_1d_{e}",
                filter_datetime=filt_
            )
            if hist_df.empty:
                continue
            hist_df['mkt_cap'] = hist_df['open_interest'] * hist_df['close']
            daily_df = hist_df.groupby(['symbol', 'trading_date'])[['mkt_cap', 'open_interest']].sum()
            res_df = pd.concat([res_df, daily_df], axis=0)
        return res_df

    def live_sym_market_size(self):
        all_curr_df = self.live_source.realtime_data.get_stored(
            sql_str=f"""
                    SELECT `contract`, `open_interest`, `last`, `symbol`, `trading_date`
                    FROM origin_future_cn_md_data.all_tick_CTP
                    WHERE (`contract`, `datetime`) IN (
                        SELECT `contract`, MAX(`datetime`)
                        FROM origin_future_cn_md_data.all_tick_CTP
                        GROUP BY contract
                    )
                    AND `contract` NOT LIKE '%-%'
                """
        )
        all_curr_df = all_curr_df.sort_values(by=['contract', 'open_interest'], ascending=True).drop_duplicates(
            subset=['contract'], keep='last')
        all_curr_df['mkt_cap'] = all_curr_df['open_interest'] * all_curr_df['last']
        res_df = all_curr_df.groupby(['symbol', 'trading_date'])[['mkt_cap', 'open_interest']].sum()
        return res_df

    def iv_pos(self):
        rdf = self.read_dataframe_from_rds('raw_options')
        rdf['direction'] = rdf['contract'].str.split('-').str.get(1)
        cur_df = rdf.groupby(['underlying', 'direction'])[['iv']].mean().reset_index(drop=False).pivot(index='underlying', columns='direction', values='iv')
        cur_df = cur_df / 100
        cur_df = cur_df.assign(symbol=cur_df.index.str[:-4]).groupby('symbol')[['C', 'P']].mean().rename(
            columns={
                'C': 'call_avg',
                'P': 'put_avg'
            }
        ).reset_index(drop=False).assign(trading_date=self.live_source.this_trading_date)

        hist_df = self.hist_iv_range_data
        if hist_df.empty:
            return
        hist_df = hist_df.assign(symbol=hist_df['underlying_contract'].str[:-4]).groupby(['trading_date', 'symbol'])[['call_avg', 'put_avg']].mean().reset_index(drop=False)

        res_df = pd.concat([hist_df, cur_df], axis=0)

        res = pd.DataFrame()
        for s, v in res_df.groupby('symbol'):
            v = v.drop_duplicates(subset=['trading_date'], keep='last').set_index('trading_date').drop(columns=['symbol'])
            r = (v.loc[v.index.max()] - v.min()) / (v.max() - v.min()) * 100
            r = pd.DataFrame(r, columns=[s]).T
            res = pd.concat([res, r], axis=0)
        return res


if __name__ == '__main__':
    m = MdAssess()
    m.mkt_val_chg_on_this_trading_date()