import time
import pandas as pd
import numpy as np
from typing import Union, Tuple
from datetime import datetime, timedelta
from time import sleep
from infra.trade.service.ctp.market_data import BaseCTPMd
from data.realtime.ctp import RealTimeCTPRecv
from utils.database.unified_db_control import UnifiedControl
from infra.data.service.data_client import DataClient
from infra.tool.rules import TradeRules
from utils.tool.configer import Config


class MDO:
    def __init__(self):
        config = Config()
        trade_conf = config.get_trade_conf
        md_broker = trade_conf.get("LIVESETTINGS", "live_md_broker")
        md_channel = trade_conf.get("LIVESETTINGS", "live_md_channel_code")

        self.udc = UnifiedControl(db_type='base')

        self.rules = TradeRules()

        self.__CtpMdCls__ = BaseCTPMd(broker_name=md_broker, channel=md_channel)
        self.realtime_data = RealTimeCTPRecv()
        self.exchange_ls = config.exchange_list

    @property
    def this_trading_date(self):
        return self.__CtpMdCls__.md_handle.get_current_trading_date()

    @property
    def option_spec_info(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_option_cn_meta_data", f"spec_info_{e}"
            )
            res_df = pd.concat([res_df, df.set_index('symbol')[['trade_unit']]])
        return res_df['trade_unit']

    @property
    def future_spec_info(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_future_cn_meta_data", f"spec_info_{e}"
            )
            res_df = pd.concat([res_df, df.set_index('symbol')[['trade_unit']]])
        return res_df['trade_unit']

    @property
    def option_contract(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_option_cn_meta_data", f"contract_info_{e}",
                filter_datetime={'last_trading_date': {'gte': datetime.now().strftime("%Y-%m-%d")}}
            )
            res_df = pd.concat([res_df, df])
        if res_df.empty:
            return res_df
        else:
            res_df = res_df.set_index('contract')
            res_df['curr'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            res_df = res_df.assign(
                days_before_expire=(res_df['last_trading_date'] - res_df['curr']).dt.days
            ).drop(columns=['curr'])
        return res_df

    @property
    def future_contract(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_future_cn_meta_data", f"contract_info_{e}",
                filter_datetime={'last_trading_date': {'gte': datetime.now().strftime("%Y-%m-%d")}}
            )
            res_df = pd.concat([res_df, df])
        if res_df.empty:
            return res_df
        else:
            res_df = res_df.set_index('contract')
            res_df['curr'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            res_df = res_df.assign(
                days_before_expire=(res_df['last_trading_date'] - res_df['curr']).dt.days
            ).drop(columns=['curr'])
        return res_df

    @property
    def hv(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            tdf = self.udc.read_dataframe(
                "processed_future_cn_trade_data", f"historical_volatility_{e}",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            if tdf.empty:
                continue
            else:
                t_date = tdf.iloc[0]['trading_date']
                df = self.udc.read_dataframe(
                    "processed_future_cn_trade_data", f"historical_volatility_{e}",
                    filter_datetime={"trading_date": {'eq': t_date.strftime("%Y-%m-%d")}},
                )
                res_df = pd.concat([res_df, df])
        if not res_df.empty:
            res_df = res_df.set_index('contract').sort_index()
        return res_df

    def init_live_source_data(self):
        while not self.rules.api_exit_signal(datetime.now()):
            cur_dt = (datetime.now() + timedelta(minutes=-1)).strftime("%Y-%m-%d %H:%M:%S")

            realtime_md = self.realtime_data.get_stored(
                sql_str=f"select * "
                        f"from origin_future_cn_md_data.all_tick_CTP "
                        f"where `datetime_minute` >= '{cur_dt}' "
                        f"and `ask_vol1` > 0 and `bid_vol1` > 0 and `open` > 0 and `high` > 0 and `low` > 0 "
                        f"and `last` > 0 and `open_interest` > 0 and `volume` > 0"
            )
            if realtime_md.empty:
                sleep(15)
                continue

            realtime_md = realtime_md.groupby('contract').apply(
                lambda x: x.sort_values(by='datetime', ascending=False).iloc[:1]
            ).reset_index(drop=True)
            yield realtime_md
        else:
            print("API exited outside trading time.")

    def md_snapshot(self):
        for source in self.init_live_source_data():
            source.set_index('symbol', drop=False, inplace=True)
            source = source.sort_values(by='datetime', ascending=False).drop_duplicates(subset=['contract'], keep='first')
            source['fut_mul'] = self.future_spec_info.loc[
                self.future_spec_info.index.intersection(source.index)
            ].reindex(source.index)
            source['opt_mul'] = self.option_spec_info.loc[
                self.option_spec_info.index.intersection(source.index)
            ].reindex(source.index)
            source['multiplier'] = np.where(
                source['contract'].str.contains('-'),
                source['opt_mul'],
                source['fut_mul']
            )
            source.reset_index(drop=True, inplace=True)

            source['average_price'] = np.where(
                source['exchange'] == 'CZCE',
                source['average_price'],
                source['average_price'] / source['multiplier']
            )
            yield source

    def prev_trade_date(self, prev_n: int):
        trading_date_str = self.this_trading_date.strftime('%Y-%m-%d')
        dt_df = self.realtime_data.get_stored(
            sql_str=f"select DISTINCT(`trading_date`) "
                    f"from origin_future_cn_md_data.all_tick_CTP "
                    f"where `trading_date` < '{trading_date_str}' order by `trading_date` desc"
        )
        if dt_df.empty or len(dt_df) < prev_n - 1:
            return None
        else:
            return dt_df.iloc[prev_n-1]['trading_date']

    def min_trade_date(self):
        df = self.realtime_data.get_stored(
            sql_str=f"select MIN(`trading_date`) "
                    f"from origin_future_cn_md_data.all_tick_CTP "
        )
        if df.empty:
            return
        else:
            return df.iloc[0, 0]

    def tick_md_data(self, contract: str):
        md_df = self.realtime_data.get_stored(
            sql_str=f"select * "
                    f"from origin_future_cn_md_data.all_tick_CTP "
                    f"where `contract` == '{contract}' "
                    f"and `ask_vol1` > 0 and `bid_vol1` > 0 and `open` > 0 and `high` > 0 and `low` > 0 "
                    f"and `last` > 0 and `open_interest` > 0 and `volume` > 0"
        )
        if md_df.empty:
            return md_df
        else:
            md_df = md_df.sort_values(by=['datetime', 'volume'], ascending=True).drop_duplicates(subset=['datetime'], keep='last')
            return md_df

    def derivatives_tick_md_data(self, underlying_contract: str):
        md_df = self.realtime_data.get_stored(
            sql_str=f"select * "
                    f"from origin_future_cn_md_data.all_tick_CTP "
                    f"where `contract` LIKE '%{underlying_contract}-%' "
                    f"and `ask_vol1` > 0 and `bid_vol1` > 0 and `open` > 0 and `high` > 0 and `low` > 0 "
                    f"and `last` > 0 and `open_interest` > 0 and `volume` > 0"
        )
        if md_df.empty:
            return md_df
        else:
            md_df = md_df.sort_values(by=['datetime', 'volume'], ascending=True).drop_duplicates(subset=['datetime'], keep='last')
            return md_df

    def big_orders(self, dt: datetime = None, contract: str = None, contract_list: list = None):
        """
        对于big_orders的使用：
        1. 要么就按照多开、多平、空开、空平区分筛选后使用real money，这样可以用来计算多头走势和空头走势
        2. 要么就按照对价格的推进作用（即做涨/做跌的所有real money的绝对值相加，而不是带符号的real money相加），计算做涨做跌相对比值
        不可以按照对价格的推进作用而把带符号的real money累加，因为这样你累加的是多开的oi和空平的oi的仓差，这个数据没有意义
        3. 唯一有意义的是不考虑价格，只计算对应价格位置所有的大单集合，这时才可以用带符号的real money
        """
        filter_contract = {'contract': {'eq': contract}} if contract is not None else None
        if contract is None and contract_list is not None:
            filter_contract = {'contract': {'in': contract_list}}
        filter_dt = {'trading_date': {'gte': dt.strftime('%Y-%m-%d')}} if dt is not None else None
        bdf = self.realtime_data.get_stored(
            db_name="origin_future_cn_model_data", tb_name="filtered_orders_DIY",
            filter_datetime=filter_dt,
            filter_keyword=filter_contract,
        )
        return bdf

    def archived_orders(self, dt: datetime = None, contract: str = None):
        filter_contract = {'contract': {'eq': contract}} if contract is not None else None
        filter_dt = {'trading_date': {'gte': dt.strftime('%Y-%m-%d')}} if dt is not None else None
        adf = self.realtime_data.get_stored(
            db_name="origin_future_cn_model_data", tb_name="archived_orders_DIY",
            filter_datetime=filter_dt,
            filter_keyword=filter_contract,
        )
        return adf

    def prev_n_trading_date(self, n: int):
        tdf = self.udc.read_dataframe(
            "processed_future_cn_meta_data",
            "hist_trading_date_DIY",
            filter_datetime={'trading_date': {'lte': self.this_trading_date.strftime('%Y-%m-%d')}},
            ascending=[('trading_date', False)],
            filter_row_limit=n
        )
        ls = [] if tdf.empty else tdf['trading_date'].sort_values(ascending=False).tolist()
        return ls

    def get_exchange_info(self, symbol: str = None, contract: str = None):
        filt_ = f"`symbol` == '{symbol}'" if symbol is not None else f"`contract` == '{contract}'"
        sql_line = f"select `exchange` from origin_future_cn_md_data.all_tick_CTP where {filt_} limit 1"
        df = self.realtime_data.get_stored(
            sql_str=sql_line
        )
        if df.empty:
            return None
        else:
            return df.iloc[0]['exchange']

    def get_tick_compressed_data(self, trading_date: datetime):
        this_trading_date = trading_date.strftime("%Y-%m-%d")
        sql_line = f"""
            SELECT DISTINCT
                earliest.datetime AS earliest_datetime,
                latest.datetime AS latest_datetime,
                earliest.trading_date,
                earliest.symbol,
                earliest.contract,
                earliest.exchange,
                earliest.open_interest AS earliest_open_interest,
                latest.open_interest AS latest_open_interest,
                earliest.average_price AS earliest_average_price,
                latest.average_price AS latest_average_price,
                latest.last AS latest_last_price
            FROM 
                (SELECT datetime, trading_date, symbol, contract, open_interest, average_price, exchange, last
                 FROM origin_future_cn_md_data.all_tick_CTP
                 WHERE (contract, datetime) IN 
                     (SELECT contract, min(datetime) 
                      FROM origin_future_cn_md_data.all_tick_CTP
                      WHERE trading_date = '{this_trading_date}' 
                      AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                      AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0 
                      GROUP BY contract)) AS earliest
            JOIN 
                (SELECT datetime, trading_date, symbol, contract, open_interest, average_price, exchange, last
                 FROM origin_future_cn_md_data.all_tick_CTP
                 WHERE (contract, datetime) IN 
                     (SELECT contract, max(datetime) 
                      FROM origin_future_cn_md_data.all_tick_CTP
                      WHERE trading_date = '{this_trading_date}' 
                      AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                      AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0 
                      GROUP BY contract)) AS latest
            ON earliest.contract = latest.contract
        """
        df = self.realtime_data.get_stored(
            sql_str=sql_line
        )
        if df.empty:
            return
        opt_df = df[df['contract'].str.contains('-')].copy()
        opt_df.set_index('symbol', drop=False, inplace=True)
        opt_df['multiplier'] = self.option_spec_info.loc[self.option_spec_info.index.intersection(opt_df.index)].reindex(opt_df.index)
        opt_df.reset_index(drop=True, inplace=True)
        opt_df['earliest_average_price'] = np.where(
            opt_df['exchange'] == 'CZCE',
            opt_df['earliest_average_price'],
            opt_df['earliest_average_price'] / opt_df['multiplier']
        )
        opt_df['latest_average_price'] = np.where(
            opt_df['exchange'] == 'CZCE',
            opt_df['latest_average_price'],
            opt_df['latest_average_price'] / opt_df['multiplier']
        )
        opt_df['mkt_val_chg'] = (opt_df['latest_open_interest'] * opt_df['latest_average_price'] - opt_df['earliest_open_interest'] * opt_df['earliest_average_price']) * opt_df['multiplier']
        call_df = opt_df[opt_df['contract'].str.contains('-C-')].copy()
        put_df = opt_df[opt_df['contract'].str.contains('-P-')].copy()

        fut_df = df[~df['contract'].str.contains('-')].copy()
        fut_df.set_index('symbol', drop=False, inplace=True)
        fut_df['multiplier'] = self.future_spec_info.loc[self.future_spec_info.index.intersection(fut_df.index)].reindex(fut_df.index)
        fut_df.reset_index(drop=True, inplace=True)
        fut_df['earliest_average_price'] = np.where(
            fut_df['exchange'] == 'CZCE',
            fut_df['earliest_average_price'],
            fut_df['earliest_average_price'] / fut_df['multiplier']
        )
        fut_df['latest_average_price'] = np.where(
            fut_df['exchange'] == 'CZCE',
            fut_df['latest_average_price'],
            fut_df['latest_average_price'] / fut_df['multiplier']
        )
        fut_df['mkt_val_chg'] = (fut_df['latest_open_interest'] * fut_df['latest_average_price'] - fut_df['earliest_open_interest'] * fut_df['earliest_average_price']) * fut_df['multiplier']

        return fut_df, call_df, put_df

    def get_bulk_tick_iv_data(self, current_trading_date: datetime):
        df = self.realtime_data.get_stored(
            db_name="origin_future_cn_md_data",
            tb_name="iv_records_by_option_contract_DIY",
            filter_datetime={'trading_date': {'eq': current_trading_date.strftime('%Y-%m-%d')}}
        )
        max_dt_minute = df['datetime_minute'].max()
        df = df[df['datetime_minute'] == max_dt_minute].copy().sort_values(by='datetime', ascending=True).drop_duplicates(subset=['contract'], keep='last')
        return df

    def get_tick_iv_data(self, underlying: str, current_trading_date: datetime):
        df = self.realtime_data.get_stored(
            db_name="origin_future_cn_md_data",
            tb_name="iv_records_by_option_contract_DIY",
            filter_keyword={'underlying': {'eq': underlying}},
            filter_datetime={'trading_date': {'eq': current_trading_date.strftime('%Y-%m-%d')}}
        )
        return df

    def get_tick_iv_data_by_option_contract(self, option_contract: str, start_date: datetime):
        df = self.realtime_data.get_stored(
            db_name="origin_future_cn_md_data",
            tb_name="iv_records_by_option_contract_DIY",
            filter_keyword={'contract': {'eq': option_contract}},
            filter_datetime={'trading_date': {'gte': start_date.strftime('%Y-%m-%d')}}
        )
        df = df.groupby('datetime_minute')['iv'].median()
        return df

    def recent_md_snap(self):
        cur_dt = (datetime.now() + timedelta(minutes=-35)).strftime("%Y-%m-%d %H:%M:%S")

        df = self.realtime_data.get_stored(
            sql_str=f"""
                SELECT `contract`, `open_interest`, `volume`, `datetime`
                FROM origin_future_cn_md_data.all_tick_CTP
                WHERE (`contract`, `datetime`) IN (
                    SELECT `contract`, MAX(`datetime`)
                    FROM origin_future_cn_md_data.all_tick_CTP
                    WHERE `datetime_minute` >= '{cur_dt}' 
                    GROUP BY contract
                )
                AND `contract` NOT LIKE '%-%'
            """
        )
        df = df.sort_values(by='datetime').drop_duplicates(subset=['contract'], keep='last')
        return df

    def get_1min_md_data_by_contract(self, contract: str):
        df = self.realtime_data.get_stored(
            sql_str=f"""
                SELECT *
                FROM (
                    SELECT
                        `contract`, `datetime_minute`, `last`, `volume`, `open_interest`, `datetime`, `trading_date`,
                        ROW_NUMBER() OVER (PARTITION BY toStartOfMinute(datetime) ORDER BY datetime DESC) AS rn
                        FROM origin_future_cn_md_data.all_tick_CTP
                        WHERE contract = '{contract}'
                        AND `ask_vol1` > 0 AND `bid_vol1` > 0 AND `open` > 0 AND `high` > 0 AND `low` > 0 
                        AND `last` > 0 AND `open_interest` > 0 AND `volume` > 0 AND `average_price` > 0 
                ) AS subquery
                WHERE rn = 1
                ORDER BY datetime ASC
            """
        )
        if df.empty:
            return df
        else:
            df.sort_values(by='datetime_minute', inplace=True, ascending=True)
            df = df.drop(columns=['rn', 'datetime'])
            df.set_index('datetime_minute', inplace=True)
            new_vol = pd.Series(dtype=float)
            for t, v in df.groupby('trading_date'):
                raw_vol = v['volume']
                vol_x = v['volume'].diff()
                vol_x[0] = raw_vol[0]
                new_vol = pd.concat([new_vol, vol_x])
            df['volume'] = new_vol
            df.reset_index(drop=False, inplace=True)
            return df
