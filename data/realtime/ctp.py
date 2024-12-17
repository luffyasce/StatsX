import traceback
import json
import pandas as pd
import numpy as np
from typing import Union, Iterable, Any
from operator import itemgetter
from datetime import datetime, timedelta
from infra.trade.service.ctp.market_data import BaseCTPMd
from utils.buffer.redis_handle import RedisMsg
from utils.database.unified_db_control import UnifiedControl
from utils.tool.beep import Beep
from utils.tool.decorator import try_catch
from utils.tool.logger import log
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = log(__file__, "data", warning_only=True)


class RealTimeCTPDownload:
    def __init__(self, broker: str, channel: str):
        self.broker = broker
        self.channel = channel
        self.ctp_md = BaseCTPMd(broker, channel).md_handle
        self.udc = UnifiedControl(db_type='base')
        self.origin = UnifiedControl(db_type='origin')
        conf = Config()
        self.exchange_ls = conf.exchange_list

    @property
    def future_instruments(self):
        rdf = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_future_cn_meta_data", f"contract_info_{e}",
                filter_datetime={'last_trading_date': {'gte': datetime.now().strftime("%Y-%m-%d")}}
            )
            rdf = pd.concat([rdf, df], axis=0)
        return rdf

    @property
    def option_instruments(self):
        rdf = pd.DataFrame()
        for e in self.exchange_ls:
            df = self.udc.read_dataframe(
                "processed_option_cn_meta_data", f"contract_info_{e}",
                filter_datetime={'last_trading_date': {'gte': datetime.now().strftime("%Y-%m-%d")}}
            )
            rdf = pd.concat([rdf, df], axis=0)
        return rdf

    def sort_out_instrument_ls(self, derivatives_include: bool):
        instrument_df = self.future_instruments
        if derivatives_include:
            option_df: pd.DataFrame = self.option_instruments
            instrument_list = pd.concat(
                [
                    instrument_df[['contract', 'exchange']],
                    option_df[['contract', 'exchange']]
                ], axis=0
            ).sort_values(by='exchange').to_numpy().tolist()
        else:
            instrument_list = instrument_df[['contract', 'exchange']].sort_values(by='exchange').to_numpy().tolist()
        return instrument_list

    @staticmethod
    def proc_raw_md_data(reserve_df: pd.DataFrame, contract_df: pd.DataFrame, trading_dt, act_dt):
        reserve_df[['contract', 'exchange']] = contract_df.loc[
            contract_df.index.intersection(reserve_df.index)
        ]
        cond = reserve_df[
                   reserve_df.dtypes[
                       reserve_df.dtypes.astype(str).isin(['float64', 'int64'])].index.tolist()
               ] > 1e10
        reserve_df[
            reserve_df.dtypes[reserve_df.dtypes.astype(str).isin(['float64', 'int64'])].index.tolist()
        ] = reserve_df[
            reserve_df.dtypes[reserve_df.dtypes.astype(str).isin(['float64', 'int64'])].index.tolist()
        ].mask(cond, np.nan)
        reserve_df['TradingDay'] = pd.to_datetime(trading_dt)
        reserve_df['ActionDay'] = pd.to_datetime(act_dt)
        reserve_df['datetime'] = pd.to_datetime(
            reserve_df['ActionDay'].astype(str) +
            " " +
            reserve_df['UpdateTime'] +
            "." +
            reserve_df['UpdateMillisec'].astype(str)
        )
        reserve_df['datetime_minute'] = pd.to_datetime(
            reserve_df['ActionDay'].astype(str) +
            " " +
            reserve_df['UpdateTime']
        ).dt.ceil("1min")
        reserve_df = reserve_df[
            (reserve_df['datetime'] <= (datetime.now() + timedelta(minutes=2))) &
            (reserve_df['datetime'] >= (datetime.now() + timedelta(minutes=-2)))
            ].copy()
        reserve_df['symbol'] = reserve_df['contract'].apply(
            lambda x: x[:-4] if '-' not in x else x.split('-')[0][:-4]
        )
        return reserve_df

    @staticmethod
    def pretreat_md(raw_df: pd.DataFrame):
        return raw_df.drop(
            columns=[
                'ExchangeID', 'ExchangeInstID', 'PreDelta', 'CurrDelta',
                'UpdateTime', 'UpdateMillisec', 'ActionDay'
            ]
        ).rename(
            columns={
                'TradingDay': 'trading_date',
                'InstrumentID': 'ctp_contract',
                'LastPrice': 'last',
                'PreSettlementPrice': 'pre_settlement',
                'PreClosePrice': 'pre_close',
                'PreOpenInterest': 'pre_open_interest',
                'OpenPrice': 'open',
                'HighestPrice': 'high',
                'LowestPrice': 'low',
                'Volume': 'volume',
                'Turnover': 'turnover',
                'OpenInterest': 'open_interest',
                'ClosePrice': 'close',
                'SettlementPrice': 'settlement',
                'UpperLimitPrice': 'limit_up',
                'LowerLimitPrice': 'limit_down',
                'BidPrice1': 'bid1',
                'AskPrice1': 'ask1',
                'BidVolume1': 'bid_vol1',
                'AskVolume1': 'ask_vol1',
                'AveragePrice': 'average_price',

            }
        ).set_index('ctp_contract', drop=False)

    @staticmethod
    def broadcasting(data: pd.DataFrame, start: bool, broadcast_handle):
        if not start:
            return
        else:
            data[['trading_date', 'datetime', 'datetime_minute']] = data[['trading_date', 'datetime', 'datetime_minute']].astype(str)
            return broadcast_handle(json.dumps(data.to_dict(orient='index')))

    @property
    def rec_dates(self) -> pd.DataFrame:
        record_dt = self.origin.read_dataframe(
            db_name="origin_future_cn_md_data",
            tb_name="all_tick_CTP",
            sql_str="select DISTINCT(`trading_date`) from origin_future_cn_md_data.all_tick_CTP"
        )
        return record_dt

    @try_catch(suppress_traceback=True, catch_args=True)
    def clear_hist_data(self, current_t_date, days_limit: int):
        rec_dts = pd.concat(
            [self.rec_dates['trading_date'], pd.Series([current_t_date])]
        ).drop_duplicates()
        if len(rec_dts) > days_limit:
            earliest_dt = rec_dts.sort_values(ascending=False).iloc[days_limit-1]
            self.origin.del_row(
                db_name="origin_future_cn_md_data",
                tb_name="all_tick_CTP",
                filter_datetime={'trading_date': {'lt': earliest_dt.strftime('%Y-%m-%d')}}
            )

    def realtime_data_download(
            self,
            derivatives_include: bool = False,
            broadcast: bool = False,
            limit_md_history: int = 0
    ):
        with self.ctp_md as handle:
            instrument_list = self.sort_out_instrument_ls(derivatives_include)

            instrument_list_ctp_format = [
                handle.trade_rule.standard_contract_to_trade_code(*i) for i in instrument_list
            ]
            contract_dict = {
                handle.trade_rule.standard_contract_to_trade_code(*i): i for i in instrument_list
            }
            contract_df = pd.DataFrame.from_dict(contract_dict, orient='index').rename(
                columns={0: 'contract', 1: 'exchange'})

            handle.subscribe_md(instrument_list_ctp_format)
            if limit_md_history > 0:
                self.clear_hist_data(handle.get_current_trading_date(), limit_md_history)

            while not handle.trade_rule.api_exit_signal(datetime.now()):
                real_trading_date = handle.get_current_trading_date()
                real_action_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                data = handle.md_spi.latest_md_buf.copy()
                reserve_df = pd.DataFrame.from_dict(data, orient='index')
                if reserve_df.empty:
                    pass
                else:
                    try:
                        raw_df = self.proc_raw_md_data(reserve_df, contract_df, real_trading_date, real_action_date)
                        pretreated_df = self.pretreat_md(raw_df)
                        self.origin.insert_dataframe(
                            pretreated_df,
                            "origin_future_cn_md_data", f"all_tick_CTP",
                            set_index=['contract', 'exchange', 'datetime'],
                            partition=['trading_date']
                        )
                        sub_num = self.broadcasting(pretreated_df, broadcast, handle.msg.pub)
                        print(
                            f"\r{self.broker}-{self.channel} MD update: {len(pretreated_df)} "
                            f"@ {pretreated_df['datetime'].max()}. "
                            f"--- {datetime.now()} -- {sub_num} subscribers.",
                            end="", flush=True
                        )
                    except Exception as err:
                        logger.error(str(traceback.format_exc()))
                        Beep.emergency()
                    else:
                        pass
            else:
                print(
                    f"\r{self.broker}-{self.channel} "
                    f"Conn Expired -- {datetime.now()} -- API VER {handle.ctp_ver()}",
                    end="", flush=True
                )


class RealTimeCTPRecv:
    """
    Ways to get realtime data from CTP
        1. read realtime stored tick data from clickhouse
        2. read realtime pub msg from redis channel
        3. sub to pub, and yield
    """
    def __init__(self, redis_db: int = None, redis_channel: str = ""):
        self.db = UnifiedControl(db_type='origin')
        if redis_db is not None:
            self.msg = RedisMsg(redis_db, redis_channel)
        else:
            self.msg = None

    def md_iter(self):
        for dat in self.msg.sub():
            res_ = dat.get("data", b'{}')
            if isinstance(res_, int):
                pass
            else:
                yield json.loads(res_)

    def get_stored(self, **kwargs) -> pd.DataFrame:
        """
        get realtime data from storage database.
        :param kwargs:
        :return:
        """
        return self.db.read_dataframe(
            **kwargs
        )

    def get_msg(self, contract: Union[str, Iterable, None]) -> Any:
        if self.msg is None:
            raise AttributeError(f"Need param to initialize MQ.")
        if isinstance(contract, str):
            while True:
                data_ = self.md_iter().__next__()
                if contract in data_.keys():
                    return data_.get(contract)
        elif isinstance(contract, Iterable):
            while True:
                data_ = self.md_iter().__next__()
                contract = [c for c in contract if c in data_.keys()]
                if len(contract) > 0:
                    return itemgetter(*contract)(data_)
        else:
            while True:
                data_ = self.md_iter().__next__()
                return data_

    def yield_msg(self, contract: Union[str, Iterable, None]) -> Any:
        if self.msg is None:
            raise AttributeError(f"Need param to initialize MQ.")
        if isinstance(contract, str):
            for data_ in self.md_iter():
                if contract in data_.keys():
                    yield data_.get(contract)
        elif isinstance(contract, Iterable):
            for data_ in self.md_iter():
                contract = [c for c in contract if c in data_.keys()]
                yield data_.get(contract)
        else:
            for data_ in self.md_iter():
                yield data_


if __name__ == "__main__":
    downloader = RealTimeCTPDownload("HAQH", "haqh")
    downloader.realtime_data_download(
        derivatives_include=True,
        broadcast=False,
    )