import random
import re
import json
import traceback
import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, timedelta
import requests.exceptions
from lxml import etree
from utils.crawler.crawler_base import Crawler
from utils.database.unified_db_control import UnifiedControl
from utils.tool.decorator import try_catch
from utils.tool.datetime_wrangle import yield_dates
from utils.tool.configer import Config
from utils.tool.logger import log


logger = log(__file__, 'data')

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class CrawlerGFEX(Crawler):
    def __init__(self):
        super().__init__()
        self.base = UnifiedControl(db_type='base')
        self.conf = Config()

    """ CONTRACT INFO """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_contract_info(self, data_type: str):
        api_ = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"
        ty_ = '1' if data_type == 'option' else '0'
        data = {
            "variety": '',
            "trade_type": ty_,
        }
        resp = self.request_url(api_, "POST", data=data)
        res = json.loads(resp)
        data = res.get('data', {})
        res_df = pd.DataFrame().from_dict(data)
        res_df['startTradeDate'] = res_df['startTradeDate'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
        res_df['endTradeDate'] = res_df['endTradeDate'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
        res_df['endDeliveryDate0'] = res_df['endDeliveryDate0'].replace('-', None).apply(lambda x: datetime.strptime(x, "%Y%m%d") if x else None)
        return res_df

    def save_contract_info(self, df: pd.DataFrame, type_name: str):
        self.base.insert_dataframe(
            df, f"raw_{type_name}_cn_meta_data", "contract_info_GFEX",
            set_index=['contractId', 'startTradeDate'], partition=['startTradeDate']
        )

    def download_all_contract_info(self):
        df_f = self.download_contract_info("future")
        self.save_contract_info(df_f, "future")
        df_o = self.download_contract_info('option')
        self.save_contract_info(df_o, 'option')

    """ MD DATA """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_md_data(self, trading_date: datetime, data_type: str):
        dt_str = trading_date.strftime('%Y%m%d')
        ty_ = '1' if data_type == 'option' else '0'
        api_ = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"
        data = {
            "trade_date": dt_str,
            "trade_type": ty_
        }
        header = {
            'Referer': 'http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml'
        }
        resp = self.request_url(api_, "POST", data=data, header=header)
        res = json.loads(resp)
        data = res.get('data', {})
        res_df = pd.DataFrame().from_dict(data)
        res_df = res_df[~res_df['variety'].str.contains('小计|总计')].copy()
        if res_df.empty:
            return res_df
        res_df['contract'] = res_df.apply(lambda row: row['delivMonth'] if row['delivMonth'].find(row['varietyOrder']) != -1 else row['varietyOrder'] + row['delivMonth'], axis=1)
        res_df['trading_date'] = trading_date
        res_df['datetime'] = trading_date
        return res_df

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_underlying_iv_data(self, trading_date: datetime):
        dt_str = trading_date.strftime('%Y%m%d')
        api_ = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadListOptVolatility"
        data = {
            "trade_date": dt_str,
        }
        header = {
            'Referer': 'http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml'
        }
        resp = self.request_url(api_, "POST", data=data, header=header)
        res = json.loads(resp)
        data = res.get('data', {})
        res_df = pd.DataFrame().from_dict(data)
        res_df['trading_date'] = trading_date
        res_df['datetime'] = trading_date
        return res_df

    def save_daily_md_data(self, md_df, data_type: str):
        self.base.insert_dataframe(
            md_df, f"raw_{data_type}_cn_md_data", "all_1d_GFEX",
            set_index=['contract', 'datetime'], partition=['trading_date']
        )

    def save_daily_underlying_iv_data(self, iv_df):
        self.base.insert_dataframe(
            iv_df, "raw_option_cn_md_data", "all_1d_iv_GFEX",
            set_index=['seriesId', 'datetime'], partition=['trading_date']
        )

    def download_all_daily_md_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_md_data", "all_1d_GFEX",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_md_data", "all_1d_GFEX",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        start_datetime = min(last_df_opt, last_df_fut)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            df_f = self.download_daily_md_data(t, "future")
            self.save_daily_md_data(df_f, "future")
            df_o = self.download_daily_md_data(t, "option")
            self.save_daily_md_data(df_o, "option")
            iv_o = self.download_daily_underlying_iv_data(t)
            self.save_daily_underlying_iv_data(iv_o)
            sec = random.randint(3, 10)
            sleep(sec)
            print(f"Done fetching md data on {t}. Wait {sec} seconds before next query.")

    """ POSITION RANKING DATA"""
    @try_catch(suppress_traceback=True, catch_args=True)
    def load_contract_id(self, symbol: str, trading_date: datetime):
        dt = trading_date.strftime('%Y%m%d')
        symbol = symbol.lower()
        api_ = "http://www.gfex.com.cn/u/interfacesWebTiMemberDealPosiQuotes/loadListContract_id"
        data = {
            "variety": symbol,
            "trade_date": dt
        }
        header = {
            'Referer': 'http://www.gfex.com.cn/gfex/rcjccpm/hqsj_tjsj.shtml'
        }
        resp = self.request_url(api_, "POST", data=data, header=header)
        res = json.loads(resp)
        data = res.get('data', [])
        return data

    @try_catch(suppress_traceback=True, catch_args=True)
    def __download_position_rank_data_by_data_type__(self, date: datetime, symbol: str, contract: str, trade_type: str, data_type: str):
        trade_type_code_ref = {
            'future': '0',
            'option': '1'
        }
        data_type_code_ref = {
            'vol': '1',
            'buy': '2',
            'sell': '3',
        }
        api_ = "http://www.gfex.com.cn/u/interfacesWebTiMemberDealPosiQuotes/loadList"
        dt = date.strftime('%Y%m%d')
        symbol = symbol.lower()
        contract = contract.lower()
        data = {
            "trade_date": dt,
            "trade_type": trade_type_code_ref[trade_type],
            "variety": symbol,
            "contract_id": contract,
            "data_type": data_type_code_ref[data_type]
        }
        header = {
            'Referer': 'http://www.gfex.com.cn/gfex/rcjccpm/hqsj_tjsj.shtml'
        }
        resp = self.request_url(api_, "POST", data=data, header=header)
        res = json.loads(resp)
        data = res.get('data', {})
        res_df = pd.DataFrame().from_dict(data)
        if res_df.empty:
            return
        res_df = res_df[~res_df['memberId'].fillna('').str.contains('小计|总计')].copy()
        res_df = res_df[~res_df['contractId'].fillna('').str.contains('小计|总计')].copy()
        res_df = res_df.rename(
            columns={
                'abbr': f"{data_type}_abbr",
                'todayQty': f"{data_type}_todayQty",
                'qtySub': f"{data_type}_qtySub",
            }
        )
        return res_df

    def save_option_position_rank_data(self, df: pd.DataFrame):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_option_cn_trade_data", "position_rank_GFEX",
            set_index=['memberId', 'trading_date', 'contract', 'cpFlag'], partition=['trading_date']
        )

    def save_future_position_rank_data(self, df: pd.DataFrame):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_future_cn_trade_data", "position_rank_GFEX",
            set_index=['contractId', 'trading_date', 'contract'], partition=['trading_date']
        )

    def download_option_position_rank_data(self, given_dt: datetime):
        spec_opt = self.base.read_dataframe(
            "raw_option_cn_meta_data", "contract_info_GFEX",
            filter_datetime={'endTradeDate': {'gte': given_dt.strftime("%Y-%m-%d")}, 'startTradeDate': {'lte': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['varietyOrder']
        )
        if spec_opt.empty:
            return
        sym_ls = spec_opt['varietyOrder'].drop_duplicates()
        contract_ls = []
        for s in sym_ls:
            sc_ls = self.load_contract_id(s, given_dt)
            if sc_ls is None:
                continue
            contract_ls += sc_ls
        today_completed_contracts_df = self.base.read_dataframe(
            "raw_option_cn_trade_data", "position_rank_GFEX",
            filter_datetime={'trading_date': {'eq': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['contract']
        )
        if today_completed_contracts_df.empty:
            today_completed_contracts = []
        else:
            today_completed_contracts = today_completed_contracts_df['contract'].drop_duplicates().str.lower().tolist()
        contract_ls = [i for i in contract_ls if i not in today_completed_contracts]
        for c in contract_ls:
            s = c[:-4]
            vol_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'option', 'vol')
            buy_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'option', 'buy')
            sell_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'option', 'sell')
            if vol_df is None or buy_df is None or sell_df is None:
                continue
            res_df = pd.concat(
                [
                    vol_df.set_index(['memberId', 'cpFlag']),
                    buy_df.set_index(['memberId', 'cpFlag']),
                    sell_df.set_index(['memberId', 'cpFlag']),
                ],
                axis=1
            ).dropna(axis=1, how='all').reset_index(drop=False).assign(
                trading_date=given_dt,
                symbol=s,
                contract=c
            )
            yield res_df

    def download_future_position_rank_data(self, given_dt: datetime):
        spec_fut = self.base.read_dataframe(
            "raw_future_cn_meta_data", "contract_info_GFEX",
            filter_datetime={'endTradeDate': {'gte': given_dt.strftime("%Y-%m-%d")},
                             'startTradeDate': {'lte': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['varietyOrder']
        )
        if spec_fut.empty:
            return
        sym_ls = spec_fut['varietyOrder'].drop_duplicates()
        contract_ls = []
        for s in sym_ls:
            sc_ls = self.load_contract_id(s, given_dt)
            if sc_ls is None:
                continue
            contract_ls += sc_ls
        today_completed_contracts_df = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_GFEX",
            filter_datetime={'trading_date': {'eq': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['contract']
        )
        if today_completed_contracts_df.empty:
            today_completed_contracts = []
        else:
            today_completed_contracts = today_completed_contracts_df['contract'].drop_duplicates().str.lower().tolist()
        contract_ls = [i for i in contract_ls if i not in today_completed_contracts]
        for c in contract_ls:
            s = c[:-4]
            vol_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'future', 'vol')
            buy_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'future', 'buy')
            sell_df = self.__download_position_rank_data_by_data_type__(given_dt, s, c, 'future', 'sell')
            if vol_df is None or buy_df is None or sell_df is None:
                continue
            res_df = pd.concat(
                [
                    vol_df.set_index('contractId'),
                    buy_df.set_index('contractId'),
                    sell_df.set_index('contractId'),
                ],
                axis=1
            ).dropna(axis=1, how='all').reset_index(drop=False).assign(
                trading_date=given_dt,
                symbol=s,
                contract=c
            ).drop(columns=['memberId'])
            yield res_df

    def download_all_position_rank_data(self):
        WEB_START_DATE = datetime(2023, 11, 10)     # 网站上此项数据来源最早日期为2023-11-10
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_trade_data", "position_rank_GFEX",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = WEB_START_DATE if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_GFEX",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = WEB_START_DATE if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        end_date = datetime.now()
        start_date = min(last_df_opt, last_df_fut)
        for t in yield_dates(start_date, end_date):
            for r_fut in self.download_future_position_rank_data(t):
                self.save_future_position_rank_data(r_fut)
            for r_opt in self.download_option_position_rank_data(t):
                self.save_option_position_rank_data(r_opt)
            sec = random.randint(3, 10)
            sleep(sec)
            print(f"Done fetching pos rank data on {t}. Wait {sec} seconds before next query.")


if __name__ == "__main__":
    import random
    import time
    c = CrawlerGFEX()
    while datetime.now() < datetime.now().replace(hour=22, minute=30, second=0, microsecond=0):
        c.download_all_contract_info()
        c.download_all_daily_md_data()
        c.download_all_position_rank_data()
        t = 10 * 60 * random.random()
        print(f"SUB LOOP COMPLETE @{datetime.now()}, PENDING {t} seconds.")
        time.sleep(t)
    print(f"TASK COMPLETE @{datetime.now()}.")