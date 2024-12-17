import traceback
import urllib.error
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
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


class CrawlerCZCE(Crawler):
    def __init__(self):
        super().__init__()
        self.base = UnifiedControl(db_type='base')
        self.conf = Config()

    """ CONTRACT INFO """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_future_contract_info(self, date_: datetime):
        dat = date_.strftime("%Y%m%d")
        year = dat[:4]
        api_u = f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{dat}/FutureDataReferenceData.csv"
        res_df = self.read_online_excel_with_auto_engine_switch(api_u)
        res_df.dropna(axis=0, how="all", inplace=True)
        if res_df.empty:
            pass
        else:
            res_df = res_df[[
                '产品名称', '合约代码', '产品代码', '到期时间', '最小变动价位', '最小变动价值', '交易单位', '计量单位', '最大下单量',
                '日持仓限额', '大宗交易最小规模', '上市周期', '交割通知日', '第一交易日', '最后交易日', '交割结算日', '月份代码', '年份代码',
                '最后交割日', '合约交割月份', '交易保证金率', '涨跌停板', '交易手续费', '交割手续费', '平今仓手续费'
            ]].copy()
            res_df = res_df.applymap(lambda x: str(x).strip())
            res_df = res_df.replace(to_replace='n.a.', value=np.nan).dropna(subset=['到期时间'])
            res_df = res_df[res_df['产品名称'].str.contains("期货")].copy()
            res_df['到期时间'] = res_df['到期时间'].apply(
                lambda x: datetime.strptime(
                    re.findall("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}", x)[0], "%Y-%m-%d %H:%M"
                )
            )
            res_df['第一交易日'] = pd.to_datetime(res_df['第一交易日'])
            res_df['最后交易日'] = pd.to_datetime(res_df['最后交易日'])
            res_df['交割结算日'] = pd.to_datetime(res_df['交割结算日'])
            res_df['最后交割日'] = pd.to_datetime(res_df['最后交割日'])
            res_df.dropna(subset=['第一交易日', '最后交易日', '最后交割日'], how='any', inplace=True)
        return res_df

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_option_contract_info(self, date_: datetime):
        dat = date_.strftime("%Y%m%d")
        year = dat[:4]
        api_u = f"http://www.czce.com.cn/cn/DFSStaticFiles/Option/{year}/{dat}/OptionDataReferenceData.csv"
        res_df = self.read_online_excel_with_auto_engine_switch(api_u)

        res_df.dropna(subset=['产品名称'], inplace=True)
        if res_df.empty:
            pass
        else:
            res_df = res_df[[
                '产品名称', '合约代码', '产品代码', '到期时间', '最小变动价位', '最小变动价值', '交易单位', '计量单位', '最大下单量',
                '日持仓限额', '大宗交易最小规模', '上市周期', '交割通知日', '第一交易日', '最后交易日', '结算日', '月份代码', '年份代码',
                '行权价', '到期日', '期权卖保证金额', '交易手续费', '行权/履约手续费', '平今仓手续费'
            ]].copy()
            res_df = res_df.applymap(lambda x: str(x)).replace(to_replace='None', value=None)
            res_df.dropna(subset=['到期时间', '第一交易日', '最后交易日', '到期日'], how='any', inplace=True)
            res_df['到期时间'] = res_df['到期时间'].apply(lambda x: datetime.strptime(x[:16], "%Y-%m-%d %H:%M"))
            res_df['第一交易日'] = pd.to_datetime(res_df['第一交易日'])
            res_df['最后交易日'] = pd.to_datetime(res_df['最后交易日'])
            res_df['结算日'] = pd.to_datetime(res_df['结算日'])
            res_df['到期日'] = pd.to_datetime(res_df['到期日'])
            res_df.dropna(subset=['第一交易日', '最后交易日', '到期日'], how='any', inplace=True)
        return res_df

    def save_contract_info(self, df: pd.DataFrame, type_name: str):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_{type_name}_cn_meta_data", "contract_info_CZCE",
            set_index=['合约代码', '第一交易日'], partition=['第一交易日']
        )

    def download_all_contract_info(self):
        last_odf = self.base.read_dataframe(
            "raw_option_cn_meta_data", "contract_info_CZCE",
            ascending=[('第一交易日', False)],
            filter_row_limit=1
        )
        last_ot = last_odf.iloc[0]['第一交易日'] if not last_odf.empty else self.conf.project_start_date
        last_fdf = self.base.read_dataframe(
            "raw_future_cn_meta_data", "contract_info_CZCE",
            ascending=[('第一交易日', False)],
            filter_row_limit=1
        )
        last_ft = last_fdf.iloc[0]['第一交易日'] if not last_fdf.empty else self.conf.project_start_date
        start_datetime = min(last_ot, last_ft)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            odf = self.download_option_contract_info(t)
            self.save_contract_info(odf, type_name='option')
            fdf = self.download_future_contract_info(t)
            self.save_contract_info(fdf, type_name='future')

    """ MD DATA """
    @staticmethod
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_option_md_data(trading_date: datetime):
        year_ = trading_date.year
        dt_ = trading_date.strftime("%Y%m%d")
        api_ = f"http://www.czce.com.cn/cn/DFSStaticFiles/Option/{year_}/{dt_}/OptionDataDaily.xls"
        df = pd.read_excel(api_)
        data = df.iloc[1:]
        data.columns = df.iloc[0].tolist()
        data = data[~data['合约代码'].str.contains('|'.join(['小计', '总计', '说明', '合计']))].copy()
        if not data.empty:
            data['datetime'] = data['trading_date'] = pd.to_datetime(trading_date)
            data = data.rename(
                columns={'品种代码': '合约代码'},
                errors='ignore'
            ).dropna(
                subset=['合约代码']
            )
            return data
        else:
            return pd.DataFrame()

    @staticmethod
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_future_md_data(trading_date: datetime):
        year_ = trading_date.year
        dt_ = trading_date.strftime("%Y%m%d")
        api_ = f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year_}/{dt_}/FutureDataDaily.xls"
        df = pd.read_excel(api_)
        data = df.iloc[1:]
        data.columns = df.iloc[0].tolist()
        data = data[~data['合约代码'].str.contains('|'.join(['小计', '总计', '说明', '合计']))].copy()
        if not data.empty:
            data['datetime'] = data['trading_date'] = pd.to_datetime(trading_date)
            data = data.rename(
                columns={'品种代码': '合约代码'},
                errors='ignore'
            ).dropna(
                subset=['合约代码']
            )
            return data
        else:
            return pd.DataFrame()

    def save_daily_md_data(self, md_df: pd.DataFrame, type_name: str):
        if md_df is None:
            return
        self.base.insert_dataframe(
            md_df, f"raw_{type_name}_cn_md_data", "all_1d_CZCE",
            set_index=['合约代码', 'datetime'], partition=['trading_date']
        )

    def download_all_daily_md_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_md_data", "all_1d_CZCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_md_data", "all_1d_CZCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        start_datetime = min(last_df_opt, last_df_fut)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            df_opt = self.download_daily_option_md_data(t)
            self.save_daily_md_data(df_opt, "option")
            df_fut = self.download_daily_future_md_data(t)
            self.save_daily_md_data(df_fut, "future")

    """ POSITION RANKING DATA """
    @staticmethod
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_position_rank_data(date: datetime, type_name: str):
        ty_ = type_name.capitalize()
        year_ = date.year
        dt_ = date.strftime("%Y%m%d")
        api_ = f"http://www.czce.com.cn/cn/DFSStaticFiles/{ty_}/{year_}/{dt_}/{ty_}DataHolding.xls"
        df = pd.read_excel(api_)
        df.columns = [f"c{i}" for i in range(10)]
        right_column_s: pd.Series = df[df["c0"] == "名次"].iloc[0].reset_index(drop=True)
        right_columns = [
            v if v not in ['会员简称', '增减量'] else (
                v + right_column_s.iloc[i + 1] if v == '会员简称' else right_column_s.iloc[i - 1] + v
            ) for i, v in right_column_s.items()
        ]
        df_ls = np.split(df, df[df['c0'].str.contains('|'.join(['品种', '合约']), na=False)].index)
        res = pd.DataFrame()
        for dv in df_ls:
            if dv.empty:
                continue
            xv = dv.iloc[0, 0]
            xls = [i.split('：') for i in xv.split(' ') if i.strip() != '']
            dv.columns = right_columns
            dv = dv.iloc[2:]
            dv = dv[~dv['名次'].isin(['小计', '总计', '说明', '合计'])].assign(
                data_type=xls[0][0],
                data_symbol=xls[0][1],
                trading_date=datetime.strptime(xls[1][1], "%Y-%m-%d")
            )
            res = pd.concat([res, dv], axis=0)
        return res

    def save_position_rank_data(self, md_df: pd.DataFrame, type_name: str):
        if md_df is None:
            return
        self.base.insert_dataframe(
            md_df, f"raw_{type_name}_cn_trade_data", "position_rank_CZCE",
            set_index=['名次', 'data_type', 'data_symbol', 'trading_date'], partition=['trading_date']
        )

    def download_all_position_rank_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_trade_data", "position_rank_CZCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_CZCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        start_datetime = min(last_df_opt, last_df_fut)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            df_o = self.download_position_rank_data(t, "option")
            self.save_position_rank_data(df_o, "option")
            df_f = self.download_position_rank_data(t, "future")
            self.save_position_rank_data(df_f, "future")


if __name__ == "__main__":
    c = CrawlerCZCE()
    c.download_all_contract_info()

