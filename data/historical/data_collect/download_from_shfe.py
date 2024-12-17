import traceback
import json
import pandas as pd
from datetime import datetime, timedelta
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


class CrawlerSHFE(Crawler):
    def __init__(self):
        super().__init__()
        self.base = UnifiedControl(db_type='base')
        self.conf = Config()

    """ CONTRACT INFO """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_future_contract_info(self, date: datetime):
        dat = date.strftime("%Y%m%d")
        api_u = f"http://tsite.shfe.com.cn/data/instrument/ContractBaseInfo{dat}.dat"
        req = self.request_url(api_u)
        try:
            req = json.loads(req)
        except json.JSONDecodeError:
            return pd.DataFrame()
        else:
            res = req["ContractBaseInfo"]
            res_df = pd.DataFrame(res)
            res_df['UPDATE_DATE'] = pd.to_datetime(res_df['UPDATE_DATE'])
            res_df['INSTRUMENTID'] = res_df['INSTRUMENTID'].apply(lambda x: x.upper())
            res_df['EXPIREDATE'] = res_df['EXPIREDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['OPENDATE'] = res_df['OPENDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['TRADINGDAY'] = res_df['TRADINGDAY'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['ENDDELIVDATE'] = res_df['ENDDELIVDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['STARTDELIVDATE'] = res_df['STARTDELIVDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['BASISPRICE'] = res_df['BASISPRICE'].astype(float)
            return res_df

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_option_contract_info(self, date: datetime):
        dat = date.strftime("%Y%m%d")
        api_u = f"http://tsite.shfe.com.cn/data/instrument/option/ContractBaseInfo{dat}.dat"
        req = self.request_url(api_u)
        try:
            req = json.loads(req)
        except json.JSONDecodeError:
            return pd.DataFrame()
        else:
            res = req["OptionContractBaseInfo"]
            res_df = pd.DataFrame(res)
            res_df.dropna(axis=0, how="all", inplace=True)
            res_df['UPDATE_DATE'] = pd.to_datetime(res_df['UPDATE_DATE'])
            res_df['COMMODITYID'] = res_df['COMMODITYID'].apply(lambda x: x.upper())
            res_df['INSTRUMENTID'] = res_df['INSTRUMENTID'].apply(lambda x: x.upper())
            res_df['EXPIREDATE'] = res_df['EXPIREDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['OPENDATE'] = res_df['OPENDATE'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df['TRADINGDAY'] = res_df['TRADINGDAY'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            res_df[['PRICETICK', 'TRADEUNIT']] = res_df[['PRICETICK', 'TRADEUNIT']].astype(float)
            return res_df

    def save_contract_info(self, df: pd.DataFrame, type_name: str):
        self.base.insert_dataframe(
            df, f"raw_{type_name}_cn_meta_data", "contract_info_SHFE",
            set_index=['INSTRUMENTID', 'OPENDATE'], partition=['OPENDATE']
        )

    def download_all_contract_info(self):
        last_odf = self.base.read_dataframe(
            "raw_option_cn_meta_data", "contract_info_SHFE",
            ascending=[('OPENDATE', False)],
            filter_row_limit=1
        )
        last_ot = last_odf.iloc[0]['OPENDATE'] if not last_odf.empty else self.conf.project_start_date
        last_fdf = self.base.read_dataframe(
            "raw_future_cn_meta_data", "contract_info_SHFE",
            ascending=[('OPENDATE', False)],
            filter_row_limit=1
        )
        last_ft = last_fdf.iloc[0]['OPENDATE'] if not last_fdf.empty else self.conf.project_start_date
        start_datetime = min(last_ot, last_ft)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            odf = self.download_option_contract_info(t)
            self.save_contract_info(odf, type_name='option')
            fdf = self.download_future_contract_info(t)
            self.save_contract_info(fdf, type_name='future')

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_option_md_data(self, trading_date: datetime):
        dt = trading_date.strftime("%Y%m%d")
        api_ = f"http://tsite.shfe.com.cn/data/dailydata/option/kx/kx{dt}.dat"
        resp = self.request_url(api_)
        if "PAGE NOT FOUND" in resp.upper():
            return
        else:
            try:
                resp = eval(resp)
            except SyntaxError:
                return
            else:
                dt_r = datetime.strptime(
                    f"{resp.pop('o_year')}{resp.pop('o_month')}{resp.pop('o_day')}",
                    "%Y%m%d"
                )
                if dt_r != trading_date:
                    logger.warning(f"Given date: {trading_date} not matching result date: {dt_r}")
                md_df = pd.DataFrame(resp.pop('o_curinstrument'))
                md_df = md_df.loc[~md_df.where(md_df.isin(['小计', '总计', '说明', '合计'])).any(axis=1)]
                md_df['datetime'] = md_df['trading_date'] = pd.to_datetime(dt_r)
                pd_df = pd.DataFrame(resp.pop('o_curproduct'))
                pd_df = pd_df.loc[~pd_df.where(pd_df.isin(['小计', '总计', '说明', '合计'])).any(axis=1)]
                pd_df['datetime'] = pd_df['trading_date'] = pd.to_datetime(dt_r)
                iv_df = pd.DataFrame(resp.pop('o_cursigma'))
                iv_df = iv_df.loc[~iv_df.where(iv_df.isin(['小计', '总计', '说明', '合计'])).any(axis=1)]
                iv_df['datetime'] = iv_df['trading_date'] = pd.to_datetime(dt_r)
                return md_df, pd_df, iv_df

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_future_md_data(self, trading_date: datetime):
        dt = trading_date.strftime("%Y%m%d")
        api_ = f"http://tsite.shfe.com.cn/data/dailydata/kx/kx{dt}.dat"
        resp = self.request_url(api_)
        if "PAGE NOT FOUND" in resp.upper():
            return pd.DataFrame()
        else:
            resp = eval(resp)
            dt_r = datetime.strptime(
                f"{resp.pop('o_year')}{resp.pop('o_month')}{resp.pop('o_day')}",
                "%Y%m%d"
            )
            if dt_r != trading_date:
                logger.warning(f"Given date: {trading_date} not matching result date: {dt_r}")
            md_df = pd.DataFrame(resp.pop('o_curinstrument'))
            md_df = md_df.loc[~md_df.where(md_df.isin(['小计', '总计', '说明', '合计'])).any(axis=1)]
            md_df['datetime'] = md_df['trading_date'] = pd.to_datetime(dt_r)
            # product 品种成交概况 以及 metal index 金属期货价格指数暂不录入
            # pro_df = pd.DataFrame(resp.pop('o_curproduct'))
            # idx_df = pd.DataFrame(resp.pop('o_curmetalindex'))
            return md_df

    def save_daily_option_md_data(self, md_df: pd.DataFrame, pd_df: pd.DataFrame, iv_df: pd.DataFrame):
        self.base.insert_dataframe(
            md_df, "raw_option_cn_md_data", "all_1d_SHFE",
            set_index=['INSTRUMENTID', 'datetime'], partition=['trading_date']
        )
        self.base.insert_dataframe(
            pd_df, "raw_option_cn_md_data", "all_1d_summary_SHFE",
            set_index=['PRODUCTID', 'datetime'], partition=['trading_date']
        )
        self.base.insert_dataframe(
            iv_df, "raw_option_cn_md_data", "all_1d_iv_SHFE",
            set_index=['INSTRUMENTID', 'datetime'], partition=['trading_date']
        )

    def save_daily_future_md_data(self, md_df: pd.DataFrame):
        self.base.insert_dataframe(
            md_df, "raw_future_cn_md_data", "all_1d_SHFE",
            set_index=['PRODUCTID', 'DELIVERYMONTH', 'datetime'], partition=['trading_date']
        )

    def download_all_daily_md_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_md_data", "all_1d_SHFE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_md_data", "all_1d_SHFE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        start_datetime = min(last_df_opt, last_df_fut)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            res_f = self.download_daily_future_md_data(t)
            self.save_daily_future_md_data(res_f)
            res_o = self.download_daily_option_md_data(t)
            if res_o is not None:
                self.save_daily_option_md_data(*res_o)

    @try_catch(suppress_traceback=True, catch_args=True)
    def download_future_position_rank_data(self, trading_date: datetime):
        dt = trading_date.strftime("%Y%m%d")
        api_ = f"https://tsite.shfe.com.cn/data/dailydata/kx/pm{dt}.dat"
        resp = self.request_url(api_)
        if "PAGE NOT FOUND" in resp.upper():
            logger.warning(f"empty position rank data: {trading_date}")
            return pd.DataFrame()
        else:
            resp = eval(resp)
            dt_r = datetime.strptime(resp.pop('report_date'), "%Y%m%d")
            if dt_r != trading_date:
                logger.warning(f"Given date: {trading_date} not matching result date: {dt_r}")
            res_df = pd.DataFrame(resp.pop('o_cursor'))
            res_df = res_df[(res_df['RANK'] > 0) & (res_df['RANK'] <= 20)].copy()
            res_df = res_df.loc[
                ~res_df.where(
                    res_df.isin(['小计', '总计', '说明', '合计', '期货公司', '非期货公司'])
                ).any(axis=1)
            ]
            res_df['datetime'] = res_df['trading_date'] = pd.to_datetime(dt_r)
            return res_df

    def save_future_position_rank_data(self, df: pd.DataFrame):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_future_cn_trade_data", "position_rank_SHFE",
            set_index=['INSTRUMENTID', 'datetime', 'RANK'], partition=['trading_date']
        )

    def download_all_position_rank_data(self):
        last_df = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_SHFE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_dt = self.conf.project_start_date if last_df.empty else last_df.iloc[0]['trading_date']
        end_date = datetime.now()
        for t in yield_dates(last_dt, end_date):
            df = self.download_future_position_rank_data(t)
            self.save_future_position_rank_data(df)


if __name__ == "__main__":
    c = CrawlerSHFE()
    c.download_all_contract_info()
    c.download_all_daily_md_data()
    c.download_all_position_rank_data()
