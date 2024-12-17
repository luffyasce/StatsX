import re
import json
import random
import time
import traceback
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
import requests.exceptions
from lxml import etree
from utils.crawler.crawler_base import Crawler
from utils.custom.exception.errors import CrawlerError
from utils.database.unified_db_control import UnifiedControl
from utils.tool.decorator import try_catch
from utils.tool.datetime_wrangle import yield_dates
from utils.tool.configer import Config
from utils.tool.logger import log


logger = log(__file__, 'data')

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class CrawlerDCE(Crawler):
    def __init__(self):
        super().__init__()
        self.base = UnifiedControl(db_type='base')
        self.conf = Config()

        self.session = self.init_session()

    """ CONTRACT INFO """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_contract_info(self, data_type: str):
        api_ = "http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html"
        ty_ = '1' if data_type == 'option' else '0'
        data = {
            "contractInformation.variety": 'all',
            "contractInformation.trade_type": ty_,
        }
        resp = self.request_url(api_, "POST", data=data)
        html = etree.HTML(resp)
        res_head = html.xpath("//div[@class='dataArea']/table/tr//th//text()")
        res_head = [i.strip() for i in res_head]
        res_body = html.xpath("//div[@class='dataArea']/table/tr")
        body_ = []
        for i in res_body:
            body_.append([x.strip() for x in i.xpath(".//td//text()")])
        res_df = pd.DataFrame(body_, columns=res_head)
        res_df.dropna(axis=0, how="all", inplace=True)
        res_df['合约代码'] = [i.upper().replace('-', '') for i in res_df['合约代码']]
        res_df['开始交易日'] = res_df['开始交易日'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
        res_df['最后交易日'] = res_df['最后交易日'].apply(lambda x: datetime.strptime(x, "%Y%m%d"))
        res_df[['交易单位', '最小变动价位']] = res_df[['交易单位', '最小变动价位']].astype(float)

        return res_df

    def save_contract_info(self, df: pd.DataFrame, type_name: str):
        self.base.insert_dataframe(
            df, f"raw_{type_name}_cn_meta_data", "contract_info_DCE",
            set_index=['合约代码', '开始交易日'], partition=['开始交易日']
        )

    def download_all_contract_info(self):
        df_f = self.download_contract_info("future")
        self.save_contract_info(df_f, "future")
        df_o = self.download_contract_info('option')
        self.save_contract_info(df_o, 'option')

    """ MD DATA """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_daily_md_data(self, trading_date: datetime, data_type: str):
        y, m, d = trading_date.year, trading_date.month, trading_date.day
        ty_ = '1' if data_type == 'option' else '0'
        api_ = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"
        data = {
            "dayQuotes.variety": 'all',
            "dayQuotes.trade_type": ty_,
            "year": str(y),
            "month": str(m-1),
            "day": str(d)
        }
        resp = self.request_url(api_, "POST", data=data)
        html = etree.HTML(resp)
        echo_ = '---'.join(html.xpath("//div[@class='tradeResult02']//p//span//text()"))
        if "暂无数据" in echo_:
            return
        else:
            dt = datetime.strptime(re.findall("查询日期：[0-9]{8}", echo_)[0].split('：')[1], "%Y%m%d")
            if dt != trading_date:
                logger.warning(f"Given date: {trading_date} not matching result date: {dt}")
            # md data
            head_md = html.xpath("//div[@class='dataArea']//table//tr//th//text()")
            tables_md = html.xpath("//div[@class='dataArea']//table/tr")
            rows_md = []
            for r in tables_md:
                row = [x.strip() for x in r.xpath(".//td//text()")]
                if len(row) == 0 or True in set([i in row[0] for i in ['小计', '总计', '说明', '合计']]):
                    pass
                else:
                    rows_md.append(row)
            md_df = pd.DataFrame(rows_md, columns=head_md)
            md_df['datetime'] = md_df['trading_date'] = pd.to_datetime(dt)

            if data_type == 'option':
                # iv data
                head_iv = html.xpath("//div[@class='dataWrapper']//div[last()]//table//tr//th//text()")
                tables_iv = html.xpath("//div[@class='dataWrapper']//div[last()]//table/tr")
                rows_iv = []
                for r in tables_iv:
                    row = [x.strip() for x in r.xpath(".//td//text()")]
                    if len(row) == 0 or True in set([i in row[0] for i in ['小计', '总计', '说明', '合计']]):
                        pass
                    else:
                        rows_iv.append(row)
                iv_df = pd.DataFrame(rows_iv, columns=head_iv)
                iv_df['datetime'] = iv_df['trading_date'] = pd.to_datetime(dt)
            else:
                iv_df = pd.DataFrame()
            return md_df, iv_df

    def save_daily_md_data(self, md_df, iv_df, data_type: str):
        self.base.insert_dataframe(
            md_df, f"raw_{data_type}_cn_md_data", "all_1d_DCE",
            set_index=['合约名称', 'datetime'], partition=['trading_date']
        )
        if not iv_df.empty and data_type == 'option':
            self.base.insert_dataframe(
                iv_df, "raw_option_cn_md_data", "all_1d_iv_DCE",
                set_index=['合约系列', 'datetime'], partition=['trading_date']
            )

    def download_all_daily_md_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_md_data", "all_1d_DCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_md_data", "all_1d_DCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        start_datetime = min(last_df_opt, last_df_fut)
        end_datetime = datetime.now()
        for t in yield_dates(start_datetime, end_datetime):
            res_fut = self.download_daily_md_data(t, "future")
            if res_fut:
                self.save_daily_md_data(*res_fut, "future")
            res_opt = self.download_daily_md_data(t, 'option')
            if res_opt:
                self.save_daily_md_data(*res_opt, "option")

    """ POSITION RANKING DATA"""
    @try_catch(suppress_traceback=True, catch_args=True)
    def _check_available_contracts_on_page(self, date: datetime, symbol: str, data_type: str):
        api_ = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
        y, m, d = date.year, date.month, date.day
        ty_ = '0' if data_type == 'future' else '1'
        data = {
            "memberDealPosiQuotes.variety": symbol.lower(),
            "memberDealPosiQuotes.trade_type": ty_,
            "year": str(y),
            "month": str(m - 1),
            "day": str(d),
            "contract.contract_id": 'all',
            "contract.variety_id": symbol.lower(),
            "contract": "",
        }
        try:
            resp = self.request_url(api_, "POST", data=data)
        except requests.exceptions.ConnectionError as err:
            print("Conn Err: \n" + str(traceback.format_exc()) + "\n" + f"PARAMS: {date} {symbol} {data_type}")
            return
        else:
            pass

        html = etree.HTML(resp)
        c_res = [
            i.strip() for i in html.xpath(
                "//div[@class='tradeSel']//div[@class='selBox']//div//ul//li[@class='keyWord_100']//text()"
            ) if symbol.lower() in i.strip().lower()
        ]
        return c_res

    @staticmethod
    def __position_table_proc__(edf, date, contract, sym, type: str):
        edf = edf[~edf.apply(lambda row: row.astype(str).str.contains('小计|总计|说明|合计').any(), axis=1)].copy()
        if edf.empty or edf.isnull().all().all():
            return pd.DataFrame()
        rank_s = edf.loc[:, '名次']
        rank_s = rank_s if isinstance(rank_s, pd.Series) else rank_s.iloc[:, 0]
        edf.columns = [i.split('.')[0] for i in edf.columns]
        edf.drop(columns=['名次'], inplace=True)
        col_s = pd.Series(edf.columns.tolist()).to_dict()
        edf.columns = [
            v if v not in ['会员简称', '增减'] else (
                v + col_s[i + 1] if v == '会员简称' else col_s[i - 1] + v
            ) for i, v in col_s.items()
        ]
        edf = edf.assign(
            datetime=date,
            trading_date=date,
            contract=contract.upper(),
            symbol=sym.upper()
        )
        edf['名次'] = rank_s
        if type == 'future':
            pass
        else:
            edf = edf.assign(opt_type=type)
        return edf

    @try_catch(suppress_traceback=True, catch_args=True)
    def _download_option_position_rank_data_by_contract(self, date: datetime, contract: str):
        time.sleep(random.random())
        api_ = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesData.html"
        y, m, d = date.year, date.month, date.day
        ty_ = '1'   # option
        sym = contract[:-4]
        data = {
            "memberDealPosiQuotes.variety": sym.lower(),
            "memberDealPosiQuotes.trade_type": ty_,
            "year": str(y),
            "month": str(m-1),
            "day": str(d),
            "contract.contract_id": contract.lower(),
            "contract.variety_id": sym.lower(),
            "exportFlag": "excel"
        }
        resp = self.session.post(api_, data=data)
        if resp.status_code != 200:
            logger.warning(f"Error fetching future position rank data: {contract} @ {date}")
            return pd.DataFrame()
        else:
            excel_data = BytesIO(resp.content)
            edf = self.read_online_excel_with_auto_engine_switch(excel_data)
            if edf.empty:
                return edf
            x_call= edf.apply(lambda row: row.astype(str).str.contains('看涨期权').any(), axis=1)
            idx_call = x_call[x_call].index.tolist()[0]
            x_put = edf.apply(lambda row: row.astype(str).str.contains('看跌期权').any(), axis=1)
            idx_put = x_put[x_put].index.tolist()[0]
            cdf = edf.iloc[idx_call + 1: idx_put].copy()
            cdf.columns = cdf.iloc[0]
            cdf = cdf.drop(cdf.index[0]).reset_index(drop=True).replace('-', np.nan)
            pdf = edf.iloc[idx_put + 1:].copy()
            pdf.columns = pdf.iloc[0]
            pdf = pdf.drop(pdf.index[0]).reset_index(drop=True).replace('-', np.nan)

            cdf = self.__position_table_proc__(cdf, date, contract, sym, '看涨期权')
            pdf = self.__position_table_proc__(pdf, date, contract, sym, '看跌期权')

            res = pd.concat([cdf, pdf], axis=0)
            return res

    @try_catch(suppress_traceback=True, catch_args=True)
    def _download_future_position_rank_data_by_contract(self, date: datetime, contract: str):
        time.sleep(random.random())
        api_ = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesData.html"
        y, m, d = date.year, date.month, date.day
        ty_ = '0'   # future
        sym = contract[:-4]
        data = {
            "memberDealPosiQuotes.variety": sym.lower(),
            "memberDealPosiQuotes.trade_type": ty_,
            "year": str(y),
            "month": str(m-1),
            "day": str(d),
            "contract.contract_id": contract.lower(),
            "contract.variety_id": sym.lower(),
            "exportFlag": "excel"
        }
        resp = self.session.post(api_, data=data)
        if resp.status_code != 200:
            logger.warning(f"Error fetching future position rank data: {contract} @ {date}")
            return pd.DataFrame()
        else:
            excel_data = BytesIO(resp.content)
            edf = self.read_online_excel_with_auto_engine_switch(excel_data)
            if edf.empty:
                return edf
            df = self.__position_table_proc__(edf, date, contract, sym, 'future')
            return df

    def save_option_position_rank_data(self, df: pd.DataFrame):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_option_cn_trade_data", "position_rank_DCE",
            set_index=['名次', 'datetime', 'contract', 'opt_type'], partition=['trading_date']
        )

    def save_future_position_rank_data(self, df: pd.DataFrame):
        if df is None:
            return
        self.base.insert_dataframe(
            df, f"raw_future_cn_trade_data", "position_rank_DCE",
            set_index=['名次', 'datetime', 'contract'], partition=['trading_date']
        )

    def download_option_position_rank_data(self, given_dt: datetime):
        spec_opt = self.base.read_dataframe(
            "raw_option_cn_meta_data", "contract_info_DCE",
            filter_datetime={'最后交易日': {'gte': given_dt.strftime("%Y-%m-%d")}, '开始交易日': {'lte': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['合约代码']
        )
        if not spec_opt.empty:
            contract_s = spec_opt['合约代码'].apply(lambda x: re.findall("[A-Z]+[0-9]{4}", x)[0]).drop_duplicates()
            sym_ls = contract_s.apply(lambda x: x[:-4].lower()).drop_duplicates()
            contract_ls = []
            for s in sym_ls:
                contracts_on_page = self._check_available_contracts_on_page(given_dt, s, 'option')
                if contracts_on_page is None:
                    continue
                contract_ls += contracts_on_page
        else:
            contract_ls = []
        today_completed_contracts_df = self.base.read_dataframe(
            "raw_option_cn_trade_data", "position_rank_DCE",
            filter_datetime={'trading_date': {'eq': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['contract']
        )
        if today_completed_contracts_df.empty:
            today_completed_contracts = []
        else:
            today_completed_contracts = today_completed_contracts_df['contract'].drop_duplicates().str.lower().tolist()
        contract_ls = [i for i in contract_ls if i not in today_completed_contracts]
        random.shuffle(contract_ls)
        for c in contract_ls:
            df = self._download_option_position_rank_data_by_contract(given_dt, c)
            self.save_option_position_rank_data(df)

    def download_future_position_rank_data(self, given_dt: datetime):
        spec_fut = self.base.read_dataframe(
            "raw_future_cn_meta_data", "contract_info_DCE",
            filter_datetime={'最后交易日': {'gte': given_dt.strftime("%Y-%m-%d")}, '开始交易日': {'lte': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['合约代码']
        )
        if not spec_fut.empty:
            contract_s = spec_fut['合约代码'].drop_duplicates()
            sym_ls = contract_s.apply(lambda x: x[:-4].lower()).drop_duplicates()
            contract_ls = []
            for s in sym_ls:
                contracts_on_page = self._check_available_contracts_on_page(given_dt, s, 'future')
                if contracts_on_page is None:
                    continue
                contract_ls += contracts_on_page
        else:
            contract_ls = []
        today_completed_contracts_df = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_DCE",
            filter_datetime={'trading_date': {'eq': given_dt.strftime("%Y-%m-%d")}},
            filter_columns=['contract']
        )
        if today_completed_contracts_df.empty:
            today_completed_contracts = []
        else:
            today_completed_contracts = today_completed_contracts_df['contract'].drop_duplicates().str.lower().tolist()
        contract_ls = [i for i in contract_ls if i not in today_completed_contracts]
        random.shuffle(contract_ls)
        for c in contract_ls:
            df = self._download_future_position_rank_data_by_contract(given_dt, c)
            self.save_future_position_rank_data(df)

    def download_all_position_rank_data(self):
        last_df_opt = self.base.read_dataframe(
            "raw_option_cn_trade_data", "position_rank_DCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_opt = self.conf.project_start_date if last_df_opt.empty else last_df_opt.iloc[0]['trading_date']
        last_df_fut = self.base.read_dataframe(
            "raw_future_cn_trade_data", "position_rank_DCE",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        last_df_fut = self.conf.project_start_date if last_df_fut.empty else last_df_fut.iloc[0]['trading_date']
        end_date = datetime.now()
        start_date = min(last_df_opt, last_df_fut)
        for t in yield_dates(start_date, end_date):
            self.download_future_position_rank_data(t)
            self.download_option_position_rank_data(t)

    """OTC data"""
    # standard warehouse bills
    def download_otc_standard_warehouse_bill_offer_data(self, page: int):
        page_url = "http://otc.dce.com.cn/?activeName=wbill&activeServiceName=wbillQuot&activeIndex=1"
        api_ = "http://otc.dce.com.cn/portal/data/app/wbillApplyList"
        params = {
            "startDate": "",
            "endDate": "",
            "varietyIdList": [],
            "wbillMatchQryData": {},
            "page": page,
            "limit": 100
        }
        resp = self.request_url(api_, 'POST', cookies=self.get_cookies(api_), json=params)
        data = json.loads(resp).get("data").get('wbillMatchResultData').get('rows')
        res = pd.DataFrame(data)
        return res

    def save_otc_standard_warehouse_bill_offer_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_trade_data", "otc_standard_warehouse_bill_offer_DCE",
            set_index=['varietyId', 'varietyName', 'whCode', 'whAbbr', 'opDate', 'seqNo']
        )

    def download_otc_standard_warehouse_bill_match_data(self, page: int):
        page_url = "http://otc.dce.com.cn/?activeName=wbill&activeServiceName=wbillQuot&activeIndex=1"
        api_ = "http://otc.dce.com.cn/portal/data/app/wbillMatchList"
        params = {
            "startDate": "",
            "endDate": "",
            "varietyIdList": [],
            "wbillMatchQryData": {},
            "page": page,
            "limit": 100
        }
        resp = self.request_url(api_, 'POST', cookies=self.get_cookies(api_), json=params)
        data = json.loads(resp).get("data").get("wbillMatchResultData").get("rows")
        res = pd.DataFrame(data)
        return res

    def save_otc_standard_warehouse_bill_match_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_trade_data", "otc_standard_warehouse_bill_match_DCE",
            set_index=['varietyId', 'varietyName', 'confirmDate', 'buyOpTime']
        )

    # non-standard warehouse bills
    def download_otc_non_standard_warehouse_bill_offer_data(self, page: int):
        page_url = "http://otc.dce.com.cn/?activeName=nonWbill&activeServiceName=nonWbillQuot&activeIndex=1"
        api_ = "http://otc.dce.com.cn/portal/data/app/nonWbillApplyList"
        params = {
            "startDate": "",
            "endDate": "",
            "varietyIdList": [],
            "spotQryData": {},
            "page": page,
            "limit": 100
        }
        resp = self.request_url(api_, 'POST', cookies=self.get_cookies(api_), json=params)
        data = json.loads(resp).get("data").get('spotResultData').get('rows')
        res = pd.DataFrame(data)
        return res

    def save_otc_non_standard_warehouse_bill_offer_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_trade_data", "otc_non_standard_warehouse_bill_offer_DCE",
            set_index=df.columns.tolist()
        )

    def download_otc_non_standard_warehouse_bill_match_data(self, page: int):
        page_url = "http://otc.dce.com.cn/?activeName=nonWbill&activeServiceName=nonWbillQuot&activeIndex=1"
        api_ = "http://otc.dce.com.cn/portal/data/app/nonWbillMatchList"
        params = {
            "startDate": "",
            "endDate": "",
            "varietyIdList": [],
            "spotQryData": {},
            "page": page,
            "limit": 100
        }
        resp = self.request_url(api_, 'POST', cookies=self.get_cookies(api_), json=params)
        data = json.loads(resp).get("data").get("spotResultData").get("rows")
        res = pd.DataFrame(data)
        return res

    def save_otc_non_standard_warehouse_bill_match_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_trade_data", "otc_non_standard_warehouse_bill_match_DCE",
            set_index=df.columns.tolist()
        )


if __name__ == "__main__":
    import random
    import time
    c = CrawlerDCE()
    # c._download_future_position_rank_data_by_contract(datetime(2024, 5, 17), 'eb2409')
    c.download_all_position_rank_data()
    # c.download_option_position_rank_data(datetime(2024, 5, 20))

    # while datetime.now() < datetime.now().replace(hour=20, minute=0, second=0, microsecond=0):
    #     c.download_all_contract_info()
    #     c.download_all_daily_md_data()
    #     c.download_all_position_rank_data()
    #     t = 10 * 60 * random.random()
    #     print(f"SUB LOOP COMPLETE @{datetime.now()}, PENDING {t} seconds.")
    #     time.sleep(t)
    # print(f"TASK COMPLETE @{datetime.now()}.")


