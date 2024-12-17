import random
import time
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


class Crawler100Ppi(Crawler):
    def __init__(self):
        super().__init__()
        self.base = UnifiedControl(db_type='base')
        self.conf = Config()

    """ spot price INFO """
    @try_catch(suppress_traceback=True, catch_args=True)
    def download_standard_spot_price_data(self, given_date: datetime):
        given_date = given_date.replace(hour=0, minute=0, second=0, microsecond=0)
        dt = given_date.strftime('%Y-%m-%d')
        url_ = f"https://www.100ppi.com/sf/day-{dt}.html"
        req = self.request_url(url_)
        html = etree.HTML(req)
        res = html.xpath("//table[@id='fdata']//tr")
        namings, spot = [], []
        for tr in res:
            td = tr.xpath(".//td")
            if td[0].xpath(".//@href"):
                namings.append(td[0].xpath(".//text()")[0].strip())
                spot.append(td[1].xpath(".//text()")[0].strip())
        data = pd.Series(dict(zip(namings, spot)), name='price')
        data = pd.DataFrame(data).reset_index(drop=False, names=['spec']).assign(record_date=given_date)
        return data

    def save_standard_spot_price_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, f"raw_spot_cn_md_data", "standard_price_100ppi",
            set_index=['spec', 'record_date']
        )

    def download_all_standard_spot_price_data(self):
        last_odf = self.base.read_dataframe(
            "raw_spot_cn_md_data", "standard_price_100ppi",
            ascending=[('record_date', False)],
            filter_row_limit=1
        )
        last_ot = last_odf.iloc[0]['record_date'] if not last_odf.empty else self.conf.project_start_date
        for t in yield_dates(start=last_ot, end=datetime.now()):
            df = self.download_standard_spot_price_data(t)
            self.save_standard_spot_price_data(df)
            time.sleep(random.random())
