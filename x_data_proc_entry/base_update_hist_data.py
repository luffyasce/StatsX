import random
import time
from datetime import datetime
from data.historical.data_collect.download_from_dce import CrawlerDCE
from data.historical.data_collect.download_from_czce import CrawlerCZCE
from data.historical.data_collect.download_from_shfe import CrawlerSHFE
from data.historical.data_collect.download_from_gfex import CrawlerGFEX
from data.historical.data_collect.download_from_100ppi import Crawler100Ppi
from data.historical.data_pretreat.pretreat_data_from_czce import PretreatCZCE
from data.historical.data_pretreat.pretreat_data_from_dce import PretreatDCE
from data.historical.data_pretreat.pretreat_data_from_shfe import PretreatSHFE
from data.historical.data_pretreat.pretreat_data_from_gfex import PretreatGFEX
from data.historical.data_pretreat.pretreat_data_from_local import PretreatLocal
from data.historical.data_pretreat.pretreat_data_from_100ppi import Pretreat100Ppi
from data.historical.data_process.process_future_cn_md_data import ProcessFutureCnMdData
from data.historical.data_process.process_future_cn_trade_data import ProcessFutureCnTradeData
from data.historical.data_process.process_future_cn_meta_data import ProcessFutureCnMeta
from data.historical.data_process.process_option_cn_meta_data import ProcessOptionCnMeta
from data.historical.data_process.process_option_cn_trade_data import ProcessOptionCnTrade
from utils.tool.datetime_wrangle import yield_dates
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase
from utils.tool.configer import Config


class DataUpdateBaseCls:
    def __init__(self):
        config = Config()
        self.exchange_list = config.exchange_list

        self.collect_dce = CrawlerDCE()
        self.collect_czce = CrawlerCZCE()
        self.collect_shfe = CrawlerSHFE()
        self.collect_gfex = CrawlerGFEX()
        self.collect_100ppi = Crawler100Ppi()

        self.pret_dce = PretreatDCE()
        self.pret_czce = PretreatCZCE()
        self.pret_shfe = PretreatSHFE()
        self.pret_gfex = PretreatGFEX()
        self.pret_local = PretreatLocal()
        self.pret_100ppi = Pretreat100Ppi()

        self.pro_fut_cn_md = ProcessFutureCnMdData()
        self.pro_fut_cn_trade = ProcessFutureCnTradeData()
        self.pro_fut_cn_meta = ProcessFutureCnMeta()
        self.pro_opt_cn_meta = ProcessOptionCnMeta()
        self.pro_opt_cn_trade = ProcessOptionCnTrade()

    def start_collect_czce(self):
        self.collect_czce.download_all_contract_info()
        self.collect_czce.download_all_daily_md_data()
        self.collect_czce.download_all_position_rank_data()

    def start_collect_dce(self):
        self.collect_dce.download_all_contract_info()
        self.collect_dce.download_all_daily_md_data()
        self.collect_dce.download_all_position_rank_data()

    def start_collect_shfe(self):
        self.collect_shfe.download_all_contract_info()
        self.collect_shfe.download_all_daily_md_data()
        self.collect_shfe.download_all_position_rank_data()

    def start_collect_gfex(self):
        self.collect_gfex.download_all_contract_info()
        self.collect_gfex.download_all_daily_md_data()
        self.collect_gfex.download_all_position_rank_data()

    def start_collect_misc(self):
        self.collect_100ppi.download_all_standard_spot_price_data()

    def start_pretreat(self, initiation: bool):
        df = self.pret_czce.pretreat_option_contract_info_data()
        self.pret_czce.save_option_contract_info_data(df)
        df = self.pret_czce.pretreat_future_contract_info_data()
        self.pret_czce.save_future_contract_info_data(df)
        mdf, odf = self.pret_czce.pretreat_option_md_data()
        self.pret_czce.save_option_md_data(mdf)
        self.pret_czce.save_option_summary_data(odf)
        df = self.pret_czce.pretreat_future_md_data()
        self.pret_czce.save_future_md_data(df)
        res = self.pret_czce.pretreat_future_position_rank_data()
        self.pret_czce.save_future_position_rank_data(*res)
        res = self.pret_czce.pretreat_option_position_rank_data()
        self.pret_czce.save_option_position_rank_data(*res)

        df = self.pret_dce.pretreat_option_contract_info_data()
        self.pret_dce.save_option_contract_info_data(df)
        df = self.pret_dce.pretreat_future_contract_info_data()
        self.pret_dce.save_future_contract_info_data(df)
        mdf, odf = self.pret_dce.pretreat_option_md_data()
        self.pret_dce.save_option_md_data(mdf)
        self.pret_dce.save_option_summary_data(odf)
        df = self.pret_dce.pretreat_future_md_data()
        self.pret_dce.save_future_md_data(df)
        res = self.pret_dce.pretreat_future_position_rank_data()
        self.pret_dce.save_future_position_rank_data(res)
        res = self.pret_dce.pretreat_option_position_rank_data()
        self.pret_dce.save_option_position_rank_data(res)

        df = self.pret_shfe.pretreat_option_contract_info_data()
        self.pret_shfe.save_option_contract_info_data(df)
        df = self.pret_shfe.pretreat_future_contract_info_data()
        self.pret_shfe.save_future_contract_info_data(df)
        mdf, odf = self.pret_shfe.pretreat_option_md_data()
        self.pret_shfe.save_option_md_data(mdf)
        self.pret_shfe.save_option_summary_data(odf)
        df = self.pret_shfe.pretreat_future_md_data()
        self.pret_shfe.save_future_md_data(df)
        res = self.pret_shfe.pretreat_future_position_rank_data()
        self.pret_shfe.save_future_position_rank_data(res)

        df = self.pret_gfex.pretreat_option_contract_info_data()
        self.pret_gfex.save_option_contract_info_data(df)
        df = self.pret_gfex.pretreat_future_contract_info_data()
        self.pret_gfex.save_future_contract_info_data(df)
        mdf, odf = self.pret_gfex.pretreat_option_md_data()
        self.pret_gfex.save_option_md_data(mdf)
        self.pret_gfex.save_option_summary_data(odf)
        df = self.pret_gfex.pretreat_future_md_data()
        self.pret_gfex.save_future_md_data(df)
        res = self.pret_gfex.pretreat_future_position_rank_data(start_with_last=True)
        self.pret_gfex.save_future_position_rank_data(res)
        res = self.pret_gfex.pretreat_option_position_rank_data(start_with_last=True)
        self.pret_gfex.save_option_position_rank_data(res)

        if not initiation:
            for iv_range_data in self.pret_local.pretreat_daily_iv_range_data():
                self.pret_local.save_daily_iv_range_data(iv_range_data)
            self.pret_local.del_past_iv_record_data()
            for filtered_order_data in self.pret_local.pretreat_filtered_orders_data():
                self.pret_local.save_filtered_orders_data(filtered_order_data)

        df = self.pret_100ppi.pretreat_standard_spot_price_data()
        self.pret_100ppi.save_standard_spot_price_data(df)

    def start_process(self):
        for e in self.exchange_list:
            for r in self.pro_fut_cn_md.entry_process_main_roll_calendar(
                    process_from_all=True, save_to_all=True, start_with_last=True,
                    data_source=e
            ):
                self.pro_fut_cn_md.save_main_roll_calendar(*r)
        for e in self.exchange_list:
            for i in self.pro_fut_cn_md.entry_process_main_continuous_md_data_no_adjust(
                    timeframe='1d', process_from_all=True,
                    save_to_all=True, start_with_last=True, data_source=e
            ):
                self.pro_fut_cn_md.save_continuous_main_md_data(*i)

        for e in self.exchange_list:
            df = self.pro_fut_cn_meta.process_contract_info_from_exchanges(e)
            self.pro_fut_cn_meta.save_contract_info(df, e)
            df = self.pro_fut_cn_meta.process_spec_info_from_exchanges(e)
            self.pro_fut_cn_meta.save_spec_info(df, e)

        hist_trading_date_df = self.pro_fut_cn_meta.process_hist_trading_date()
        self.pro_fut_cn_meta.save_hist_trading_date(hist_trading_date_df)

        for e in self.exchange_list:
            for r in self.pro_fut_cn_trade.process_future_net_position_data(e, "contract"):
                self.pro_fut_cn_trade.save_future_net_position_data(r, e, "contract")
            for r in self.pro_fut_cn_trade.process_future_net_position_by_symbol(e):
                self.pro_fut_cn_trade.save_future_net_position_data(r, e, "symbol")
            for r in self.pro_fut_cn_trade.process_historical_volatility(e):
                self.pro_fut_cn_trade.save_historical_volatility_data(r, e)

        for e in self.exchange_list:
            df = self.pro_opt_cn_meta.process_contract_info_from_exchanges(e)
            self.pro_opt_cn_meta.save_contract_info(df, e)
            df = self.pro_opt_cn_meta.process_spec_info(e)
            self.pro_opt_cn_meta.save_spec_info(df, e)

        for e in self.exchange_list:
            for s in ['symbol', 'contract']:
                for r in self.pro_opt_cn_trade.process_option_net_position_data(e, s):
                    self.pro_opt_cn_trade.save_option_net_position_data(r, e, s)

        for e in self.exchange_list:
            pnl_dist = self.pro_fut_cn_md.process_pnl_distribute_data(start_with_last=True, exchange=e)
            self.pro_fut_cn_md.save_pnl_distribute_data(pnl_dist, e)


if __name__ == "__main__":
    b = DataUpdateBaseCls()
    # b.start_collect_czce()
    # b.start_collect_dce()
    # b.start_collect_dce()
    b.start_pretreat(initiation=False)
    b.start_process()