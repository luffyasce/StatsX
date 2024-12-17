from collections import Counter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.database.unified_db_control import UnifiedControl
from data.data_utils.data_standardization import logarithm_change
from model.tool.technicals import technical_indicators as ti
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ValidBrokerPosAnalyse:
    def __init__(self, end_date: datetime):
        self.end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.udc = UnifiedControl(db_type='base')
        config = Config()
        self.exchange_ls = config.exchange_list

    def brokers_sum_up(self, tb_key: str):
        xdf = pd.DataFrame()
        for t in [
            i for i in self.udc.get_table_names("pretreated_future_cn_model_data") if tb_key in i
        ]:
            df = self.udc.read_dataframe(
                "pretreated_future_cn_model_data", t,
                filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}}
            )
            if df.empty:
                continue
            else:
                df = df.set_index('broker').drop(columns=['trading_date'])
                xdf = pd.concat([xdf, df], axis=1)
        xdf = xdf.fillna(0)
        res = pd.DataFrame()
        for c in xdf.columns.to_series().drop_duplicates():
            res = pd.concat([res, xdf[c].sum(axis=1).rename(c)], axis=1)

        res = res.assign(trading_date=self.end).reset_index(drop=False, names=['broker'])
        return res

    def save_summed_broker_score_data(self, df: pd.DataFrame, tb_key: str):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", f"{tb_key}_SUM",
            set_index=['broker', 'trading_date'], partition=['trading_date']
        )

    """main contract"""
    def main_contract_valid_position_analyse(self):
        for t in self.exchange_ls:
            rd = self.udc.read_dataframe(
                "processed_future_cn_roll_data", f"all_main_{t}",
                filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
                filter_columns=['O_NM_N']
            )
            if rd.empty:
                continue
            else:
                main_contracts = rd['O_NM_N'].tolist()
                df = self.udc.read_dataframe(
                    "pretreated_future_cn_model_data", f"valid_position_by_contract_{t}",
                    filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
                    filter_keyword={'contract': {'in': main_contracts}}
                )
                if df.empty:
                    continue
                vdf = df.groupby('contract')[['valid_net_pos']].sum()
                vdf['informed_net_pos'] = df[df['net_pos_corr'] > 0].groupby('contract')['net_pos'].sum()
                vdf['informed_corr_avg'] = df[df['net_pos_corr'] > 0].groupby('contract')['net_pos_corr'].mean()
                vdf['uninformed_net_pos'] = df[df['net_pos_corr'] < 0].groupby('contract')['net_pos'].sum()
                vdf['uninformed_corr_avg'] = df[df['net_pos_corr'] < 0].groupby('contract')['net_pos_corr'].mean()
                vdf = vdf.assign(
                    trading_date=self.end,
                    symbol=vdf.index.to_series().apply(lambda x: x[:-4])
                ).reset_index(names=['contract'])
                yield vdf

    def save_main_contract_valid_position_analyse_result(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", "valid_position_by_main_contract_SUM",
            set_index=['trading_date', 'contract', 'symbol'], partition=['trading_date']
        )

    def main_contract_valid_pos_chg_analyse(self):
        for t in self.exchange_ls:
            rd = self.udc.read_dataframe(
                "processed_future_cn_roll_data", f"all_main_{t}",
                filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
                filter_columns=['O_NM_N']
            )
            if rd.empty:
                continue
            else:
                main_contracts = rd['O_NM_N'].tolist()
                df = self.udc.read_dataframe(
                    "pretreated_future_cn_model_data", f"valid_pos_chg_by_contract_{t}",
                    filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
                    filter_keyword={'contract': {'in': main_contracts}}
                )
                if df.empty:
                    continue
                vdf = df.groupby('contract')[['valid_net_chg']].sum()
                vdf['informed_net_chg'] = df[df['net_chg_corr'] > 0].groupby('contract')['net_chg'].sum()
                vdf['informed_corr_avg'] = df[df['net_chg_corr'] > 0].groupby('contract')['net_chg_corr'].mean()
                vdf['uninformed_net_chg'] = df[df['net_chg_corr'] < 0].groupby('contract')['net_chg'].sum()
                vdf['uninformed_corr_avg'] = df[df['net_chg_corr'] < 0].groupby('contract')['net_chg_corr'].mean()
                vdf = vdf.assign(
                    trading_date=self.end,
                    symbol=vdf.index.to_series().apply(lambda x: x[:-4])
                ).reset_index(names=['contract'])
                yield vdf

    def save_main_contract_valid_pos_chg_analyse_result(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", "valid_pos_chg_by_main_contract_SUM",
            set_index=['trading_date', 'contract', 'symbol'], partition=['trading_date']
        )

    """by symbol total contract"""
    def symbol_valid_position_analyse(self):
        for t in self.exchange_ls:
            df = self.udc.read_dataframe(
                "pretreated_future_cn_model_data", f"valid_position_by_contract_{t}",
                filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
            )
            if df.empty:
                continue
            vdf = df.groupby('symbol')[['valid_net_pos']].sum()
            vdf['informed_net_pos'] = df[df['net_pos_corr'] > 0].groupby('symbol')['net_pos'].sum()
            vdf['informed_corr_avg'] = df[df['net_pos_corr'] > 0].groupby('symbol')['net_pos_corr'].mean()
            vdf['uninformed_net_pos'] = df[df['net_pos_corr'] < 0].groupby('symbol')['net_pos'].sum()
            vdf['uninformed_corr_avg'] = df[df['net_pos_corr'] < 0].groupby('symbol')['net_pos_corr'].mean()
            vdf = vdf.assign(
                trading_date=self.end,
            ).reset_index(names=['symbol'])
            yield vdf

    def save_symbol_valid_position_analyse_result(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", "valid_position_by_symbol_SUM",
            set_index=['trading_date', 'symbol'], partition=['trading_date']
        )

    def symbol_valid_pos_chg_analyse(self):
        for t in self.exchange_ls:
            df = self.udc.read_dataframe(
                "pretreated_future_cn_model_data", f"valid_pos_chg_by_contract_{t}",
                filter_datetime={'trading_date': {'eq': self.end.strftime("%Y-%m-%d")}},
            )
            if df.empty:
                continue
            vdf = df.groupby('symbol')[['valid_net_chg']].sum()
            vdf['informed_net_chg'] = df[df['net_chg_corr'] > 0].groupby('symbol')['net_chg'].sum()
            vdf['informed_corr_avg'] = df[df['net_chg_corr'] > 0].groupby('symbol')['net_chg_corr'].mean()
            vdf['uninformed_net_chg'] = df[df['net_chg_corr'] < 0].groupby('symbol')['net_chg'].sum()
            vdf['uninformed_corr_avg'] = df[df['net_chg_corr'] < 0].groupby('symbol')['net_chg_corr'].mean()
            vdf = vdf.assign(
                trading_date=self.end,
            ).reset_index(names=['symbol'])
            yield vdf

    def save_symbol_valid_pos_chg_analyse_result(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "processed_future_cn_model_data", "valid_pos_chg_by_symbol_SUM",
            set_index=['trading_date', 'symbol'], partition=['trading_date']
        )


if __name__ == "__main__":
    from utils.tool.datetime_wrangle import yield_dates

    for t in yield_dates(datetime(2023, 8, 16), datetime(2023, 8, 17)):
        ipa = ValidBrokerPosAnalyse(t)
        r = ipa.brokers_sum_up("broker_position_information_score")
        ipa.save_summed_broker_score_data(r, "broker_position_information_score")
        r = ipa.brokers_sum_up("broker_pos_chg_information_score")
        ipa.save_summed_broker_score_data(r, "broker_pos_chg_information_score")
        for r in ipa.main_contract_valid_position_analyse():
            ipa.save_main_contract_valid_position_analyse_result(r)
        for r in ipa.main_contract_valid_pos_chg_analyse():
            ipa.save_main_contract_valid_pos_chg_analyse_result(r)
        for r in ipa.symbol_valid_position_analyse():
            ipa.save_symbol_valid_position_analyse_result(r)
        for r in ipa.symbol_valid_pos_chg_analyse():
            ipa.save_symbol_valid_pos_chg_analyse_result(r)