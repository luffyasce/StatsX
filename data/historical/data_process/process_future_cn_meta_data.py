"""
process method for meta data
"""
import re
import pandas as pd
import numpy as np
from typing import Union
from datetime import datetime, time, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ProcessFutureCnMeta:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')
        conf = Config()
        self.exchange_list = conf.exchange_list

    def process_contract_info_from_exchanges(self, exchange: str, df: Union[pd.DataFrame, None] = None):
        if df is None:
            current_dt = datetime.now().strftime("%Y-%m-%d")
            filter_ = {
                "last_trading_date": {
                    "gte": current_dt
                }
            }
            df = self.base.read_dataframe(
                "pretreated_future_cn_meta_data",
                f"contract_info_{exchange}",
                filter_datetime=filter_
            )
        else:
            pass
        if df.empty:
            return pd.DataFrame()
        df = df[['symbol', 'exchange', 'contract', 'listed_date', 'last_trading_date']].copy()
        return df

    def save_contract_info(self, df: pd.DataFrame, data_source: str):
        self.base.insert_dataframe(
            df,
            db_name='processed_future_cn_meta_data',
            tb_name=f'contract_info_{data_source}',
            set_index=['listed_date', 'contract', 'exchange'],
            partition=['listed_date']
        )

    def process_spec_info_from_exchanges(self, exchange: str, df: Union[pd.DataFrame, None] = None):
        if df is None:
            current_dt = datetime.now().strftime("%Y-%m-%d")
            filter_ = {
                "last_trading_date": {
                    "gte": current_dt
                }
            }
            df = self.base.read_dataframe(
                "pretreated_future_cn_meta_data",
                f"contract_info_{exchange}",
                filter_datetime=filter_
            )
        else:
            pass
        if df.empty:
            return pd.DataFrame()
        if 'trade_unit' not in df.columns:
            df = df[[
                'symbol', 'exchange'
            ]].copy().drop_duplicates(subset=['symbol', 'exchange']).assign(
                trade_unit=None, tick_price=None
            )
            current_record = self.base.read_dataframe(
                "processed_future_cn_meta_data", f"spec_info_{exchange}"
            )
            merge_results = pd.merge(df, current_record, on='symbol', how='outer', indicator=True)
            missing_df = merge_results[merge_results['_merge'] == 'left_only']
            missing_df = missing_df.drop(columns=['_merge'])
            if missing_df.empty:
                pass
            else:
                print(
                    f"{exchange} spec info (tick & unit) missing, please manually fill these in database. "
                    f"Table: processed_future_cn_meta_data.spec_info_{exchange}"
                )
            return missing_df
        else:
            df = df[[
                'symbol', 'exchange', 'trade_unit', 'tick_price'
            ]].copy().drop_duplicates(subset=['symbol', 'exchange'])
            return df

    def save_spec_info(self, df: pd.DataFrame, source_name: str):
        self.base.insert_dataframe(
            df,
            db_name='processed_future_cn_meta_data',
            tb_name=f'spec_info_{source_name}',
            set_index=['symbol', 'exchange']
        )

    def process_hist_trading_date(self):
        last_ = self.base.read_dataframe(
            "processed_future_cn_meta_data",
            "hist_trading_date_DIY",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        if last_.empty:
            filt_ = None
        else:
            filt_ = {'trading_date': {'gte': last_['trading_date'].max()}}
        df = pd.DataFrame()
        for e in self.exchange_list:
            tdf = self.base.read_dataframe(
                "pretreated_future_cn_md_data",
                f"all_1d_{e}",
                filter_datetime=filt_
            )
            if tdf.empty:
                continue
            df = pd.concat([df, tdf[['trading_date']].drop_duplicates()], axis=0)
        df = df.drop_duplicates().sort_values(by='trading_date', ascending=False).reset_index(drop=True)
        return df

    def save_hist_trading_date(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_meta_data",
            "hist_trading_date_DIY",
            set_index=['trading_date']
        )


if __name__ == "__main__":
    p = ProcessFutureCnMeta()
    for e in ['SHFE']:
        # df = p.process_contract_info_from_exchanges(e)
        # p.save_contract_info(df, e)
        df = p.process_spec_info_from_exchanges(e)
        p.save_spec_info(df, e)

    # df = p.process_hist_trading_date()
    # p.save_hist_trading_date(df)