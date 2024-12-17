"""
process method for meta data
"""
from datetime import datetime
import re
import pandas as pd
import numpy as np
from typing import Union
import utils.database.unified_db_control as udc

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ProcessOptionCnMeta:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def process_contract_info_from_exchanges(self, exchange: str, df: Union[pd.DataFrame, None] = None):
        if df is None:
            current_dt = datetime.now().strftime("%Y-%m-%d")
            filter_ = {
                "last_trading_date": {
                    "gte": current_dt
                }
            }
            df = self.base.read_dataframe(
                "pretreated_option_cn_meta_data",
                f"contract_info_{exchange}",
                filter_datetime=filter_
            )
        else:
            pass
        if df.empty:
            return pd.DataFrame()
        df = df[[
            'symbol', 'exchange', 'contract', 'listed_date', 'last_trading_date', 'underlying_contract',
            'direction', 'strike_price'
        ]].copy()
        return df

    def save_contract_info(self, df: pd.DataFrame, source_name: str):
        self.base.insert_dataframe(
            df,
            db_name='processed_option_cn_meta_data',
            tb_name=f'contract_info_{source_name}',
            set_index=['listed_date', 'contract', 'exchange'],
            partition=['listed_date']
        )

    def process_spec_info(self, exchange: str, df: Union[pd.DataFrame, None] = None):
        if df is None:
            current_dt = datetime.now().strftime("%Y-%m-%d")
            filter_ = {
                "last_trading_date": {
                    "gte": current_dt
                }
            }
            df = self.base.read_dataframe(
                "pretreated_option_cn_meta_data",
                f"contract_info_{exchange}",
                filter_datetime=filter_
            )
        else:
            pass
        if df.empty:
            return pd.DataFrame()
        df = df[[
            'symbol', 'exchange', 'trade_unit', 'tick_price'
        ]].copy().drop_duplicates(subset=['symbol', 'exchange'])
        return df

    def save_spec_info(self, df: pd.DataFrame, source_name: str):
        self.base.insert_dataframe(
            df,
            db_name='processed_option_cn_meta_data',
            tb_name=f'spec_info_{source_name}',
            set_index=['symbol', 'exchange']
        )


if __name__ == "__main__":
    pr = ProcessOptionCnMeta()
    for e in ['GFEX']:
        df = pr.process_contract_info_from_exchanges(e)
        pr.save_contract_info(df, e)
        df = pr.process_spec_info(e)
        pr.save_spec_info(df, e)
