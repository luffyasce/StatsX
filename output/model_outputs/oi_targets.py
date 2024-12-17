import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from utils.tool.configer import Config

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

OI_SAMPLE_LENGTH = 5
OI_SAMPLE_FITTING_LIMIT = 0.95   # 当前oi距离区间最大oi差距不超过5%


class OiTargets:
    def __init__(self, analyse_date: datetime):
        self.date = analyse_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.udc = UnifiedControl(db_type='base')
        config = Config()
        self.exchange_ls = config.exchange_list
        self.rds = Redis()

        self.oi_sample_start_date = self.get_oi_sample_start_trading_date()
        self.oi_sample_start_date = self.oi_sample_start_date if self.oi_sample_start_date is not None else self.date

    def md_data(self, exchange: str):
        df = self.udc.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{exchange}",
            filter_datetime={
                'trading_date': {'eq': self.date.strftime('%Y-%m-%d')}
            }
        )
        return df

    def get_oi_sample_start_trading_date(self):
        df = self.udc.read_dataframe(
            "processed_future_cn_meta_data", "hist_trading_date_DIY",
            ascending=[('trading_date', False)],
            filter_row_limit=OI_SAMPLE_LENGTH
        )
        if df.empty:
            return None
        else:
            return df['trading_date'].min()

    def sort_out_main_and_sub_main_contracts(self, exchange: str):
        md = self.md_data(exchange)
        if md.empty:
            return None
        mains_md = md.sort_values(by='open_interest', ascending=False).groupby('symbol').head(2)
        return mains_md['contract'].tolist()

    def oi_sample_data(self, exchange: str):
        df = self.udc.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{exchange}",
            filter_datetime={
                'trading_date': {'gte': self.oi_sample_start_date.strftime('%Y-%m-%d')}
            },
        )
        return df

    def analyse_oi_level(self, exchange: str):
        mains_and_subs = self.sort_out_main_and_sub_main_contracts(exchange)
        if mains_and_subs is None:
            return
        sample_data = self.oi_sample_data(exchange).fillna(0)
        res_s = pd.Series(dtype=str, name='cnt')
        for c, sample in sample_data.groupby('contract'):
            if np.count_nonzero(sample['open_interest']) < OI_SAMPLE_LENGTH * 0.8:
                continue
            curr_oi = sample.loc[sample['trading_date'] == sample['trading_date'].max(), 'open_interest'].iloc[0]
            max_oi = sample['open_interest'].max()
            min_oi = sample['open_interest'].min()
            if max_oi - min_oi == 0:
                continue
            oi_level = (curr_oi - min_oi) / (max_oi - min_oi)
            if oi_level > OI_SAMPLE_FITTING_LIMIT:
                res_s[c] = oi_level
        all_contract_result = pd.DataFrame(res_s).assign(symbol=res_s.index.to_series().str[:-4]).reset_index(drop=False, names=['contract'])
        symbol_restraints = all_contract_result[all_contract_result['contract'].isin(mains_and_subs)]['symbol'].drop_duplicates().tolist()
        result = all_contract_result[all_contract_result['symbol'].isin(symbol_restraints)]
        return result

    def run_and_buff_oi_targets(self):
        res_df = pd.DataFrame()
        for e in self.exchange_ls:
            r_ = self.analyse_oi_level(e)
            if r_ is None:
                continue
            res_df = pd.concat([res_df, r_], axis=0)
        self.rds.set_key(db=1, k="oi_targets", v=self.rds.encode_dataframe(res_df))


if __name__ == "__main__":
    o = OiTargets(datetime(2023, 11, 6))
    o.run_and_buff_oi_targets()
