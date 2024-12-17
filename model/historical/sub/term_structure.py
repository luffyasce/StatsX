import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.database.unified_db_control import UnifiedControl
from data.data_utils.data_standardization import logarithm_change
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class TermStructure:
    def __init__(self, analyse_date: datetime, exchange: str):
        self.date = analyse_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.exchange = exchange
        self.udc = UnifiedControl(db_type='base')

    @property
    def md_data(self):
        df = self.udc.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{self.exchange}",
            filter_datetime={
                'trading_date': {'eq': self.date.strftime('%Y-%m-%d')}
            }
        )
        return df

    @property
    def multiplier(self):
        df = self.udc.read_dataframe(
            "processed_future_cn_meta_data", f"spec_info_{self.exchange}"
        )
        m = df.set_index('symbol')['trade_unit']
        return m

    def analyse_term_structure(self):
        md = self.md_data
        if md.empty:
            return
        md.set_index('symbol', inplace=True)
        md['multiplier'] = self.multiplier.loc[self.multiplier.index.intersection(md.index)].reindex(md.index)
        md['cap'] = md['close'] * md['open_interest'] * md['multiplier']
        md = md[md['cap'] >= 1e7].copy()
        md['monthCode'] = pd.to_datetime(md['contract'].apply(lambda x: x[-4:]), format='%y%m')
        result_df = pd.DataFrame(dtype=float)
        for s, dv in md.groupby(md.index):
            if len(dv) < 2:
                result_df.loc[s, 'term_structure_slope'] = np.nan
            else:
                dv.sort_values(by='contract', ascending=True, inplace=True)
                dv['monthCode'] = (dv['monthCode'] - dv.iloc[0]['monthCode']).dt.days + 1
                res = np.polyfit(
                    dv['monthCode'].tolist(),
                    dv['close'].tolist(),
                    deg=1
                )
                ts_res = res[0]
                result_df.loc[s, 'term_structure_slope'] = ts_res
        return result_df.reset_index(names=['symbol']).assign(trading_date=self.date)

    def save_term_structure(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "raw_future_cn_model_data", f"term_structure_{self.exchange}",
            set_index=['symbol', 'trading_date']
        )


if __name__ == "__main__":
    from utils.tool.datetime_wrangle import yield_dates
    for t in yield_dates(datetime(2023, 1, 1), datetime(2023, 7, 24)):
        for e in ['SHFE', 'CZCE', 'DCE']:
            ts = TermStructure(t, e)
            r = ts.analyse_term_structure()
            ts.save_term_structure(r)