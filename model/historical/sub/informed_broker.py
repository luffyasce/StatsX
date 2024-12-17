from collections import Counter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.database.unified_db_control import UnifiedControl
from data.data_utils.data_standardization import logarithm_change
from model.tool.technicals import technical_indicators as ti

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


sample_periods = [30, 60, 90, 120]


class InformedBroker:
    def __init__(self, end_date: datetime, exchange: str):
        self.end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.exchange = exchange
        self.udc = UnifiedControl(db_type='base')

    def date_back_trading_days(self, sample_period: int):
        tdf = self.udc.read_dataframe(
            "processed_future_cn_meta_data",
            "hist_trading_date_DIY",
            filter_datetime={'trading_date': {'lte': self.end.strftime('%Y-%m-%d')}},
            ascending=[('trading_date', False)],
            filter_row_limit=sample_period
        )
        start_date = tdf['trading_date'].min()
        return start_date

    def md_data(self, sample_period: int):
        df = self.udc.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{self.exchange}",
            filter_datetime={
                'trading_date': {'gte': self.date_back_trading_days(sample_period).strftime('%Y-%m-%d'), 'lte': self.end.strftime('%Y-%m-%d')}
            }
        )
        return df

    def net_position_by_contract_data(self, sample_period: int):
        df = self.udc.read_dataframe(
            "processed_future_cn_trade_data", f"net_position_by_contract_{self.exchange}",
            filter_datetime={
                'trading_date': {'gte': self.date_back_trading_days(sample_period).strftime('%Y-%m-%d'), 'lte': self.end.strftime('%Y-%m-%d')}
            }
        )
        return df

    def yield_contract_ret(self, sample_period: int):
        for contract, df in self.md_data(sample_period).groupby('contract'):
            if not df.empty:
                df = df.set_index('trading_date').sort_index(ascending=True)
                df['ret'] = logarithm_change(df, 'close')
                ret_s = df['ret'].fillna(0) * 100
                ret_cumsum_s = ret_s.cumsum()
                yield contract, ret_s, ret_cumsum_s

    def calc_corr_by_contract(self):
        tot_sample_res = pd.DataFrame()
        for sample_period in sample_periods:
            res = pd.DataFrame()
            pos_df = self.net_position_by_contract_data(sample_period)
            if pos_df.empty:
                continue
            for c, ret, ret_cum in self.yield_contract_ret(sample_period):
                pdf = pos_df[pos_df['contract'] == c].copy()
                if pdf.empty:
                    continue
                for broker, bdf in pdf.groupby('broker'):
                    bdf = bdf.set_index('trading_date').sort_index(ascending=True)
                    # net pos net chg shifted to match next return
                    res_df = pd.concat(
                        [bdf[['net_pos', 'net_chg']].shift(), ret.rename('r1'), ret_cum.rename('r2')], axis=1
                    )
                    latest_date = res_df.index.max()
                    cleaned_res_df = res_df.dropna(subset=['net_pos', 'net_chg'])
                    if len(cleaned_res_df) < 5 or cleaned_res_df.index.max() != latest_date:
                        # 如果有效样本数量太小，或者样本没有最新持仓数据，则跳过
                        continue
                    npc = res_df['net_pos'].corr(res_df['r2'])
                    ncc = res_df['net_chg'].corr(res_df['r1'])
                    result_s = pd.DataFrame.from_dict(
                        data={
                            'trading_date': res_df.index.max(),
                            'contract': c,
                            'broker': broker,
                            'net_pos': res_df.loc[res_df.index.max(), 'net_pos'],
                            'net_pos_corr': npc,
                            'net_chg': res_df.loc[res_df.index.max(), 'net_chg'],
                            'net_chg_corr': ncc,
                            'period': sample_period
                        },
                        orient='index',
                    ).T
                    res = pd.concat([res, result_s], axis=0)
            tot_sample_res = pd.concat([tot_sample_res, res], axis=0)
        if tot_sample_res.empty:
            return
        result = tot_sample_res.groupby(['trading_date', 'contract', 'broker'])[['net_pos', 'net_pos_corr', 'net_chg', 'net_chg_corr']].mean().reset_index(drop=False)
        return result

    def save_pos_corr_by_contract(self, df: pd.DataFrame):
        self.udc.insert_dataframe(
            df, "raw_future_cn_model_data", f"position_correlation_by_contract_{self.exchange}",
            set_index=['trading_date', 'contract', 'broker']
        )

    def select_informed_broker_by_contract(self):
        df = self.udc.read_dataframe(
            "raw_future_cn_model_data", f"position_correlation_by_contract_{self.exchange}",
            filter_datetime={'trading_date': {'eq': self.end.strftime('%Y-%m-%d')}}
        )
        if df.empty:
            return
        df = df.assign(symbol=df['contract'].apply(lambda x: x[:-4]))
        df['valid_net_pos'] = df['net_pos'] * df['net_pos_corr']
        df['valid_net_chg'] = df['net_chg'] * df['net_chg_corr']

        def __proc__(df: pd.DataFrame, filter_key: str, informed_threshold: float = 0.6):
            xdf = df[df[filter_key].abs() >= informed_threshold].copy()
            xws = pd.Series(dict(Counter(xdf[xdf[filter_key] > 0]['broker'].tolist()))).rename("informed")
            xls = pd.Series(dict(Counter(xdf[xdf[filter_key] < 0]['broker'].tolist()))).rename("uninformed")
            x_inf = pd.concat([xws, xls], axis=1).fillna(0)
            x_inf = x_inf.assign(
                information_score=x_inf['informed'] - x_inf['uninformed'],
                trading_date=self.end
            ).sort_values(by='information_score').reset_index(names=['broker'])
            return xdf, x_inf

        pos_df, pos_sc_df = __proc__(df, 'net_pos_corr')
        chg_df, chg_sc_df = __proc__(df, 'net_chg_corr')

        return pos_df, pos_sc_df, chg_df, chg_sc_df

    def save_informed_broker_info_by_contract(
            self, pdf: pd.DataFrame, psc: pd.DataFrame, cdf: pd.DataFrame, csc: pd.DataFrame
    ):
        self.udc.insert_dataframe(
            pdf, "pretreated_future_cn_model_data", f"valid_position_by_contract_{self.exchange}",
            set_index=['trading_date', 'broker', 'contract'], partition=['trading_date']
        )
        self.udc.insert_dataframe(
            psc, "pretreated_future_cn_model_data", f"broker_position_information_score_{self.exchange}",
            set_index=['trading_date', 'broker'], partition=['trading_date']
        )
        self.udc.insert_dataframe(
            cdf, "pretreated_future_cn_model_data", f"valid_pos_chg_by_contract_{self.exchange}",
            set_index=['trading_date', 'broker', 'contract'], partition=['trading_date']
        )
        self.udc.insert_dataframe(
            csc, "pretreated_future_cn_model_data", f"broker_pos_chg_information_score_{self.exchange}",
            set_index=['trading_date', 'broker'], partition=['trading_date']
        )


if __name__ == "__main__":
    # ib = InformedBroker(datetime(2023, 7, 18), 'CZCE')
    # for res in ib.calc_corr_by_contract():
    #     ib.save_pos_corr_by_contract(res)

    ib = InformedBroker(datetime(2024, 7, 1), 'CZCE')
    ib.calc_corr_by_contract()