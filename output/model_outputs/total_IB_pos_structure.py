import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import dataframe_image
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from model.tool.calculus_layers.pandas_calculus import ScalingCalculus
from utils.tool.configer import Config
from infra.tool.rules import TradeRules
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class TotIBPosStructure:
    def __init__(self, date: datetime):
        self.dt = date.strftime("%Y%m%d%H%M%S")
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.conf = Config()
        self.udc = UnifiedControl(db_type='base')
        self.rds = Redis()

        self.targets = TradeRules().targets
        self.exchange_ls = self.conf.exchange_list

    def save_model_outcome(self, k: str, v: pd.DataFrame):
        self.rds.set_key(db=1, k=k, v=self.rds.encode_dataframe(v))

    def save_model_complete_dt(self):
        self.rds.set_hash(db=1, name='model_dt', k=self.__class__.__name__, v=self.dt)

    def file_location(self, *args, filename: str):
        folder_pth = os.path.join(
            self.conf.path, os.sep.join(['front_end', 'page', 'static', 'model', 'visual_outputs', 'total_position_structure', *args])
        )
        if not os.path.exists(folder_pth):
            os.makedirs(folder_pth)
        return os.path.join(
            folder_pth, filename
        )

    @property
    def informed_total(self):
        return self.udc.read_dataframe(
            "processed_future_cn_model_data", "informed_broker_total_position_SUM",
            filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}}
        )

    @property
    def uninformed_total(self):
        return self.udc.read_dataframe(
            "processed_future_cn_model_data", "uninformed_broker_total_position_SUM",
            filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}}
        )

    def total_up(self):
        idf = self.informed_total
        if idf.empty:
            idf.columns = ['informed_total', 'informed_chg']
        else:
            idf = idf.drop(columns=['trading_date']).set_index(
                'symbol'
            ).rename(
                columns={'net_pos': 'informed_total', 'net_chg': 'informed_chg'}
            )
        udf = self.uninformed_total
        if udf.empty:
            udf.columns = ['uninformed_total', 'uninformed_chg']
        else:
            udf = udf.drop(columns=['trading_date']).set_index(
                'symbol'
            ).rename(
                columns={'net_pos': 'uninformed_total', 'net_chg': 'uninformed_chg'}
            )
        df = pd.concat([idf, udf], axis=1).fillna(0)
        df = df.assign(
            net_total=df['informed_total'] - df['uninformed_total'],
            net_chg=df['informed_chg'] - df['uninformed_chg']
        ).sort_values(by='net_total', ascending=False)
        return df

    def daily_brokers(self, sample_limit=10):
        df = self.udc.read_dataframe(
            "processed_future_cn_model_data", "broker_position_information_score_SUM",
            filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}}
        )
        if df.empty:
            return
        else:
            sample_winners = df[df['information_score'] > 0].sort_values(by='information_score', ascending=False).head(sample_limit)
            sample_losers = df[df['information_score'] < 0].sort_values(by='information_score', ascending=True).head(sample_limit)
            winners = sample_winners['broker']
            losers = sample_losers['broker']
            sample_total = pd.concat([sample_winners, sample_losers], axis=0).sort_values(by='information_score', ascending=False)
            sample_total = sample_total.drop(columns=['trading_date'])
            return winners, losers, sample_total

    def get_broker_position(self, broker: str, by: str):
        df = pd.DataFrame()
        for e in self.exchange_ls:
            data_e = self.udc.read_dataframe(
                "processed_future_cn_trade_data", f"net_position_by_{by}_{e}",
                filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}},
                filter_keyword={'broker': {'eq': broker}},
                filter_columns=['net_pos', 'net_chg', by]
            )
            if data_e.empty:
                continue
            if by == 'contract':
                cor_df = self.udc.read_dataframe(
                    "pretreated_future_cn_model_data", f"valid_position_by_{by}_{e}",
                    filter_datetime={'trading_date': {'eq': self.date.strftime("%Y-%m-%d")}},
                    filter_keyword={'broker': {'eq': broker}},
                    filter_columns=['net_pos_corr', by]
                )
                if cor_df.empty:
                    continue
                data_e.set_index(by, inplace=True)
                cor_df.set_index(by, inplace=True)
                data_e['net_pos_corr'] = cor_df.loc[cor_df.index.intersection(data_e.index)]['net_pos_corr'].reindex(data_e.index)
                data_e.reset_index(drop=False, inplace=True)
            df = pd.concat([df, data_e], axis=0)
        return df

    def sort_out_distinguished_broker_position_details(self):
        brokers = self.daily_brokers()
        if brokers is None:
            return
        winners, losers, vip_brokers = brokers

        self.save_model_outcome('vip_ranking', vip_brokers)

        def __pos_detail_proc__(b):
            cdf = self.get_broker_position(b, 'contract')
            sdf = self.get_broker_position(b, 'symbol')
            cdf = cdf.assign(symbol=cdf['contract'].str.extract("([A-Z]+)")).rename(
                columns={'net_pos': 'c_net_pos', 'net_chg': 'c_net_chg'}
            )
            rk_df = cdf.groupby('symbol', group_keys=False).apply(lambda x: x.assign(Rank=x['c_net_pos'].abs().rank(method='dense', ascending=False)))
            cdf = rk_df[rk_df['Rank'] <= 3].drop(columns=['Rank']).set_index('symbol')      # 每个品种只取净持仓排名前三的合约
            sdf.set_index('symbol', inplace=True)
            cdf[['net_pos', 'net_chg']] = sdf.loc[sdf.index.intersection(cdf.index)][['net_pos', 'net_chg']].reindex(
                cdf.index)
            cdf = cdf.reset_index(drop=False, names=['symbol']).dropna(subset=['net_pos_corr'])
            cdf = cdf[cdf['symbol'].isin(self.targets)].copy().assign(broker=b)
            return cdf

        wdf = pd.DataFrame()
        for w in winners:
            res = __pos_detail_proc__(w)
            wdf = pd.concat([wdf, res], axis=0)
        wdf['broker'] = 'W' + wdf['broker']
        ldf = pd.DataFrame()
        for l in losers:
            res = __pos_detail_proc__(l)
            ldf = pd.concat([ldf, res], axis=0)
        ldf['broker'] = 'L' + ldf['broker']

        wldf = pd.concat([wdf, ldf], axis=0)

        wldf = wldf.sort_values(
            by=['symbol', 'contract', 'net_pos_corr', 'broker'],
            ascending=[True, True, False, False]
        ).set_index(['symbol', 'contract', 'net_pos_corr'])[['net_pos', 'net_chg', 'c_net_pos', 'c_net_chg', 'broker']]

        self.save_model_outcome('wl_struct', wldf)
        wldf.to_csv(self.file_location(self.dt, filename=f"wl_struct.csv"))

    def plot_and_save(self, df: pd.DataFrame):
        pos_df = df[df['net_total'] > 0].copy()
        neg_df = df[df['net_total'] < 0].copy()

        fig, (ax0, ax1) = plt.subplots(
            nrows=2, ncols=1, gridspec_kw={'height_ratios': [1, 1]},
            figsize=(16, 24)
        )
        pos_df[['net_total']].plot(kind='pie', y='net_total', ax=ax0)
        neg_df[['net_total']].abs().plot(kind='pie', y='net_total', ax=ax1)
        ax0.legend(loc='upper left')
        ax1.legend(loc='upper left')
        ax0.title.set_text("LongPosition")
        ax1.title.set_text("ShortPosition")
        ax0.grid(True)
        ax1.grid(True)
        plt.tight_layout()
        plt.savefig(
            self.file_location(self.dt, filename="net_total.jpeg")
        )
        plt.close()

        df.to_csv(self.file_location(self.dt, filename="position_detail.csv"))

        self.save_model_complete_dt()


if __name__ == "__main__":
    tib = TotIBPosStructure(datetime(2024, 9, 10))
    df = tib.total_up()
    tib.plot_and_save(df)
    tib.sort_out_distinguished_broker_position_details()