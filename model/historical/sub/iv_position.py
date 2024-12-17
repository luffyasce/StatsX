import os
from collections import Counter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.database.unified_db_control import UnifiedControl
from data.data_utils.data_standardization import logarithm_change
from utils.tool.configer import Config
import matplotlib.pyplot as plt
import seaborn as sns


"""
historical iv position of each underlying contract
"""


class IVPosition:
    def __init__(self, exchange: str):
        """
        by default, use all the available historical data of currently trading contracts.
        """
        self.exchange = exchange
        self.udc = UnifiedControl(db_type='base')
        self.now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    @property
    def all_currently_trading_contract_df(self):
        df = self.udc.read_dataframe(
            "processed_option_cn_meta_data", f"contract_info_{self.exchange}",
            filter_datetime={'last_trading_date': {'gte': self.now.strftime('%Y-%m-%d')}}
        )
        return df

    @property
    def md_data(self):
        df = self.udc.read_dataframe(
            "pretreated_future_cn_md_data", f"all_1d_{self.exchange}",
            filter_datetime={
                'trading_date': {'eq': self.date.strftime('%Y-%m-%d')}
            }
        )
        return df

    def get_iv_data(self, contract_df):
        opt_contracts = contract_df['contract'].drop_duplicates().tolist()
        df = self.udc.read_dataframe(
            "pretreated_option_cn_md_data", f"all_1d_opt_summary_{self.exchange}",
            filter_keyword={'contract': {'in': opt_contracts}}
        )
        iv_df = df.groupby(['underlying_contract', 'trading_date'])[['iv']].mean()
        iv_df = iv_df.reset_index(drop=False)
        iv_df = iv_df.assign(symbol=iv_df['underlying_contract'].str[:-4])
        return iv_df

    def run(self):
        contracts = self.all_currently_trading_contract_df
        iv_df = self.get_iv_data(contracts)
        res_dict = {}
        for s, v in iv_df.groupby('underlying_contract'):
            latest_iv = v[v['trading_date'] == v['trading_date'].max()].iloc[0]['iv']
            iv_pos = (latest_iv - v['iv'].min()) / (v['iv'].max() - v['iv'].min())
            res_dict[s] = iv_pos
        res_s = pd.Series(res_dict)
        return iv_df, res_s


def file_location(*args, filename: str):
    pth = Config().path
    folder_pth = os.path.join(
        pth, os.sep.join(['front_end', 'page', 'static', 'model', 'visual_outputs', 'iv_position', *args])
    )
    if not os.path.exists(folder_pth):
        os.makedirs(folder_pth)
    return os.path.join(
        folder_pth, filename
    )


def plot_and_save_iv_position_data():
    exchange_ls = Config().exchange_list
    iv_df, iv_pos_series = pd.DataFrame(), pd.Series(dtype=float)
    for e in exchange_ls:
        ivp = IVPosition(e)
        ivd, ivps = ivp.run()
        iv_df = pd.concat([iv_df, ivd], axis=0)
        iv_pos_series = pd.concat([iv_pos_series, ivps])

    iv_pos_series = pd.DataFrame(iv_pos_series.rename('iv_position'))
    iv_pos_series = iv_pos_series.assign(symbol=iv_pos_series.index.to_series().str[:-4]).reset_index(drop=False, names=['contract'])

    iv_pos_avg = iv_pos_series.groupby('symbol')['iv_position'].mean().round(2)

    for s, v in iv_df.groupby('symbol'):
        iv_pos_data = iv_pos_series[iv_pos_series['symbol'] == s].copy()

        fig, (ax, ax2) = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [2, 1]}, figsize=(10, 6))
        for contract in v['underlying_contract'].unique():
            sub_v = v[v['underlying_contract'] == contract]
            ax.plot(sub_v.set_index('trading_date')['iv'], label=contract)

        iv_pos_data.set_index('contract')[['iv_position']].round(2).plot(kind='bar', y='iv_position', ax=ax2)

        ax.set_title('Implied Volatility Over Time by Contract')
        ax.set_xlabel('Trading Date')
        ax.set_ylabel('Implied Volatility (IV)')
        ax.legend(title='Contract')

        ax2.set_title('Implied Volatility Position')
        ax2.set_xlabel('Contracts')
        ax2.set_ylabel('Implied Volatility Position')
        ax2.legend(title='Contract')

        # Rotate the x-axis labels for better readability
        plt.xticks(rotation=45)
        # Save the plot
        plt.tight_layout()
        plt.savefig(file_location(filename=f"{s}.jpeg"))

    fig, ax0 = plt.subplots(figsize=(10, 6))
    iv_pos_avg.sort_values(ascending=True).plot(kind='bar', ax=ax0)
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig(file_location(filename="iv_position_overview.jpeg"))

    plt.close()


if __name__ == "__main__":
    plot_and_save_iv_position_data()