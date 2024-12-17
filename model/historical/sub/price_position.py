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


SAMPLE_NATURAL_DAYS = 30        # 样本自然日 1个月


class PricePosition:
    def __init__(self, exchange: str):
        """
        by default, use all the available historical data of currently trading contracts.
        """
        self.exchange = exchange
        self.udc = UnifiedControl(db_type='base')
        self.now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.start = self.now + timedelta(days=-SAMPLE_NATURAL_DAYS)

    @property
    def all_currently_trading_option_symbols(self):
        df = self.udc.read_dataframe(
            "processed_option_cn_meta_data", f"contract_info_{self.exchange}",
            filter_datetime={'last_trading_date': {'gte': self.now.strftime('%Y-%m-%d')}}
        )
        ls = df['symbol'].drop_duplicates().tolist()
        return ls

    def run(self):
        md = self.udc.read_dataframe('processed_future_cn_md_data', f'all_1d_main_{self.exchange}',
                                 filter_keyword={'process_type': {'eq': 'O_M_N'}, 'symbol': {'in': self.all_currently_trading_option_symbols}},
                                 filter_datetime={'trading_date': {'gte': self.start.strftime('%Y-%m-%d')}})

        md_s = md.groupby('symbol')['high'].max()
        md_l = md.groupby('symbol')['low'].min()
        md_c = md.groupby('symbol')['close'].last()

        df = pd.concat([md_s, md_l, md_c], axis=1)

        df['high'] = df['high'] / df['low']
        df['close'] = df['close'] / df['low']
        df['low'] = 1
        df = df * 100

        df['pos_pctg'] = (((df['close'] - df['low']) / (df['high'] - df['low'])) * 100).round(1)
        df['range'] = df['high'] - df['low']
        return df


def file_location(*args, filename: str):
    pth = Config().path
    folder_pth = os.path.join(
        pth, os.sep.join(['front_end', 'page', 'static', 'model', 'visual_outputs', 'price_position', *args])
    )
    if not os.path.exists(folder_pth):
        os.makedirs(folder_pth)
    return os.path.join(
        folder_pth, filename
    )


def plot_and_save_price_position_data():
    exchange_ls = Config().exchange_list
    df = pd.DataFrame()
    for e in exchange_ls:
        ppos = PricePosition(e)
        ppos_df = ppos.run()
        df = pd.concat([df, ppos_df], axis=0)

    df.sort_values(by=['range', 'pos_pctg'], ascending=True, inplace=True)
    df = df.dropna(subset=['pos_pctg'])

    width = max(10, len(df) * 0.8)  # 每个品种至少占0.8的宽度
    fig, ax = plt.subplots(figsize=(width, 8))
    for idx, row in enumerate(df.iterrows()):
        row = row[1]
        # 绘制价格区间线
        ax.vlines(x=idx, ymin=row['low'], ymax=row['high'], color='gray', alpha=0.5, linewidth=4)

        # 绘制当前价格点
        ax.scatter(idx, row['close'], color='red', s=20, zorder=3)

        # 添加价格标签
        # ax.text(idx, row['low'], f'{row["low"]:.2f}', ha='center', va='top', rotation=45, fontsize=9)
        ax.text(idx, row['high'], f'{row["high"]:.2f}', ha='center', va='bottom', rotation=45, fontsize=9)
        ax.text(idx, row['close'],
                f'{row["close"]:.2f}\n({row["pos_pctg"]}%)',
                ha='left', va='center', rotation=45, fontsize=9)

    # 设置X轴标签
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df.index, rotation=45, fontsize=9)
    ax.set_xlim(-0.5, len(df) - 0.5)
    # 添加标题和标签
    ax.set_title('Price Position Analysis')
    ax.set_ylabel('Price')
    ax.grid(True, linestyle='--', alpha=0.3)
    plt.subplots_adjust(
        bottom=0.15,  # 底部边距
        right=0.95,  # 右边距
        left=0.1,  # 左边距
        top=0.95  # 顶部边距
    )
    # 调整布局以确保标签不被裁切
    plt.tight_layout()
    plt.savefig(file_location(filename="price_position_overview.jpeg"))

    plt.close()


if __name__ == "__main__":
    plot_and_save_price_position_data()
