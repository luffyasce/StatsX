import pandas as pd
import numpy as np
from datetime import datetime
from utils.database.unified_db_control import UnifiedControl
import matplotlib.pyplot as plt


def polyfit_ts_data(d: pd.Series):
    x = d.index.tolist()
    y = d.values.tolist()
    return np.polyfit(x, y, 1)


def md_channel(df: pd.DataFrame):
    """
    划出行情数据的高低范围
    df: md data that contains high, low and indexed with datetime
    """
    df = df.sort_index(ascending=True)
    md = pd.concat(
        [
            df['high'].expanding().max().rename('ah'),
            df['high'].sort_index(ascending=False).expanding().max().rename('bh'),
            df['low'].expanding().min().rename('al'),
            df['low'].sort_index(ascending=False).expanding().min().rename('bl'),
        ], axis=1
    )
    md['tops'] = md[['ah', 'bh']].min(axis=1)
    md['bots'] = md[['al', 'bl']].max(axis=1)
    return md.loc[:, ['tops', 'bots']]


def realized_volitility(log_return: pd.Series):
    """
    计算给定周期的行情数据整体的已实现波动率
    :param log_return: log return
    :return: realized vol
    """
    return np.sqrt(255 * log_return.var())


def historical_volitility(log_return: pd.Series):
    """
    计算给定周期的行情数据整体的历史波动率
    :param log_return: log return
    :return: historical vol
    """
    return log_return.std() * np.sqrt(255)


def calculate_orderbook_vwap(ask1, ask_vol1, bid1, bid_vol1):
    """
    计算orderbook的vwap
    """
    return (ask1 * ask_vol1 + bid1 * bid_vol1) / (ask_vol1 + bid_vol1)


"""
分析交易方向对持仓和成交的影响：
    四种动作：
        1. 多开 （多）
        2. 空开 （空）
        3. 多平 （空）
        4. 空平 （多）
    组合情形：
        1. 多开 + 多平：换手，持仓不变
        2. 空开 + 空平：换手，持仓不变
        3. 多开 + 空开：增仓，持仓增加
        4. 多平 + 空平：减仓，持仓减少
"""


def filter_orders(
        prev_total_vol: float, current_total_vol: float,
        prev_open_interest: float, current_open_interest: float,
        last_price: float, prev_price: float, multiplier: float,
        cap_threshold: float, ir_threshold: float
) -> dict:
    """
    分析合约相邻的两次orderbook数据，判断和过滤出有效的订单
    :param prev_total_vol: 上一次的总成交量
    :param current_total_vol: 当前的总成交量
    :param prev_open_interest: 上一次的总持仓量
    :param current_open_interest: 当前的总持仓量
    :param last_price: 最新成交价
    :param prev_price: 前一成交价
    :param multiplier: 合约乘数
    :param ir_threshold:  信息比率阈值
    :param cap_threshold:  交易量阈值
    :return:    有效订单信息
    """
    vol_delta = current_total_vol - prev_total_vol
    oi_delta = current_open_interest - prev_open_interest
    money_delta = oi_delta * last_price * multiplier
    information_ratio = 0 if vol_delta == 0 else oi_delta / vol_delta
    price_move = last_price / prev_price - 1
    if abs(money_delta) >= cap_threshold and abs(information_ratio) >= ir_threshold:
        return {"money_delta": money_delta, "information_ratio": information_ratio, 'price_chg': price_move}
        
