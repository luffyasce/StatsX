import numpy as np
import pandas as pd
import bottleneck as bn
import scipy.stats as stats
from datetime import time, datetime
from typing import List
from data.data_utils.data_screening import *


class Calculus:
    @classmethod
    def max(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).max()

    @classmethod
    def min(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).min()

    @classmethod
    def mean(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).mean()

    @classmethod
    def median(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).median()
    
    @classmethod
    def mad(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).apply(lambda x: bn.nanmedian(abs(x - bn.nanmedian(x))))

    @classmethod
    def lower_quarter(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).quantile(0.25)

    @classmethod
    def upper_quarter(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).quantile(0.75)

    @classmethod
    def std(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).std()

    @classmethod
    def cov(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).cov()

    @classmethod
    def sem(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).sem()

    @classmethod
    def var(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).var()

    @classmethod
    def sum(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).sum()

    @classmethod
    def ratio_on_sum(cls, data: pd.Series, N: int):
        return data / data.rolling(N, min_periods=N).sum()

    @classmethod
    def signed_sum(cls, data: pd.Series, N: int):
        return pd.Series(np.sign(data), index=data.index).rolling(N, min_periods=N).sum()

    @classmethod
    def skew(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).skew()

    @classmethod
    def kurt(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).kurt()

    @classmethod
    def shift(cls, data: pd.Series, N: int):
        return data.shift(N)

    @classmethod
    def shift_diff(cls, data: pd.Series, N: int):
        return data.diff(periods=N)

    @classmethod
    def shift_div(cls, data: pd.Series, N: int):
        return data / data.shift(N)

    @classmethod
    def mean_diff(cls, data: pd.Series, N: int):
        return data - data.rolling(N, min_periods=N).mean()

    @classmethod
    def std_mean_diff(cls, data: pd.Series, N: int):
        return (data - data.rolling(N, min_periods=N).mean()) / data.rolling(N, min_periods=N).std()

    @classmethod
    def exclude_std_mean_diff(cls, data: pd.Series, N: int):
        _d = (data - data.rolling(N, min_periods=N).mean()) / data.rolling(N, min_periods=N).std()
        _d = pd.Series(np.where(abs(_d) >= 1, _d, 0), index=_d.index)
        return _d

    @classmethod
    def exclude_double_std_mean_diff(cls, data: pd.Series, N: int):
        _d = (data - data.rolling(N, min_periods=N).mean()) / data.rolling(N, min_periods=N).std()
        _d = pd.Series(np.where(abs(_d) >= 2, _d, 0), index=_d.index)
        return _d

    @classmethod
    def mean_div(cls, data: pd.Series, N: int):
        return data / data.rolling(N, min_periods=N).mean()

    @classmethod
    def cum_min(cls, data: pd.Series, N: int):
        return data.cumsum().rolling(N, min_periods=N).min()

    @classmethod
    def cum_max(cls, data: pd.Series, N: int):
        return data.cumsum().rolling(N, min_periods=N).max()

    @classmethod
    def diff_max(cls, data: pd.Series, N: int):
        return data - data.rolling(N, min_periods=N).max()

    @classmethod
    def diff_min(cls, data: pd.Series, N: int):
        return data - data.rolling(N, min_periods=N).min()

    @classmethod
    def max_only(cls, data: pd.Series, N: int):
        data = pd.Series(np.where(data == data.rolling(N, min_periods=N).max(), data, 0), index=data.index)
        return data

    @classmethod
    def min_only(cls, data: pd.Series, N: int):
        data = pd.Series(np.where(data == data.rolling(N, min_periods=N).min(), data, 0), index=data.index)
        return data

    @classmethod
    def annualize(cls, data: pd.Series, N: int):
        data = pd.Series(np.power((data + 1), int(250 / N)) - 1, index=data.index)
        return data

    @classmethod
    def filter_fill_std(cls, data: pd.Series, N: int):
        _d = (data - data.rolling(N, min_periods=N).mean()) / data.rolling(N, min_periods=N).std()
        _d = pd.Series(np.where(abs(_d) >= 1, _d, 0), index=_d.index).replace(to_replace=0, method='pad')
        return _d

    @classmethod
    def abs_sum(cls, data: pd.Series, N: int):
        return abs(data).rolling(N, min_periods=N).sum()

    @classmethod
    def cnt_abv(cls, data: pd.Series, N: int):
        return pd.Series(bn.move_rank(data, N, N), index=data.index)

    @classmethod
    def cnt_abv_diff(cls, data: pd.Series, N: int):
        return pd.Series(bn.move_rank(data, N, N), index=data.index).diff()

    @classmethod
    def mean_angular(cls, data: pd.Series, N: int):
        v_ = data.rolling(N, min_periods=N).mean()
        ang_ = (v_ * 180) / np.square(np.pi)
        data = pd.Series(np.mod(np.abs(ang_), 360) * np.sign(ang_), index=data.index, name=data.name)
        return data


class FixationCalculus:

    @classmethod
    def reverse_sign(cls, data: pd.Series):
        return data * (-1)


class FutureDataCalculus:
    """
    WARNING: Usage of this category should be cautious since this will introduce future data.
    """

    @classmethod
    def future_data_sum(cls, data: pd.Series, N: int):
        data.sort_index(ascending=False, inplace=True)
        data = data.rolling(N, min_periods=N).sum()
        return data.sort_index(ascending=True)

    @classmethod
    def future_data_mean(cls, data: pd.Series, N: int):
        data.sort_index(ascending=False, inplace=True)
        data = data.rolling(N, min_periods=N).mean()
        return data.sort_index(ascending=True)


class StatisticCalculus:

    @classmethod
    def periodical_sum(cls, data: pd.Series, N: int):
        return data.rolling(N, min_periods=N).sum()

    @classmethod
    def periodical_max(cls, data: pd.Series, N: int):
        data = pd.Series(np.where(data == data.rolling(N, min_periods=N).max(), data, 0), index=data.index)
        return data

    @classmethod
    def periodical_min(cls, data: pd.Series, N: int):
        data = pd.Series(np.where(data == data.rolling(N, min_periods=N).min(), data, 0), index=data.index)
        return data

    @classmethod
    def periodical_std(cls, data: pd.Series, N: int):
        data = pd.Series(
            np.where(
                abs(data) >= (data.rolling(N, min_periods=N).mean() + data.rolling(N, min_periods=N).std()),
                data,
                0
            ),
            index=data.index
        )
        return data

    @classmethod
    def periodical_double_std(cls, data: pd.Series, N: int):
        data = pd.Series(
            np.where(
                abs(data) >= (data.rolling(N, min_periods=N).mean() + (data.rolling(N, min_periods=N).std() * 2)),
                data,
                0
            ),
            index=data.index
        )
        return data


class GroupCalculus:

    @classmethod
    def date_group_sum(cls, data: pd.Series):
        return data.groupby(data.index.date).sum()

    @classmethod
    def date_group_mean(cls, data: pd.Series):
        return data.groupby(data.index.date).mean()

    @classmethod
    def period_group_sum(cls, data: pd.Series, N: int):
        temp_data = period_mark(pd.DataFrame(data.rename('data')), step=N, incremental=True, mark_last=False)
        temp_data['period_mark'] = temp_data['period_mark'].fillna(method='bfill')
        return temp_data.groupby('period_mark')['data'].sum()

    @classmethod
    def period_group_mean(cls, data: pd.Series, N: int):
        temp_data = period_mark(pd.DataFrame(data.rename('data')), step=N, incremental=True, mark_last=False)
        temp_data['period_mark'] = temp_data['period_mark'].fillna(method='bfill')
        return temp_data.groupby('period_mark')['data'].mean()

    @classmethod
    def point_group_sum(cls, data: pd.Series, time_point: str):
        time_point = time.fromisoformat(time_point)
        temp_data = point_mark(pd.DataFrame(data.rename('data')), point=time_point)
        temp_data['point_mark'] = temp_data['point_mark'].fillna(method='bfill')
        temp_data['point_mark'] = from_timestamp(temp_data['point_mark'].dropna())
        return temp_data.groupby('point_mark')['data'].sum()

    @classmethod
    def point_group_mean(cls, data: pd.Series, time_point: str):
        time_point = time.fromisoformat(time_point)
        temp_data = point_mark(pd.DataFrame(data.rename('data')), point=time_point)
        temp_data['point_mark'] = temp_data['point_mark'].fillna(method='bfill')
        temp_data['point_mark'] = from_timestamp(temp_data['point_mark'].dropna())
        return temp_data.groupby('point_mark')['data'].mean()

    @classmethod
    def multi_points_group_sum(cls, data: pd.Series, time_pts: List[str]):
        data = pd.DataFrame(data.rename('data'))
        temp_mark = pd.Series(np.zeros(len(data)), index=data.index)
        for t in time_pts:
            t = time.fromisoformat(t)
            temp_ = point_mark(data.copy(), point=t)
            temp_mark += temp_['point_mark'].fillna(0)
        data['point_mark'] = temp_mark.replace(to_replace=0, method='bfill').replace(to_replace=0, value=np.nan).dropna()
        data['point_mark'] = from_timestamp(data['point_mark'].dropna())
        return data.groupby('point_mark')['data'].sum()

    @classmethod
    def multi_points_group_mean(cls, data: pd.Series, time_pts: List[str]):
        data = pd.DataFrame(data.rename('data'))
        temp_mark = pd.Series(np.zeros(len(data)), index=data.index)
        for t in time_pts:
            t = time.fromisoformat(t)
            temp_ = point_mark(data.copy(), point=t)
            temp_mark += temp_['point_mark'].fillna(0)
        data['point_mark'] = temp_mark.replace(to_replace=0, method='bfill').replace(to_replace=0, value=np.nan).dropna()
        data['point_mark'] = from_timestamp(data['point_mark'].dropna())
        return data.groupby('point_mark')['data'].mean()


class FactorActivationCalculus:

    @classmethod
    def step_sign(cls, data: pd.Series):
        return pd.Series(np.sign(data), index=data.index)

    @classmethod
    def sigmod(cls, data: pd.Series):
        return pd.Series(1 / (1 + np.exp(-data)), index=data.index) - 0.5

    @classmethod
    def tanh(cls, data: pd.Series):
        return pd.Series(np.tanh(data), index=data.index)

    @classmethod
    def arctan(cls, data: pd.Series):
        return pd.Series(np.arctan(data), index=data.index)


class StandardizationCalculus:

    @classmethod
    def log(cls, data: pd.Series):
        return pd.Series(np.log(data), index=data.index)

    @classmethod
    def log10(cls, data: pd.Series):
        return pd.Series(np.log10(data), index=data.index)

    @classmethod
    def sqrt(cls, data: pd.Series):
        return pd.Series(np.sqrt(np.abs(data)), index=data.index) * np.sign(data)


class ScalingCalculus:

    @classmethod
    def zscore(cls, data: pd.Series, N: int):
        return ((data - data.rolling(N, min_periods=N).mean()) / data.rolling(N, min_periods=N).std()).fillna(0) if N > 0 else data
    
    @classmethod
    def robust_zscore(cls, data: pd.Series, N: int):
        return ((data - data.rolling(N, min_periods=N).median()) / Calculus.mad(data, N)).fillna(0) if N > 0 else data

    @classmethod
    def minmax(cls, data: pd.Series, N: int):
        return (
            (data - data.rolling(N, min_periods=N).min()) / (data.rolling(N, min_periods=N).max() - data.rolling(N, min_periods=N).min()) if N > 0 else data
        ).fillna(0)
