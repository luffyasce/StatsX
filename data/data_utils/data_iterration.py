import pandas as pd
import math
from typing import Union


def data_rolling(data: Union[pd.Series, pd.DataFrame], window: int, min_periods: int):
    for dat_ in data.rolling(window):
        idx_ = dat_.index.to_series().iloc[-1]
        if len(dat_) < min_periods:
            pass
        else:
            yield idx_, dat_


def data_perioding(data: Union[pd.Series, pd.DataFrame], step: int, residual: bool):
    length_ = math.floor(len(data) / step)
    length_ = length_ + 1 if residual else length_
    for i in range(length_):
        dat_ = data.iloc[i*step: (i+1)*step]
        idx_ = dat_.index.to_series().iloc[-1]
        yield idx_, dat_.copy()


def data_date_splitting(data: Union[pd.Series, pd.DataFrame]):
    for d, v in data.groupby(data.index.date):
        yield d, v


def data_hour_splitting(data: Union[pd.Series, pd.DataFrame]):
    for h, v in data.groupby(data.index.hour):
        yield h, v

