import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from utils.tool.datetime_wrangle import to_timestamp, from_timestamp


def period_mark(sample_df: pd.DataFrame, step: int, incremental: bool = False, mark_last: bool = False):
    idx_ = sample_df.index
    sample_df.reset_index(drop=True, inplace=True)
    sample_df['period_mark'] = np.where(
        (sample_df.index.to_series() + 1) % step != 0,
        np.nan,
        (sample_df.index.to_series() + 1) / step if incremental else 1
    )
    if mark_last and np.isnan(sample_df.iloc[-1]['period_mark']):
        sample_df.iloc[-1, list(sample_df.columns).index('period_mark')] = sample_df['period_mark'].max() + 1
    sample_df.index = idx_
    return sample_df


def point_mark(sample_df: pd.DataFrame, point: time):
    raw_pt = datetime(2010, 1, 1, point.hour, point.minute, point.second)
    sample_df['point_mark'] = to_timestamp(sample_df.index.to_series() - raw_pt)
    sample_df['point_mark'] = np.where(
        sample_df['point_mark'].astype(int) % (24 * 60 * 60) == 0,
        to_timestamp(sample_df.index.to_series()),
        np.nan,
    )
    return sample_df

