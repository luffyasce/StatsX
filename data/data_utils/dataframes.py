import re
import pandas as pd
from data.data_utils.data_screening import *


def split_num_from_str(string_d: str):
    """
    separate digits from string
    eg: pass in 15min return 15 and min
    :param string_d:
    :return:
    """
    num_ = re.findall("[0-9]+", string_d)[0]
    str_ = re.findall("[a-z]+", string_d)[0]
    return num_, str_


def resample_tick_price_to_bar_md(data_s: pd.Series, level: str):
    """
    resample tick data into minute or daily bar data
    only accepts last price series
    :param data_s: tick last price series
    :param level: 1min, 15min, 30min, 1d, etc
    :return: bar md data
    """
    data_s.sort_index(ascending=True)
    frequency_dict = {
        'min': 'T',
        'd': 'D'
    }
    freq_pair = split_num_from_str(level)
    frequency = freq_pair[0] + frequency_dict[freq_pair[1]]
    ohlc_data = data_s.resample(frequency).ohlc()
    ohlc_data.dropna(inplace=True)
    return ohlc_data


def simple_resample_md_data(df: pd.DataFrame, level: str):
    """
    resample md data or tick data to higher granular bar data
    only accepts OHLC(open high low last) and rest of the columns will be dropped.
    :param df: tick md or md data
    :param level: 1min, 15min, 30min, 1d, etc
    :return: bar md data
    """
    current_price_calling = 'last' if 'last' in df.columns else 'close'
    df = df.sort_index(ascending=True)[['open', 'low', 'high', current_price_calling]].copy()
    frequency_dict = {
        'min': 'T',
        'd': 'D'
    }
    freq_pair = split_num_from_str(level)
    frequency = freq_pair[0] + frequency_dict[freq_pair[1]]
    resampled_df = df.resample(frequency).agg({'open': 'first', 'high': 'max', 'low': 'min', current_price_calling: 'last'})
    resampled_df.dropna(inplace=True)
    return resampled_df


def resample_md_data(df: pd.DataFrame, target_timeframe: str, label_at: str = "right", close_at: str = "right", sum: bool = True):
    """
    to resample higher granular md data into lower granular md data
    :param df:  source dataframe
    :param target_timeframe:  timeframe to which you want to transform source dataframe
    :param label_at: where to extract your target data label. 'right' by default
    :param close_at: where to extract your target data from sample bins, 'right' by default
    :return: resampled dataframe
    """
    if target_timeframe != 'D':
        target_df = df[['open']].resample(target_timeframe, label=label_at, closed=close_at).first()
        target_df['high'] = df['high'].resample(target_timeframe, label=label_at, closed=close_at).max()
        target_df['low'] = df['low'].resample(target_timeframe, label=label_at, closed=close_at).min()
        if 'close' in df.columns:
            target_df['close'] = df['close'].resample(target_timeframe, label=label_at, closed=close_at).last()
        if 'last' in df.columns:
            target_df['last'] = df['last'].resample(target_timeframe, label=label_at, closed=close_at).last()
        if 'pre_close' in df.columns:
            target_df['pre_close'] = df['pre_close'].resample(target_timeframe, label=label_at, closed=close_at).first()
        if 'volume' in df.columns:
            target_df['volume'] = df['volume'].resample(target_timeframe, label=label_at, closed=close_at).sum() if sum else \
                df['volume'].resample(target_timeframe, label=label_at, closed=close_at).last()
        if 'turnover' in df.columns:
            target_df['turnover'] = df['turnover'].resample(target_timeframe, label=label_at, closed=close_at).sum() if sum else \
                df['turnover'].resample(target_timeframe, label=label_at, closed=close_at).last()
        if 'open_interest' in df.columns:
            target_df['open_interest'] = df['open_interest'].resample(target_timeframe, label=label_at, closed=close_at).last()
    else:
        df = df.reset_index(drop=False).set_index('trading_date').sort_values(by='datetime', ascending=True)
        target_df = df[['open', 'datetime']].groupby(df.index).apply(
            lambda x: x[x['datetime'] == x['datetime'].min()][['open']]
        ).reset_index(level=0, drop=True)
        target_df['high'] = df[['high', 'datetime']].groupby(df.index).apply(
            lambda x: x['high'].max()
        )
        target_df['low'] = df[['low', 'datetime']].groupby(df.index).apply(
            lambda x: x['low'].min()
        )
        if 'close' in df.columns:
            target_df['close'] = df[['close', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].max()]['close']
            ).reset_index(level=0, drop=True)
        if 'last' in df.columns:
            target_df['last'] = df[['last', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].max()]['last']
            ).reset_index(level=0, drop=True)
        if 'pre_close' in df.columns:
            target_df['pre_close'] = df[['pre_close', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].min()]['pre_close']
            ).reset_index(level=0, drop=True)
        if 'volume' in df.columns:
            target_df['volume'] = df[['volume', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].max()]['volume']
            ).reset_index(level=0, drop=True) if not sum else \
                df[['volume', 'datetime']].groupby(df.index).apply(
                    lambda x: x['volume'].sum()
                )
        if 'turnover' in df.columns:
            target_df['turnover'] = df[['turnover', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].max()]['turnover']
            ).reset_index(level=0, drop=True) if not sum else \
                df[['turnover', 'datetime']].groupby(df.index).apply(
                    lambda x: x['turnover'].sum()
                )
        if 'open_interest' in df.columns:
            target_df['open_interest'] = df[['open_interest', 'datetime']].groupby(df.index).apply(
                lambda x: x[x['datetime'] == x['datetime'].max()]['open_interest']
            ).reset_index(level=0, drop=True) if not sum else \
                df[['open_interest', 'datetime']].groupby(df.index).apply(
                    lambda x: x['open_interest'].sum()
                )
    return target_df


def period_group_md_data(df: pd.DataFrame, N: int):
    df = period_mark(df, N, True, False)
    df['period_mark'] = df['period_mark'].replace(to_replace=0, method='bfill')
    df.dropna(subset=['period_mark'], inplace=True)
    mdv = df.groupby('period_mark').apply(
        lambda x: [
            # open high low (close | last) [pre_close] volume turnover open_interest
            x.index.to_series().iloc[-1],
            x['datetime'].loc[x['datetime'].last_valid_index()] if 'datetime' in x.columns else np.nan,
            x['trading_date'].loc[x['trading_date'].last_valid_index()] if 'trading_date' in x.columns else np.nan,
            x['open'].loc[x['open'].first_valid_index()],
            x['high'].max(),
            x['low'].min(),
            x['close'].loc[x['close'].last_valid_index()] if 'close' in x.columns else np.nan,
            x['last'].loc[x['last'].last_valid_index()] if 'last' in x.columns else np.nan,
            x['pre_close'].loc[x['pre_close'].first_valid_index()] if 'pre_close' in x.columns else np.nan,
            x['volume'].sum() if 'volume' in x.columns else np.nan,
            x['turnover'].sum() if 'turnover' in x.columns else np.nan,
            x['open_interest'].loc[x['open_interest'].last_valid_index()] if 'open_interest' in x.columns else np.nan,
        ]
    ).tolist()
    md = pd.DataFrame(
        mdv,
        columns=[
            'idx_', 'datetime', 'trading_date', 'open', 'high', 'low', 'close', 'last', 'pre_close',
            'volume', 'turnover', 'open_interest'
        ]
    ).set_index('idx_').dropna(how='all', axis=1)
    return md


def point_group_md_data(df: pd.DataFrame, time_point: time, err: int):
    df = point_mark(df, time_point)
    df['point_mark'] = df['point_mark'].fillna(method='bfill')
    df.dropna(subset=['point_mark'], inplace=True)
    mdv = df.groupby('point_mark').apply(
        lambda x: [
            # open high low (close | last) [pre_close] volume turnover open_interest
            x.index.to_series().iloc[-1],
            x['datetime'].loc[x['datetime'].last_valid_index()] if 'datetime' in x.columns else np.nan,
            x['trading_date'].loc[x['trading_date'].last_valid_index()] if 'trading_date' in x.columns else np.nan,
            x['open'].loc[x['open'].first_valid_index()],
            x['high'].max(),
            x['low'].min(),
            x['close'].loc[x['close'].last_valid_index()] if 'close' in x.columns else np.nan,
            x['last'].loc[x['last'].last_valid_index()] if 'last' in x.columns else np.nan,
            x['pre_close'].loc[x['pre_close'].first_valid_index()] if 'pre_close' in x.columns else np.nan,
            x['volume'].sum() if 'volume' in x.columns else np.nan,
            x['turnover'].sum() if 'turnover' in x.columns else np.nan,
            x['open_interest'].loc[x['open_interest'].last_valid_index()] if 'open_interest' in x.columns else np.nan,
        ]
    ).tolist()
    md = pd.DataFrame(
        mdv,
        columns=[
            'idx_', 'datetime', 'trading_date', 'open', 'high', 'low', 'close', 'last', 'pre_close',
            'volume', 'turnover', 'open_interest'
        ]
    ).set_index('idx_').dropna(how='all', axis=1)
    return md


def convert_to_numpy(df: pd.DataFrame):
    for col in [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()]:
        df[col] = to_timestamp(df[col])
    return df.to_numpy(na_value=np.nan)


def convert_to_pandas(df: np.array, cols: list):
    df = pd.DataFrame(df, columns=cols)
    for col in [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()]:
        df[col] = from_timestamp(df[col])
    return df
