from datetime import datetime, timedelta
from typing import Any, List, Tuple
import pandas as pd
import numpy as np


def map_datetime(dt_string: Any):
    if isinstance(dt_string, datetime):
        return dt_string
    elif len(dt_string) == 10:
        return datetime.strptime(dt_string, "%Y-%m-%d")
    elif len(dt_string) == 19:
        return datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
    elif len(dt_string) > 19:
        dates = dt_string.split(' ')[0]
        times = dt_string.split(' ')[1].split('.')[0]
        mils = dt_string.split(' ')[1].split('.')[1].rstrip('0')
        mils = mils.ljust(3, '0')
        return datetime(
            year=int(dates.split('-')[0]),
            month=int(dates.split('-')[1]),
            day=int(dates.split('-')[2]),
            hour=int(times.split(':')[0]),
            minute=int(times.split(':')[1]),
            second=int(times.split(':')[2]),
            microsecond=int(mils) * 1000
        )
    else:
        return None


def to_timestamp(d: pd.Series) -> pd.Series:
    return pd.Series(d.values.astype(np.int64), index=d.index) / 1e9


def from_timestamp(d: pd.Series) -> pd.Series:
    return pd.Series(d.values.astype(np.float).astype('datetime64[s]'), index=d.index)


def datetime_mapping(dt_periods_sample: List[Tuple[datetime, datetime]], granular: int):
    rls = []
    for (start_, end_) in dt_periods_sample:
        rls += [
            start_ + timedelta(minutes=(i + 1) * granular) for i in range(
                int((end_ - start_).total_seconds() // (60 * granular))
            )
        ]
    for r in rls:
        print(r)
    return rls


def check_weekdays(date: datetime):
    if date.isoweekday() > 5:
        return False
    return True


def yield_dates(
    start: datetime, end: datetime, date_delta: int = 1, skip_weekends: bool = True, clean_dates: bool = True
):
    if clean_dates:
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    ret = start + timedelta(days=-date_delta)
    while ret < end:
        ret = ret + timedelta(days=date_delta)
        if skip_weekends:
            if check_weekdays(ret):
                yield ret
        else:
            yield ret
