import numpy as np
import pandas as pd
from numba import jit
from typing import Any


def _sigmod_corr(df: pd.DataFrame, ignore_diagonal: bool = False):
    """
    calculate the correlation between sigmod series, such as signals (0, 1, -1)
    """
    cols = df.columns
    ls1 = []
    for i in cols:
        ls2 = []
        for j in cols:
            rig = (df[i] == df[j]).astype(int).sum()
            ratio = rig / len(df[i])
            if ignore_diagonal:
                if i == j:
                    ratio = np.nan
                else:
                    pass
            else:
                pass
            ls2.append(ratio)
        ls1.append(ls2)
    result_df = pd.DataFrame(ls1, columns=cols, index=cols)
    return result_df


def correlation_matrix(df: pd.DataFrame, sigmod: bool, ignore_diagonal: bool, method_: str = 'pearson') -> pd.DataFrame:
    df = df.dropna(how='any')
    if not sigmod:
        cor_ = df.corr(method=method_)
        if ignore_diagonal:
            np.fill_diagonal(cor_.values, np.nan)
        return cor_
    else:
        df = np.sign(df)
        cor_ = _sigmod_corr(df, ignore_diagonal)
        return cor_


# relative frequency
@jit(nopython=True)
def _relative_frequency(sample: np.array, value: Any):
    return len(sample[sample == value]) / len(sample)


def relative_frequency(sample: pd.Series, value: Any) -> float:
    sample = sample.to_numpy()
    return _relative_frequency(sample, value)


# relative frequency with signed data series
def relative_frequency_sign(data: pd.Series) -> list:
    data = pd.Series(np.sign(data), index=data.index)
    neg = relative_frequency(data, -1)
    nil = relative_frequency(data, 0)
    pos = relative_frequency(data, 1)
    return [neg, nil, pos]


# mode num
def mode(data: pd.Series) -> Any:
    mode_ = data.mode()[0]
    return mode_

