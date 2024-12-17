import pandas as pd
import numpy as np
from sklearn import preprocessing


# Log return
def logarithm_change(data: pd.DataFrame, column_a: str, column_b: str = None) -> pd.Series:
    if column_b is not None:
        samp_ = data[column_a] / data[column_b]
    else:
        samp_ = data[column_a] / data[column_a].shift()
    log_change = np.log(samp_, where=samp_ > 0)
    return log_change


def sample_value_range(data: pd.Series):
    last_ = data.iloc[-1]
    range_ = (last_ - data.min()) / (data.max() - data.min()) if (data.max() - data.min()) != 0 else 0
    return range_


def sample_frequency_range(data: pd.Series):
    last_ = data.iloc[-1]
    range_ = data[data <= last_].count() / len(data)
    return range_


# FOLLOWING METHOD PREFIXED WITH 'standardizing' IS SAFE WITH FUTURE DATA,
# AND APPLICABLE WITH TRAINING AND MINING PROCESS.


def standardizing_linear(data: pd.Series):
    return data


def standardizing_log10(data: pd.Series):
    data = pd.Series(np.log10(data, where=data > 0), index=data.index, name=data.name)
    return data


def standardizing_log2(data: pd.Series):
    data = pd.Series(np.log2(data, where=data > 0), index=data.index, name=data.name)
    return data


def standardizing_log(data: pd.Series):
    data = pd.Series(np.log(data, where=data > 0), index=data.index, name=data.name)
    return data


def standardizing_sqrt(data: pd.Series):
    data = pd.Series(np.sqrt(data, where=data >= 0), index=data.index, name=data.name)
    return data


def standardizing_cbrt(data: pd.Series):
    data = pd.Series(np.cbrt(data), index=data.index, name=data.name)
    return data


def standardizing_angular(data: pd.Series):
    ang_ = (data * 180) / np.square(np.pi)
    data = pd.Series(np.mod(np.abs(ang_), 360) * np.sign(ang_), index=data.index, name=data.name)
    return data


# WARNING: USAGE OF FOLLOWING SCALING METHOD ON TRAINING DATASET WILL INTRODUCE FUTURE DATA.


def scaling_z_score(data: pd.Series):
    idx = data.index
    name_ = data.name
    data = data.to_numpy().reshape(-1, 1)
    std_data = preprocessing.StandardScaler().fit_transform(data)
    return pd.DataFrame(std_data, index=idx)[0].rename(name_)


def scaling_min_max(data: pd.Series):
    idx = data.index
    name_ = data.name
    data = data.to_numpy().reshape(-1, 1)
    std_data = preprocessing.MinMaxScaler().fit_transform(data)
    return pd.DataFrame(std_data, index=idx)[0].rename(name_)


def scaling_max_abs(data: pd.Series):
    idx = data.index
    name_ = data.name
    data = data.to_numpy().reshape(-1, 1)
    std_data = preprocessing.MaxAbsScaler().fit_transform(data)
    return pd.DataFrame(std_data, index=idx)[0].rename(name_)


def scaling_norm(data: pd.Series):
    idx = data.index
    name_ = data.name
    data = data.to_numpy().reshape(-1, 1)
    std_data = preprocessing.Normalizer().fit_transform(data)
    return pd.DataFrame(std_data, index=idx)[0].rename(name_)


def scaling_robust(data: pd.Series):
    idx = data.index
    name_ = data.name
    data = data.to_numpy().reshape(-1, 1)
    std_data = preprocessing.RobustScaler().fit_transform(data)
    return pd.DataFrame(std_data, index=idx)[0].rename(name_)


if __name__ == "__main__":
    a = pd.Series([1, 2, 7, -7.0, 7, 6.28, 9, 900, 1000, 1100, 100000], index=[1, 2, 4, 56, 0 ,7,8, 9, 10, 11, 12], name='aaaaa')
    print(standardizing_log(a))
    print(standardizing_log2(a))
    print(standardizing_log10(a))
    print(standardizing_sqrt(a))
    print(standardizing_angular(a))
    print(standardizing_cbrt(a))
    print(standardizing_linear(a))
