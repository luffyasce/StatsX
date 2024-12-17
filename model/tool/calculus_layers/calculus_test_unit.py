import inspect
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import torch
from torch import tensor
from strategy.tool.calculus_layers.feature import FeatureCalculus, FeatureCalculus2D
from strategy.tool.torch_addin import *


def desc():
    s = pd.Series([i[0] for i in inspect.getmembers(FeatureCalculus, predicate=inspect.ismethod)])
    return s.sort_values().reset_index(drop=True)


def check_integrity():
    ll = [f"{i[0]}2d" for i in inspect.getmembers(FeatureCalculus, predicate=inspect.ismethod)]
    ll2d = [i[0] for i in inspect.getmembers(FeatureCalculus2D, predicate=inspect.ismethod)]
    return (set(ll) - set(ll2d)) | (set(ll2d) - set(ll))


def check_corr():
    sample = torch.randn((1000, ), dtype=torch.float32)
    N_ = 20
    co_df = pd.DataFrame()
    for x in [i[1] for i in inspect.getmembers(FeatureCalculus, predicate=inspect.ismethod)]:
        d_ = x(sample, N_)
        co_df[x.__name__] = pd.Series(d_.numpy()).round(2)

    cor = co_df.corr()

    plt.figure(figsize=(20, 15))
    sns.heatmap(cor, vmax=1, annot=True)
    plt.show()

    res = cor[abs(cor) > 0.8].unstack().dropna().rename('val').reset_index(drop=False)
    res = res[res['level_0'] != res['level_1']].set_index(['level_0', 'level_1'])['val'].sort_values()
    cnt_s = pd.Series(dtype=int)
    for y in [i[0] for i in inspect.getmembers(FeatureCalculus, predicate=inspect.ismethod)]:
        try:
            print(res.loc[y].rename(y))
            cnt_ = len(res.loc[y])
        except:
            pass
        else:
            cnt_s[y] = cnt_
    return cnt_s.sort_values()


print(check_integrity())