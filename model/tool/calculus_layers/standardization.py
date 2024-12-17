import torch
from torch import tensor
from strategy.tool.torch_addin import clean_tensor
from strategy.tool.calculus_layers.feature import FeatureCalculus as fcc
from strategy.tool.calculus_layers.feature import FeatureCalculus2D as fcc2d


class StandardizingCalculus:

    @classmethod
    def standardizing_log(cls, data: tensor):
        return clean_tensor(torch.log(data))

    @classmethod
    def standardizing_log10(cls, data: tensor):
        return clean_tensor(torch.log10(data))

    @classmethod
    def standardizing_sqrt(cls, data: tensor):
        return clean_tensor(torch.sqrt(torch.abs(data)) * torch.sign(data))

    @classmethod
    def standardizing_square(cls, data: tensor):
        return clean_tensor(torch.square(data) * torch.sign(data))

    @classmethod
    def standardizing_reverse(cls, data: tensor):
        return clean_tensor(1 / data)

    @classmethod
    def standardizing_placebo(cls, data: tensor):
        return clean_tensor(data)


class ScalingCalculus:

    @classmethod
    def placebo(cls, data: tensor, N: int):
        return data

    @classmethod
    def zscore(cls, data: tensor, N: int):
        d_ = (data - fcc.mean(data, N)) / fcc.std(data, N) if N > 0 else data
        return clean_tensor(d_)

    @classmethod
    def minmax(cls, data: tensor, N: int):
        d_ = ((data - fcc.min(data, N)) / (fcc.max(data, N) - fcc.min(data, N)) - 0.5) * 2 if N > 0 else data
        return clean_tensor(d_)


class ScalingCalculus2D:

    @classmethod
    def placebo2d(cls, data: tensor, N: int):
        return data

    @classmethod
    def zscore2d(cls, data: tensor, N: int):
        d_ = (data - fcc2d.mean2d(data, N)) / fcc2d.std2d(data, N) if N > 0 else data
        return clean_tensor(d_)

    @classmethod
    def minmax2d(cls, data: tensor, N: int):
        d_ = ((data - fcc2d.min2d(data, N)) / (fcc2d.max2d(data, N) - fcc2d.min2d(data, N)) - 0.5) * 2 if N > 0 else data
        return clean_tensor(d_)