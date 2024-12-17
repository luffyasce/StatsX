import torch

from strategy.tool.torch_addin import *


class FeatureEvaluation:

    @classmethod
    def _corr(cls, sample: tensor, ignore_nil: bool):
        sample = torch.where(torch.isnan(sample), torch.zeros_like(sample), sample)
        original_length = sample.size()[1]
        sample = sample if not ignore_nil else torch.index_select(
            sample,
            dim=1,
            index=torch.squeeze((sample[0, :] != 0).nonzero())
        )
        new_length = sample.size()[1]
        if new_length / original_length <= 0.02:
            return torch.tensor(torch.nan, device=sample.device)
        else:
            corr_ = torch.corrcoef(sample)
            return torch.tensor(torch.round(corr_[0, 1] * 100) / 100, device=sample.device)

    @classmethod
    def window_ic(cls, series_A: tensor, series_B: tensor, N: int, p: int, ignore_nil: bool = True):
        sample_tensor = torch.cat(
            (torch.reshape(series_A, (1, series_A.size()[0])), torch.reshape(series_B, (1, series_B.size()[0]))), 0
        )
        leng_ = sample_tensor.size()[1]
        cor_s = tensor(
            [cls._corr(sample_tensor[:, i: i + N], ignore_nil) for i in range(0, leng_ - N + p, p)],
            device=sample_tensor.device
        )
        if len(cor_s) == 0:
            return tensor(0, device=sample_tensor.device), tensor(0, device=sample_tensor.device)
        else:
            cor_s_ = cor_s.reshape(cor_s.size()[0], -1)
            cor_s_ = cor_s_[~torch.any(cor_s_.isnan(), dim=1)]
            cor_s = torch.squeeze(cor_s_)
            return torch.mean(cor_s), torch.mean(cor_s) / torch.std(cor_s)

    @classmethod
    def information_correlation(cls, series_A: tensor, series_B: tensor, ignore_nil: bool = True):
        """Only accept 1d data"""
        sample_tensor = torch.cat(
            (torch.reshape(series_A, (1, series_A.size()[0])), torch.reshape(series_B, (1, series_B.size()[0]))), 0
        )
        return cls._corr(sample_tensor, ignore_nil)

    @classmethod
    def pnl_test(
            cls, feature_t: tensor, benchmark_pnl_t: tensor, shift: int, padding: bool, padding_limit: int = None,
            trade_cost: float = 0
    ):
        """
        basic pnl test made compatible with torch tensor.
        :param feature_t:
        :param benchmark_pnl_t:
        :param shift:
        :param padding:
        :param padding_limit:
        :param trade_cost:
        :return:
        """
        pos_chg = feature_t - clean_tensor(
            torch.cat([torch.zeros((1,), device=feature_t.device), torch.roll(feature_t, 1)[1:]])
        )
        cost = torch.abs(pos_chg) * trade_cost
        if padding:
            feature_t = zero_padding(feature_t, padding_limit)
        feature_t = clean_tensor(
            torch.cat([torch.zeros((shift,), device=feature_t.device), torch.roll(feature_t, shift)[shift:]])
        )
        pnl_ = benchmark_pnl_t * feature_t - cost
        wnl_ratio = torch.sum(
            torch.where(pnl_ > 0, torch.ones_like(pnl_), torch.zeros_like(pnl_)), dim=-1
        ) / torch.sum(
            torch.where(pnl_ != 0, torch.ones_like(pnl_), torch.zeros_like(pnl_)), dim=-1
        )
        pnl_cumsum = torch.cumsum(pnl_, dim=0)
        mdd_ = torch.min(pnl_cumsum - torch.cummax(pnl_cumsum, dim=0).values)
        return pnl_, pnl_cumsum, mdd_, wnl_ratio

    @classmethod
    def sharpe_value(cls, test_pnl: tensor, scaler: int):
        s_ = torch.div(
            torch.mean(test_pnl, dim=0), torch.std(test_pnl, dim=0)
        ) * torch.sqrt(tensor(scaler, device=test_pnl.device))
        return s_

    @classmethod
    def pnl_test_2d(
            cls,
            feature_t: tensor,
            benchmark_pnl_t: tensor,
            shift: int,
            padding: bool,
            padding_limit: int = None,
            trade_cost: float = 0,
    ):
        pos_chg = feature_t - clean_tensor(
            torch.cat([torch.zeros((feature_t.size()[0], 1), device=feature_t.device), torch.roll(feature_t, 1)[:, 1:]], dim=1)
        )
        cost = torch.abs(pos_chg) * trade_cost
        if padding:
            feature_t = zero_padding_2d(feature_t, padding_limit, dim=0)
        feature_t = clean_tensor(
            torch.cat(
                [
                    torch.zeros((feature_t.size()[0], shift), device=feature_t.device),
                    torch.roll(feature_t, shift)[:, shift:]
                ],
                dim=1
            )
        )
        pnl_ = benchmark_pnl_t * feature_t - cost
        pnl_cumsum = torch.cumsum(pnl_, dim=1)
        mdd_ = torch.min(pnl_cumsum - torch.cummax(pnl_cumsum, dim=1).values, dim=1).values
        return pnl_, pnl_cumsum, mdd_
