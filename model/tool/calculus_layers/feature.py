from strategy.tool.torch_addin import *


class FeatureCalculus:

    @classmethod
    def max(cls, data: tensor, N: int):
        return clean_tensor(torch.max(rolling(data, N), dim=-1).values)

    @classmethod
    def min(cls, data: tensor, N: int):
        return clean_tensor(torch.min(rolling(data, N), dim=-1).values)

    @classmethod
    def mean(cls, data: tensor, N: int):
        return clean_tensor(torch.mean(rolling(data, N), dim=-1))

    @classmethod
    def median(cls, data: tensor, N: int):
        return clean_tensor(torch.median(rolling(data, N), dim=-1).values)

    @classmethod
    def mode(cls, data: tensor, N: int):
        return clean_tensor(torch.mode(rolling(data, N), dim=-1).values)

    @classmethod
    def std(cls, data: tensor, N: int):
        return clean_tensor(torch.std(rolling(data, N), dim=-1, unbiased=False))

    @classmethod
    def mad(cls, data: tensor, N: int):
        roll_data = rolling(data, N)
        temp_ = torch.abs(roll_data - torch.mean(roll_data, dim=-1).reshape(-1, 1))
        res_ = clean_tensor(torch.mean(temp_, dim=-1))
        return res_

    @classmethod
    def sum(cls, data: tensor, N: int):
        return clean_tensor(torch.sum(rolling(data, N), dim=-1))

    @classmethod
    def signed_sum(cls, data: tensor, N: int):
        return clean_tensor(torch.sum(rolling(torch.sign(data), N), dim=-1))

    @classmethod
    def mean_diff(cls, data: tensor, N: int):
        return clean_tensor(data - cls.mean(data, N))

    @classmethod
    def skew(cls, data: tensor, N: int):
        dif_ = cls.mean_diff(data, N)
        zscore_ = torch.divide(dif_, cls.std(data, N))
        zscore_ = clean_inf(zscore_)
        return clean_tensor(torch.pow(zscore_, 3.0))

    @classmethod
    def shift(cls, data: tensor, N: int):
        return clean_tensor(torch.cat([torch.zeros((N, ), device=data.device), torch.roll(data, N)[N:]]))

    @classmethod
    def shift_diff(cls, data: tensor, N: int):
        shifted_ = torch.cat([torch.full((N, ), fill_value=torch.nan, device=data.device), cls.shift(data, N)[N:]])
        d_ = data - shifted_
        return clean_tensor(d_)

    @classmethod
    def shift_div(cls, data: tensor, N: int):
        d_ = torch.divide(data, cls.shift(data, N))
        return clean_tensor(d_)

    @classmethod
    def std_mean_diff(cls, data: tensor, N: int):
        d_ = torch.divide(cls.mean_diff(data, N), cls.std(data, N))
        return clean_tensor(d_)

    @classmethod
    def cummax_periodical(cls, data: tensor, N: int):
        d_ = torch.max(torch.cumsum(rolling(data, N), dim=-1), dim=-1).values
        return clean_tensor(d_)

    @classmethod
    def cummin_periodical(cls, data: tensor, N: int):
        d_ = torch.min(torch.cumsum(rolling(data, N), dim=-1), dim=-1).values
        return clean_tensor(d_)

    @classmethod
    def cummax_tot(cls, data: tensor, N: int):
        d_ = cls.max(torch.cumsum(data, dim=-1), N)
        return clean_tensor(d_)

    @classmethod
    def cummin_tot(cls, data: tensor, N: int):
        d_ = cls.min(torch.cumsum(data, dim=-1), N)
        return clean_tensor(d_)

    @classmethod
    def exclude_std_mean_diff(cls, data: tensor, N: int):
        d_ = cls.std_mean_diff(data, N)
        d_ = torch.where(torch.abs(d_) >= 1, d_, torch.full_like(d_, 0))
        return clean_tensor(d_)

    @classmethod
    def double_exclude_std_mean_diff(cls, data: tensor, N: int):
        d_ = cls.std_mean_diff(data, N)
        d_ = torch.where(torch.abs(d_) >= 2, d_, torch.full_like(d_, 0))
        return clean_tensor(d_)

    @classmethod
    def mean_div(cls, data: tensor, N: int):
        d_ = torch.divide(data, cls.mean(data, N))
        return clean_tensor(d_)

    @classmethod
    def log_mean_div(cls, data: tensor, N: int):
        d_ = torch.log(torch.divide(data, cls.mean(data, N)))
        return clean_tensor(d_)

    @classmethod
    def momentum(cls, data: tensor, N: int):
        d_ = torch.divide(cls.mean_diff(data, N), cls.mean(data, N))
        return clean_tensor(d_)

    @classmethod
    def diff_max(cls, data: tensor, N: int):
        return clean_tensor(data - cls.max(data, N))

    @classmethod
    def diff_min(cls, data: tensor, N: int):
        return clean_tensor(data - cls.min(data, N))

    @classmethod
    def rank(cls, data: tensor, N: int):
        return clean_tensor(torch.argsort(rolling(data, N), dim=-1, descending=False)[:, -1])

    @classmethod
    def mean_angular(cls, data: tensor, N: int):
        v_ = cls.mean(data, N)
        ang_ = (v_ * 180) / torch.square(tensor(torch.pi, device=data.device))
        d_ = torch.fmod(torch.abs(ang_), 360) * torch.sign(ang_)
        return clean_tensor(d_)

    @classmethod
    def fat_tail(cls, data: tensor, N: int):
        return clean_tensor(torch.divide(cls.mad(data, N), cls.std(data, N)))

    @classmethod
    def log_div_max(cls, data: tensor, N: int):
        return clean_tensor(torch.log(torch.divide(cls.max(data, N), data)))

    @classmethod
    def log_div_min(cls, data: tensor, N: int):
        return clean_tensor(torch.log(torch.divide(cls.min(data, N), data)))


class FeatureCalculus2D:

    @classmethod
    def max2d(cls, data: tensor, N: int):
        return clean_tensor(torch.max(rolling2d(data, N), dim=-1).values)

    @classmethod
    def min2d(cls, data: tensor, N: int):
        return clean_tensor(torch.min(rolling2d(data, N), dim=-1).values)

    @classmethod
    def mean2d(cls, data: tensor, N: int):
        return clean_tensor(torch.mean(rolling2d(data, N), dim=-1))

    @classmethod
    def median2d(cls, data: tensor, N: int):
        return clean_tensor(torch.median(rolling2d(data, N), dim=-1).values)

    @classmethod
    def mode2d(cls, data: tensor, N: int):
        return clean_tensor(torch.mode(rolling2d(data, N), dim=-1).values)

    @classmethod
    def std2d(cls, data: tensor, N: int):
        return clean_tensor(torch.std(rolling2d(data, N), dim=-1, unbiased=False))

    @classmethod
    def mad2d(cls, data: tensor, N: int):
        roll_data = rolling2d(data, N)
        temp_ = torch.abs(roll_data - torch.mean(roll_data, dim=-1).reshape(roll_data.size()[0], -1, 1))
        res_ = clean_tensor(torch.mean(temp_, dim=-1))
        return res_

    @classmethod
    def sum2d(cls, data: tensor, N: int):
        return clean_tensor(torch.sum(rolling2d(data, N), dim=-1))

    @classmethod
    def signed_sum2d(cls, data: tensor, N: int):
        return clean_tensor(torch.sum(rolling2d(torch.sign(data), N), dim=-1))

    @classmethod
    def mean_diff2d(cls, data: tensor, N: int):
        return clean_tensor(data - cls.mean2d(data, N))

    @classmethod
    def skew2d(cls, data: tensor, N: int):
        dif_ = cls.mean_diff2d(data, N)
        zscore_ = torch.divide(dif_, cls.std2d(data, N))
        zscore_ = clean_inf(zscore_)
        return clean_tensor(torch.pow(zscore_, 3.0))

    @classmethod
    def shift2d(cls, data: tensor, N: int):
        return clean_tensor(
            torch.cat(
                [torch.zeros((data.size()[0], N,), device=data.device), torch.roll(data, N, dims=1)[:, N:]],
                dim=1
            )
        )

    @classmethod
    def shift_diff2d(cls, data: tensor, N: int):
        d_ = data - cls.shift2d(data, N)
        return clean_tensor(d_)

    @classmethod
    def shift_div2d(cls, data: tensor, N: int):
        d_ = torch.divide(data, cls.shift2d(data, N))
        return clean_tensor(d_)

    @classmethod
    def std_mean_diff2d(cls, data: tensor, N: int):
        d_ = torch.divide(cls.mean_diff2d(data, N), cls.std2d(data, N))
        return clean_tensor(d_)

    @classmethod
    def cummax_periodical2d(cls, data: tensor, N: int):
        d_ = torch.max(torch.cumsum(rolling2d(data, N), dim=-1), dim=-1).values
        return clean_tensor(d_)

    @classmethod
    def cummin_periodical2d(cls, data: tensor, N: int):
        d_ = torch.min(torch.cumsum(rolling2d(data, N), dim=-1), dim=-1).values
        return clean_tensor(d_)

    @classmethod
    def cummax_tot2d(cls, data: tensor, N: int):
        d_ = cls.max2d(torch.cumsum(data, dim=-1), N)
        return clean_tensor(d_)

    @classmethod
    def cummin_tot2d(cls, data: tensor, N: int):
        d_ = cls.min2d(torch.cumsum(data, dim=-1), N)
        return clean_tensor(d_)

    @classmethod
    def exclude_std_mean_diff2d(cls, data: tensor, N: int):
        d_ = cls.std_mean_diff2d(data, N)
        d_ = torch.where(torch.abs(d_) >= 1, d_, torch.full_like(d_, 0))
        return clean_tensor(d_)

    @classmethod
    def double_exclude_std_mean_diff2d(cls, data: tensor, N: int):
        d_ = cls.std_mean_diff2d(data, N)
        d_ = torch.where(torch.abs(d_) >= 2, d_, torch.full_like(d_, 0))
        return clean_tensor(d_)

    @classmethod
    def mean_div2d(cls, data: tensor, N: int):
        d_ = torch.divide(data, cls.mean2d(data, N))
        return clean_tensor(d_)

    @classmethod
    def log_mean_div2d(cls, data: tensor, N: int):
        d_ = torch.log(torch.divide(data, cls.mean2d(data, N)))
        return clean_tensor(d_)

    @classmethod
    def momentum2d(cls, data: tensor, N: int):
        d_ = torch.divide(cls.mean_diff2d(data, N), cls.mean2d(data, N))
        return clean_tensor(d_)

    @classmethod
    def diff_max2d(cls, data: tensor, N: int):
        return clean_tensor(data - cls.max2d(data, N))

    @classmethod
    def diff_min2d(cls, data: tensor, N: int):
        return clean_tensor(data - cls.min2d(data, N))

    @classmethod
    def rank2d(cls, data: tensor, N: int):
        return clean_tensor(torch.argsort(rolling2d(data, N), dim=-1, descending=False)[:, :, -1])

    @classmethod
    def mean_angular2d(cls, data: tensor, N: int):
        v_ = cls.mean2d(data, N)
        ang_ = (v_ * 180) / torch.square(tensor(torch.pi, device=data.device))
        d_ = torch.fmod(torch.abs(ang_), 360) * torch.sign(ang_)
        return clean_tensor(d_)

    @classmethod
    def fat_tail2d(cls, data: tensor, N: int):
        return clean_tensor(torch.divide(cls.mad2d(data, N), cls.std2d(data, N)))

    @classmethod
    def log_div_max2d(cls, data: tensor, N: int):
        return clean_tensor(torch.log(torch.divide(cls.max2d(data, N), data)))

    @classmethod
    def log_div_min2d(cls, data: tensor, N: int):
        return clean_tensor(torch.log(torch.divide(cls.min2d(data, N), data)))
