import torch
from torch import tensor


def rolling(data: tensor, window: int):
    """
    Accept only 1d tensor
    """
    if window > 0:
        return torch.cat(
            [torch.full((window - 1, window), fill_value=torch.nan, device=data.device), data.unfold(0, window, 1)]
        )
    else:
        raise ValueError("rolling method must accept window value greater than 0")


def rolling2d(data: tensor, window: int):
    """
    Accept only 2d tensor
    """
    if window > 0:
        return torch.cat(
            [
                torch.full((data.size()[0], window - 1, window), fill_value=torch.nan, device=data.device),
                data.unfold(1, window, 1)
            ],
            dim=1
        )
    else:
        raise ValueError("rolling method must accept window value greater than 0")


def clean_nan(d_: tensor):
    d_ = torch.where(torch.isnan(d_), torch.full_like(d_, 0), d_)
    return d_


def clean_inf(d_: tensor):
    d_ = torch.where(torch.isinf(d_), torch.full_like(d_, 0), d_)
    return d_


def clean_tensor(d_: tensor):
    d_ = torch.where(torch.isnan(d_), torch.full_like(d_, 0), d_)
    d_ = torch.where(torch.isinf(d_), torch.full_like(d_, 0), d_)
    return d_


def zero_padding(a_: tensor, limit: int = None):
    b_ = torch.arange(a_.size()[0], device=a_.device)
    b_[a_ == 0] = 0
    c_ = torch.cummax(b_, dim=0).values
    d_ = torch.index_select(a_, dim=0, index=c_)
    if limit is not None:
        a_b = a_.bool().int()
        a_r = torch.sum(rolling(a_b, limit + 1), dim=1, dtype=torch.int)
        d_ = torch.where(a_r == 0, tensor(0, dtype=torch.float32, device=d_.device), d_)
    return d_


def zero_padding_2d(a_: tensor, limit: int = None, dim: int = 0):
    if abs(dim) > 1:
        raise AttributeError(f"This function only takes 1 / 2 dimension data, got dim value of {dim}.")
    new_a = tensor([], device=a_.device)
    for t in torch.split(a_, 1, dim):
        new_a = torch.cat([new_a, zero_padding(torch.flatten(t), limit)])
    new_a = new_a.reshape(*a_.size()) if dim == 0 else new_a.reshape(*a_.T.size()).T
    return new_a
