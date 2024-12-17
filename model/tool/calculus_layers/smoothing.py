from strategy.tool.torch_addin import *


class SmoothingCalculus:

    @classmethod
    def mean(cls, data: tensor, N: int):
        return clean_tensor(torch.mean(rolling(data, N), dim=-1))