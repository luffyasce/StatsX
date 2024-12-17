from strategy.tool.torch_addin import *


class Grouping:

    @classmethod
    def group_mean(cls, data: tensor, N: int):
        data = clean_tensor(data)
        return torch.mean(data.reshape(-1, N), dim=1)

    @classmethod
    def group_sum(cls, data: tensor, N: int):
        data = clean_tensor(data)
        return torch.sum(data.reshape(-1, N), dim=1)