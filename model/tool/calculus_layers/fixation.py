from torch import tensor


class FixationCalculus:

    @classmethod
    def reverse(cls, data: tensor):
        return data * (-1)
