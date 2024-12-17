from torch import tensor
import torch
from strategy.tool.torch_addin import clean_tensor


class ActivationCalculus:

    @classmethod
    def step_sign(cls, data: tensor):
        data = clean_tensor(data)
        return torch.sign(data)

    @classmethod
    def negative_step_sign(cls, data: tensor):
        data = clean_tensor(data)
        return torch.sign(data * (-1))

    @classmethod
    def sigmoid(cls, data: tensor):
        data = clean_tensor(data)
        return 1 / (1 + torch.exp(-data))

    @classmethod
    def tanh(cls, data: tensor):
        data = clean_tensor(data)
        return torch.tanh(data)

    @classmethod
    def relu(cls, data: tensor):
        data = clean_tensor(data)
        return torch.relu(data)

    @classmethod
    def negative_relu(cls, data: tensor):
        data = clean_tensor(data)
        return torch.where(data < 0, data, torch.zeros_like(data))

    @classmethod
    def reverse_relu(cls, data: tensor):
        data = clean_tensor(data)
        return torch.relu(data) * (-1)

    @classmethod
    def reverse_negative_relu(cls, data: tensor):
        data = clean_tensor(data)
        return torch.relu(data * (-1))

    @classmethod
    def sign_relu(cls, data: tensor):
        return cls.step_sign(cls.relu(data))

    @classmethod
    def sign_negative_relu(cls, data: tensor):
        return cls.step_sign(cls.negative_relu(data))

    @classmethod
    def sign_reverse_relu(cls, data: tensor):
        return cls.step_sign(cls.reverse_relu(data))

    @classmethod
    def sign_reverse_negative_relu(cls, data: tensor):
        return cls.step_sign(cls.reverse_negative_relu(data))
