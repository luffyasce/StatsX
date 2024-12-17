import pandas as pd
import torch
from torch import tensor
from typing import Union
from utils.tool.logger import log

logger = log(__file__, 'utils')


class CustomMatrix:
    def __init__(self, init_data, max_length: int = None):
        self.__data = init_data
        self.__max_length = max_length


class CusTable(CustomMatrix):
    def __init__(self, init_data: Union[pd.DataFrame, pd.Series], max_length: int = None):
        """
        This is a data class to store sample data (pandas types) that will be used to calc factors.

        :param init_data:
        :param max_length
        """
        super().__init__(init_data, max_length)
        self.__data = init_data
        self.__max_length = max_length

    @property
    def shape(self):
        return self.__data.shape

    @property
    def value(self):
        if self.__max_length and len(self.__data) < self.__max_length:
            logger.warning(f"\nVessel length less than designated: {len(self.__data)}({self.__max_length})\n")
        return self.__data.copy()

    def reset(self, new_data: Union[pd.DataFrame, pd.Series]) -> bool:
        if not isinstance(new_data, type(self.__data)):
            raise TypeError(f"Vessel type is {type(self.__data)}, but update with {type(new_data)}")
        if new_data.empty:
            return False
        else:
            self.__data = new_data
            return True

    def update(self, new_data: Union[pd.DataFrame, pd.Series]) -> bool:
        """
        update / append new data to vessel data. overlapped datetime will be trimmed off from new data.
        users must ensure by themselves that the indices are aligned and of type datetime64
        :param new_data:
        :return:
        """
        if not isinstance(new_data, type(self.__data)):
            raise TypeError(f"Vessel type is {type(self.__data)}, but update with {type(new_data)}")
        if not self.__data.empty:
            new_data = new_data[new_data.index >= self.__data.index.max()].copy()
        if self.__data.shape[0] > 0 and (new_data.index.min() - self.__data.index.max()).seconds >= 60 * 10:
            logger.warning(f"\nLive data time gap: Hist@{self.__data.index.max()} - Live@{new_data.index.min()} \n")
        if new_data.empty:
            return False
        else:
            temp_ = pd.concat(
                [self.__data[self.__data.index < new_data.index.min()].copy(), new_data], axis=0
            ).sort_index(ascending=True)
            self.__data = temp_.iloc[-self.__max_length:] if self.__max_length else temp_
            return True


class CusTensor(CustomMatrix):
    def __init__(self, init_data: tensor, max_length: int = None):
        """
        This is a data class to store sample data (only tensor) that will be used to calc factors.

        :param init_data:
        :param max_length
        """
        super().__init__(init_data, max_length)
        self.__data = init_data
        self.__max_length = max_length

    @property
    def shape(self):
        return self.__data.size()

    @property
    def value(self):
        if self.__max_length:
            if self.__data.size() == torch.Size([]):
                logger.warning(f"\nVessel length less than designated: {0}({self.__max_length})\n")
            elif self.__data.size()[0] < self.__max_length:
                logger.warning(f"\nVessel length less than designated: {self.__data.size()[0]}({self.__max_length})\n")
        return self.__data.clone().detach()

    def reset(self, new_data: tensor) -> bool:
        if not isinstance(new_data, type(self.__data)):
            raise TypeError(f"Vessel type is {type(self.__data)}, but update with {type(new_data)}")
        if new_data.size()[0] == 0:
            return False
        else:
            self.__data = new_data
            return True

    def update(self, new_data: tensor) -> bool:
        """
        since data cannot be compared without indices, user must make sure that no overlapping data were introduced.
        :param new_data:
        :return:
        """
        if new_data.size()[0] == 0:
            return False
        if not isinstance(new_data, type(self.__data)):
            raise TypeError(f"Vessel type is {type(self.__data)}, but update with {type(new_data)}")
        temp_ = new_data if (
                self.__data.size() == torch.Size([]) or self.__data.size()[0] == 0
        ) else torch.cat([self.__data, new_data], dim=0)
        self.__data = temp_[-self.__max_length:] if self.__max_length else temp_
        return True
