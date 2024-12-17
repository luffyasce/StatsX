"""
backtest logic
another revision.
"""

import pandas as pd
import numpy as np
from utils.tool.logger import log

logger = log(__file__, 'strategy')


class VecBacktest:
    """
    A simple basic backtest
    """
    def __init__(
            self,
            pnl: pd.Series
    ):
        self.pnl = pnl

    def plain_backtest(
            self,
            sig: pd.Series,
            sig_shift: int,
            sig_padding: bool = False,
            padding_limit: int = None,
    ):
        res_df = pd.DataFrame(self.pnl.values, index=self.pnl.index, columns=['return'])
        sig.fillna(0, inplace=True)
        sig.index = pd.to_datetime(sig.index)
        res_df['signal'] = sig.loc[sig.index.intersection(res_df.index)].reindex(res_df.index)
        if sig_padding:
            ll_ = None if padding_limit is None else padding_limit - 1
            res_df['signal'] = res_df['signal'].fillna(0).replace(
                to_replace=0, method='pad', limit=ll_
            ).shift(sig_shift).fillna(0)
        else:
            res_df['signal'] = res_df['signal'].shift(sig_shift).fillna(0)
        res_df['pnl'] = res_df['return'] * res_df['signal']
        res_df['pnl_cumsum'] = res_df['pnl'].cumsum()
        res_df['benchmark_pnl'] = res_df['return']
        res_df['benchmark_pnl_cumsum'] = res_df['benchmark_pnl'].cumsum()
        return res_df
