"""
backtest logic with position adjustment.
"""
from typing import Any
import pandas as pd
import numpy as np
from utils.tool.logger import log

logger = log(__file__, 'strategy')


class VecWithPosAdjBacktest:
    """
    A simple basic backtest with positional adjustment.
    For accuracy, 1-minute level md data should be used.
    """
    def __init__(
            self,
            md: pd.DataFrame,
    ):
        md.index = pd.to_datetime(md.index)
        md.sort_index(ascending=True, inplace=True)
        if "pre_close" not in md.columns:
            md['pre_close'] = md['close'].shift()
        md['return'] = (md['close'] - md['pre_close']).fillna(0)
        self.df = md

    def _plain_backtest(
            self,
            sig: pd.Series,
            sig_shift: int,
            sig_padding: bool,
            max_hands: Any,
            market_value_per_point: float,
            leverage: float,
            cushion: float,
            init_price: float = None,
            cap: float = None,
            original_timeframe: bool = False,
            sig_padding_limit: int = None,
            fixed_cost: float = 0,
            close_out: bool = False,
    ):
        sig.fillna(0, inplace=True)
        res_df = self.df.copy()
        sig.index = pd.to_datetime(sig.index)
        res_df['signal'] = sig.loc[sig.index.intersection(res_df.index)].reindex(res_df.index)
        s_ = res_df['signal'].fillna(0).groupby(res_df.index.date).sum()
        if sig_padding:
            ll_ = None if sig_padding_limit is None else sig_padding_limit - 1
            res_df['signal'] = res_df['signal'].fillna(0).replace(
                to_replace=0, method='pad', limit=ll_
            ).shift(sig_shift).fillna(0)
        else:
            res_df['signal'] = res_df['signal'].shift(sig_shift).fillna(0)
        s_chg = abs(res_df['signal'].diff()).groupby(res_df.index.date).sum()
        res_df['pnl'] = (
            res_df['return'] * res_df['signal'] - (fixed_cost * (s_chg.sum()) / len(res_df))
        ) * market_value_per_point
        # calculate minimum capital required.
        ini_price = res_df[['open', 'close', 'high', 'low']].mean(axis=1).iloc[0] if init_price is None else init_price
        min_cap = (max_hands * market_value_per_point * ini_price) / ((leverage + 1) * (1 - cushion)) if cap is None else cap
        logger.info(
            f"Capital Invested: {round(min_cap, 2)}, initial price: {ini_price} "
            f", with safe cushion: {round(cushion * 100)}% -- Maximum Loss Allowed: {round(min_cap * cushion, 2)}. "
            f"Est. position change: {s_chg.sum()}"
        )
        if close_out:
            # transform pnl from price diff to percentage based on minimum initial capital
            pnl_cumsum_: pd.Series = res_df['pnl'].cumsum()
            pnl_cumsum_ = pnl_cumsum_ - pnl_cumsum_.expanding().max()
            pnl_cumsum_ = pd.Series(np.where(pnl_cumsum_ < -(cushion * min_cap), '1', '0'), index=pnl_cumsum_.index).replace(
                to_replace='0', method='pad'
            )

            if '1' in pnl_cumsum_.tolist():
                logger.warning(f"Cushion breached at {pnl_cumsum_[pnl_cumsum_ == '1'].index.min()}")
                res_df['pnl'] = np.where(pnl_cumsum_ == '1', 0, res_df['pnl'])
                flg_ = 0
            else:
                flg_ = 1
        else:
            flg_ = 1

        if original_timeframe:
            res_df_day = res_df[[
                'pnl', 'return', 'open', 'close', 'high', 'low', 'signal'
            ]].copy()
            res_df_day['benchmark_pnl'] = res_df_day['return'] * market_value_per_point
        else:
            res_df_day = res_df[['pnl', 'return']].groupby(res_df.index.date).sum()
            res_df_day['open'] = res_df['open'].groupby(res_df.index.date).first()
            res_df_day['close'] = res_df['close'].groupby(res_df.index.date).last()
            res_df_day['high'] = res_df['high'].groupby(res_df.index.date).max()
            res_df_day['low'] = res_df['low'].groupby(res_df.index.date).min()
            res_df_day['signal'] = s_
            res_df_day['benchmark_pnl'] = res_df_day['return'] * market_value_per_point

            res_df_day.index = pd.to_datetime(res_df_day.index)
        return res_df_day[['return', 'open', 'high', 'low', 'close', 'benchmark_pnl']], \
               res_df_day['pnl'], \
               res_df_day['signal'], \
               min_cap, \
               flg_

    def plain_backtest(
            self,
            sig: pd.Series,
            sig_shift: int,
            sig_padding: bool,
            max_hands: int,
            market_value_per_point: float,
            leverage: float,
            cushion: float,
            init_price: float = None,
            cap: float = None,
            show_pnl_by_percentage: bool = True,
            original_timeframe: bool = False,
            sig_padding_limit: int = None,
            fixed_cost: float = 0,
            close_out: bool = False,
    ):
        res_df_day, r2, sig, cap, flg = self._plain_backtest(
            sig, sig_shift, sig_padding, max_hands, market_value_per_point, leverage, cushion, init_price, cap,
            original_timeframe=original_timeframe, sig_padding_limit=sig_padding_limit, fixed_cost=fixed_cost,
            close_out=close_out
        )
        res_df_day.index = pd.to_datetime(res_df_day.index)
        res_df_day['pnl'] = r2
        res_df_day['signal'] = sig
        res_df_day['pnl_cumsum'] = res_df_day['pnl'].cumsum()
        res_df_day['benchmark_pnl_cumsum'] = res_df_day['benchmark_pnl'].cumsum()
        if show_pnl_by_percentage:
            res_df_day[['pnl', 'pnl_cumsum', 'benchmark_pnl', 'benchmark_pnl_cumsum']] = res_df_day[[
                'pnl', 'pnl_cumsum', 'benchmark_pnl', 'benchmark_pnl_cumsum'
            ]] / cap
        return res_df_day, flg

    def multi_strategy_plain_backtest(
            self,
            *args,
            show_pnl_by_percentage: bool = True,
            original_timeframe: bool = False,
            sig_padding_limit: int = None,
            fixed_cost: float = 0,
            close_out: bool = False,
    ):
        p2, signal = pd.Series(dtype=float), pd.Series(dtype=int)
        tot_cap = 0
        for arg_ls in args:
            res_df_day, r2, sig, cap, flg = self._plain_backtest(
                *arg_ls,
                original_timeframe=original_timeframe,
                sig_padding_limit=sig_padding_limit,
                fixed_cost=fixed_cost,
                close_out=close_out
            )
            tot_cap += cap
            if not p2.empty:
                p2 += r2
                signal += sig
            else:
                p2 = r2
                signal = sig
        res_df_day['pnl'] = p2
        res_df_day['signal'] = signal
        res_df_day['pnl_cumsum'] = res_df_day['pnl'].cumsum()
        res_df_day['benchmark_pnl_cumsum'] = res_df_day['benchmark_pnl'].cumsum()
        if show_pnl_by_percentage:
            res_df_day[['pnl', 'pnl_cumsum', 'benchmark_pnl', 'benchmark_pnl_cumsum']] = res_df_day[[
                'pnl', 'pnl_cumsum', 'benchmark_pnl', 'benchmark_pnl_cumsum'
            ]] / tot_cap
        return res_df_day, flg
