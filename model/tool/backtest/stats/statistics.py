import pandas as pd
import numpy as np
from empyrical import alpha_beta, sharpe_ratio, max_drawdown, annual_return, tail_ratio, \
    roll_sharpe_ratio, roll_max_drawdown
from numba import jit, float64


@jit(nopython=True)
def _accumulate(s: np.array, typ_: int):
    """
    :param s:
    :param typ_: 0 -- equal to 0 | 1 -- greater than 0 | 2 -- less than 0
    :return:
    """
    ls = np.array([float64(0)])
    cum = 0
    if typ_ == 0:
        for i in s:
            if i == 0:
                cum = 0
            else:
                cum += i
            ls = np.append(ls, cum)
    elif typ_ == 1:
        for i in s:
            if i > 0:
                cum = 0
            else:
                cum += i
            ls = np.append(ls, cum)
    else:
        for i in s:
            if i < 0:
                cum = 0
            else:
                cum += i
            ls = np.append(ls, cum)
    return ls[1:]


class BasicEval:
    def __init__(self):
        self.pnl = 'pnl'
        self.bench_pnl = 'benchmark_pnl'

    @staticmethod
    def emp_eval(pnl_sum: pd.Series):
        sharpe = sharpe_ratio(pnl_sum, period='daily')
        max_down = max_drawdown(pnl_sum)
        wnl_ratio = len(pnl_sum[pnl_sum > 0]) / len(pnl_sum[pnl_sum != 0]) if len(pnl_sum[pnl_sum != 0]) != 0 else 0
        ann_return_strat = annual_return(pnl_sum, period='daily')
        t_r = tail_ratio(pnl_sum)
        return {
            'sharpe': round(sharpe, 4),
            'max_single_day_drawdown': round(pnl_sum.min(), 4),
            'max_drawdown': round(max_down, 4),
            'factor_pnl': round(pnl_sum.values.sum(), 4),
            'factor_annual_pnl': round(ann_return_strat, 4),
            'tail_ratio': round(t_r, 4),
            'w/l_ratio (exclude flats)': wnl_ratio,
            'return_on_risk_ratio': round(ann_return_strat / max_down, 4)
        }

    def evaluate(self, res_df):
        res_df['date'] = res_df.index.date
        pnl_sum = res_df.groupby('date')[self.pnl].sum()
        bench_pnl_sum = res_df.groupby('date')[self.bench_pnl].sum()
        (alpha, beta) = alpha_beta(pnl_sum, bench_pnl_sum, period='daily')
        sharpe = sharpe_ratio(pnl_sum, period='daily')
        bench_sharpe = sharpe_ratio(bench_pnl_sum, period='daily')
        max_down = max_drawdown(pnl_sum)
        wnl_ratio = len(pnl_sum[pnl_sum > 0]) / len(pnl_sum[pnl_sum != 0]) if len(pnl_sum[pnl_sum != 0]) != 0 else 0
        ann_return_strat = annual_return(pnl_sum, period='daily')
        ann_return_bench = annual_return(bench_pnl_sum, period='daily')
        t_r = tail_ratio(pnl_sum)
        sig_sample = res_df['signal'].fillna(0)
        hold_period = np.count_nonzero(sig_sample)
        trade_period = abs(sig_sample - sig_sample.shift().fillna(0)).fillna(0)
        turnover = trade_period.sum() / hold_period if hold_period != 0 else 0

        full_profit = abs(res_df[self.bench_pnl]).sum()
        factor_pnl = res_df[self.pnl].sum()
        extraction_ratio = factor_pnl / full_profit

        single_drawdown = pd.Series(np.where(pnl_sum < 0, pnl_sum, 0), index=pnl_sum.index)
        single_profit = pd.Series(np.where(pnl_sum > 0, pnl_sum, 0), index=pnl_sum.index)
        ls_d = _accumulate(single_drawdown.to_numpy(), typ_=0)
        ls_p = _accumulate(single_profit.to_numpy(), typ_=0)
        drawdown_cum = pd.Series(ls_d, index=single_drawdown.index)
        profit_cum = pd.Series(ls_p, index=single_profit.index)

        lsc_d = _accumulate(pnl_sum.to_numpy(), typ_=1)
        lsc_p = _accumulate(pnl_sum.to_numpy(), typ_=2)
        drawdown_tot_cum = pd.Series(lsc_d, index=pnl_sum.index)
        profit_tot_cum = pd.Series(lsc_p, index=pnl_sum.index)

        pnl_df = pd.concat(
            [
                pnl_sum.cumsum(), single_drawdown, drawdown_cum, drawdown_tot_cum,
                single_profit, profit_cum, profit_tot_cum
            ],
            axis=1
        )
        pnl_df.columns = [
            'pnl_cumsum',
            f'single_drawdown', f"cummulative_consecutive_drawdown", f"cummulative_drawdown",
            f'single_profit', f"cummulative_consecutive_profit", f"cummulative_profit"
        ]

        stats = {
            'alpha': round(alpha, 4),
            'beta': round(beta, 4),
            'sharpe': round(sharpe, 4),
            'benchmark_sharpe': round(bench_sharpe, 4),
            'factor_pnl': res_df[self.pnl].sum(),
            'factor_annual_pnl': ann_return_strat,
            self.bench_pnl: res_df[self.bench_pnl].sum(),
            'benchmark_annual_pnl': ann_return_bench,
            f'max_single_drawdown': round(pnl_sum.min(), 4),
            f'max_single_profit': round(pnl_sum.max(), 4),
            f'max_cumulative_consecutive_drawdown': round(drawdown_cum.min(), 4),
            f'max_cumulative_consecutive_profit': round(profit_cum.max(), 4),
            f'max_cumulative_drawdown': round(drawdown_tot_cum.min(), 4),
            f'max_cumulative_profit': round(profit_tot_cum.max(), 4),
            'max_drawdown': round(max_down, 4),
            'tail_ratio': round(t_r, 4),
            'turnover': round(turnover, 4),
            'profit_extraction_ratio': round(extraction_ratio, 4),
            'w/l_ratio (exclude flats)': wnl_ratio
        }
        stats_df = pd.DataFrame.from_dict(stats, orient='index')
        stats_df.columns = ["Stats"]

        return stats_df, pnl_df

    def roll_evaluate(self, res_df: pd.DataFrame, w: int):
        roll_sharp = roll_sharpe_ratio(res_df[self.pnl], w)
        roll_drawdown = roll_max_drawdown(res_df[self.pnl], w)
        _r = pd.concat([roll_sharp, roll_drawdown], axis=1)
        _r.columns = ['roll_sharpe', 'roll_max_drawdown']
        if len(res_df) < w:
            _r['roll_sharpe'] = np.nan
        return _r

    def evaluate_each_signal_performance(
            self,
            returns: pd.Series,
            signal_df: pd.DataFrame,
            shift: int,
            compounded_signal: pd.Series = None,
            compounded_signal_shift: int = None
    ):
        """
        To evaluate all sub signal performance.
        ***
        Notice: the signal df will be loc and clipped according to the index of return series.
                So if you want to evaluate performance under a certain period of time, just pass in the return series
                of that period, and signals will be automatically clipped accordingly.
        :param returns: the return series under which time period that you want to evaluate.
        :param signal_df: a signal dataframe containing all sub signals
        :param shift: signal shift value
        :param compounded_signal
        :param compounded_signal_shift
        :return: evaluated result dataframe & all cumulative pnl dataframe.
        """
        signal_df = signal_df.loc[signal_df.index.intersection(returns.index)].reindex(returns.index)
        signal_df = signal_df.shift(shift).T.fillna(0)
        eval_df = pd.DataFrame()
        benchmark_cumsum: pd.Series = returns.cumsum()
        benchmark_cumsum.name = 'benchmark'
        pnl_cumsum_df = pd.DataFrame(benchmark_cumsum)
        vals_df = pd.DataFrame()
        for k, v in signal_df.iterrows():
            _pnl: pd.Series = returns * v
            val_s = _pnl.copy()
            val_s.name = k
            vals_df = vals_df.append(val_s)
            _res_df = pd.concat(
                [_pnl, returns, v], axis=1
            )
            _res_df.columns = [self.pnl, self.bench_pnl, 'signal']
            _eval = self.evaluate(_res_df)[0]
            _eval.columns = [k]
            eval_df = pd.concat([eval_df, _eval], axis=1)
            pnl_cumsum_df = pd.concat([pnl_cumsum_df, pd.DataFrame(_pnl.cumsum(), columns=[k])], axis=1)
        if compounded_signal is not None and compounded_signal_shift is not None:
            compounded_signal = compounded_signal.loc[
                compounded_signal.index.intersection(returns.index)
            ].reindex(returns.index)
            v_com = compounded_signal.shift(compounded_signal_shift)
            _pnl_com = returns * v_com
            _res_df_com = pd.concat(
                [_pnl_com, returns, v_com], axis=1
            )
            _res_df_com.columns = [self.pnl, self.bench_pnl, 'signal']
            _eval_com = self.evaluate(_res_df_com)[0]
            _eval_com.columns = ['compounded']
            eval_df = pd.concat([eval_df, _eval_com], axis=1)
            pnl_cumsum_df = pd.concat([pnl_cumsum_df, pd.DataFrame(_pnl_com.cumsum(), columns=['compounded'])], axis=1)
        return eval_df, pnl_cumsum_df, vals_df


class BasicDesc(BasicEval):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df

    def backtest_eval(self):
        return self.evaluate(self.df)

    def count_performance(self):
        """count positive/negative returns"""
        perf_df = pd.DataFrame(
            [
                [
                    len(self.df[(self.df['signal'] > 0) & (self.df[self.pnl] > 0)]),
                    len(self.df[(self.df['signal'] > 0) & (self.df[self.pnl] < 0)]),
                    len(self.df[(self.df['signal'] < 0) & (self.df[self.pnl] > 0)]),
                    len(self.df[(self.df['signal'] < 0) & (self.df[self.pnl] < 0)]),
                    len(self.df[self.df['signal'] == 0]),
                ],
                [
                    self.df[(self.df['signal'] > 0) & (self.df[self.pnl] > 0)][self.pnl].sum(),
                    abs(self.df[(self.df['signal'] > 0) & (self.df[self.pnl] < 0)][self.pnl].sum()),
                    self.df[(self.df['signal'] < 0) & (self.df[self.pnl] > 0)][self.pnl].sum(),
                    abs(self.df[(self.df['signal'] < 0) & (self.df[self.pnl] < 0)][self.pnl].sum()),
                    abs(self.df[self.df['signal'] == 0][self.bench_pnl].sum())
                ]
            ],
            index=['count', 'total'],
            columns=['long_profit', 'long_loss', 'short_profit', 'short_loss', 'neutral']
        ).T
        return perf_df

    def quantile_describe_pnl(self, valve: float):
        pnl_s = pd.DataFrame(
            [
                [
                    abs(self.df[self.df[self.pnl] >= self.df[self.pnl].quantile(valve)][self.pnl].sum()),
                    abs(self.df[self.df[self.pnl] < self.df[self.pnl].quantile(valve)][self.pnl].sum()),
                ],
                [
                    abs(self.df[self.df[self.pnl] >= self.df[self.pnl].quantile(valve)][self.pnl].mean()),
                    abs(self.df[self.df[self.pnl] < self.df[self.pnl].quantile(valve)][self.pnl].mean()),
                ]
            ],
            index=['sum', 'avg'],
            columns=['upper', 'lower']
        ).T
        return pnl_s

