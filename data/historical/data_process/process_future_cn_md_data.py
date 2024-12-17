import pandas as pd
import numpy as np
from typing import Union
from datetime import datetime, time, timedelta, date
from utils.database.unified_db_control import UnifiedControl
from utils.tool.logger import log

logger = log(__file__, "data")


class ProcessFutureCnMdData:
    def __init__(self):
        self.base = UnifiedControl(db_type='base')

    @staticmethod
    def monotonicity_fail_manual_change(most_active_contract_s: pd.Series):
        """
            Ensure new most active contract is newer than old most active contract,
            if not ,Manual change to ensure monotonicity
        """
        most_active_contract_s.sort_index(ascending=True, inplace=True)
        for i in range(1, len(most_active_contract_s.index)):
            if int(most_active_contract_s.iloc[i][-4:]) < int(most_active_contract_s.iloc[i - 1][-4:]):
                most_active_contract_s.iloc[i] = most_active_contract_s.iloc[i - 1]
        return most_active_contract_s

    def process_main_roll_calendar_NM(
            self,
            pretreated_md_data_df: pd.DataFrame,
            contract_info_df: pd.DataFrame,
            proc_type: str
    ):
        contract_info_df.set_index('contract', inplace=True, drop=False)
        """process roll calendar for main contract not monotonic"""
        ls = []
        for i in pretreated_md_data_df['contract']:
            try:
                ls.append(
                    contract_info_df.loc[i]['last_trading_date']
                )
            except KeyError or IndexError:
                ls.append(np.nan)
            else:
                pass
        pretreated_md_data_df['last_trading_date'] = ls
        pretreated_md_data_df = pretreated_md_data_df.dropna(subset=['last_trading_date'])
        pretreated_md_data_df = pretreated_md_data_df.loc[
            pretreated_md_data_df[
                pretreated_md_data_df['trading_date'] <= pretreated_md_data_df['last_trading_date']
            ].index
        ]
        if proc_type == "O":
            res = pretreated_md_data_df.groupby(
                ['trading_date', 'symbol']
            ).apply(
                lambda x:
                x[x['open_interest'] == x['open_interest'].max()].sort_values(
                    by='contract', ascending=True
                ).iloc[0]['contract']
            )
            res = pd.DataFrame(res)
            res.columns = ['O_NM_N'] if not res.empty else []
        elif proc_type == "E":
            res = pretreated_md_data_df.groupby(['trading_date', 'symbol'])['contract'].min()
            res = pd.DataFrame(res)
            res = res.rename(columns={'contract': 'E_NM_N'}, errors='ignore')
        else:
            raise AttributeError(f"proc_type: {proc_type}. Wrong param here!")
        return res

    def process_main_roll_calendar(self, pretreated_future_cn_md_data_1d_df, contract_info_df):
        """
        process main roll calendar
        """
        E_NM_N = self.process_main_roll_calendar_NM(
            pretreated_future_cn_md_data_1d_df, contract_info_df, proc_type="E"
        )
        if E_NM_N.empty:
            E_M_N = pd.DataFrame()
        else:
            E_M_N=E_NM_N['E_NM_N'].groupby('symbol').apply(
                lambda x: self.monotonicity_fail_manual_change((x.droplevel(1)))
            ).unstack().unstack()
            E_M_N = pd.DataFrame(E_M_N)
            E_M_N.columns = ['E_M_N']

        O_NM_N = self.process_main_roll_calendar_NM(
            pretreated_future_cn_md_data_1d_df, contract_info_df, proc_type="O"
        )
        if O_NM_N.empty:
            O_M_N = pd.DataFrame()
        else:
            O_M_N = O_NM_N['O_NM_N'].groupby('symbol').apply(
                lambda x: self.monotonicity_fail_manual_change((x.droplevel(1)))
            ).unstack().unstack()
            O_M_N = pd.DataFrame(O_M_N)
            O_M_N.columns = ['O_M_N']

        ls = [E_M_N.dropna(), E_NM_N.dropna(), O_M_N.dropna(), O_NM_N.dropna()]
        if len(ls) != 0:
            main_roll_calendar_df = pd.concat(ls, axis=1)
            main_roll_calendar_df.reset_index(inplace=True)
            return main_roll_calendar_df

    def entry_process_main_roll_calendar(
            self,
            process_from_all: bool = True,
            save_to_all: bool = True,
            start_date: Union[str, None] = None,
            dt_delta: Union[int, None] = None,
            start_with_last: bool = True,
            data_source: str = ''
    ):
        """
        save_to_all option is only available when process_from_all is set to False.
        """
        dt_delta = -20 if dt_delta is None else dt_delta
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d") if start_date is None else start_date
        if process_from_all:
            tb_name = f"all_1d_{data_source}"
            roll_calendar_table_name = f"{tb_name.split('_')[0]}_main_{tb_name.split('_')[-1]}"
            if start_with_last:
                last_df = self.base.read_dataframe(
                    "processed_future_cn_roll_data",
                    roll_calendar_table_name,
                    ascending=[('trading_date', True)],
                    filter_row_limit=dt_delta * (-1)
                )
                start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
                filter_ = {
                    "trading_date": {"gte": start_date}
                } if start_date is not None else None
                fil_con = {'last_trading_date': {'gte': start_date}} if start_date is not None else None
            else:
                filter_ = {
                    "trading_date": {"gte": start_date}
                }
                fil_con = {'last_trading_date': {'gte': start_date}}
            contract_info = self.base.read_dataframe(
                db_name='pretreated_future_cn_meta_data',
                tb_name=f'contract_info_{data_source}',
                filter_datetime=fil_con
            )
            pretreated_future_cn_md_data_1d_df = self.base.read_dataframe(
                db_name='pretreated_future_cn_md_data',
                tb_name=f"all_1d_{data_source}",
                filter_datetime=filter_
            )

            if pretreated_future_cn_md_data_1d_df.empty or contract_info.empty:
                logger.warning(f"Encountered empty data while making roll calendar for {tb_name}")
            else:
                main_roll_calendar_df = self.process_main_roll_calendar(
                    pretreated_future_cn_md_data_1d_df,
                    contract_info
                )

                yield main_roll_calendar_df, roll_calendar_table_name
        else:
            tb_list = self.base.get_table_names('pretreated_future_cn_md_data')
            tb_list = [i for i in tb_list if 'all' not in i and '1d' in i and data_source in i]
            for tb_name in tb_list:
                sym_ = tb_name.split('_')[0]
                if save_to_all:
                    roll_calendar_table_name = f"all_main_{data_source}"
                else:
                    roll_calendar_table_name = f"{sym_}_main_{data_source}"
                if start_with_last:
                    last_df = self.base.read_dataframe(
                        "processed_future_cn_roll_data",
                        roll_calendar_table_name,
                        filter_keyword={"symbol": {"eq": sym_}},
                        ascending=[('trading_date', True)],
                        filter_row_limit=dt_delta * (-1)
                    )
                    start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
                    filter_ = {
                        "trading_date": {"gte": start_date}
                    } if start_date is not None else None
                else:
                    filter_ = {
                        "trading_date": {"gte": start_date}
                    }
                contract_info = self.base.read_dataframe(
                    db_name='pretreated_future_cn_meta_data',
                    tb_name=f'contract_info_{data_source}',
                )
                pretreated_future_cn_md_data_1d_df = self.base.read_dataframe(
                    db_name='pretreated_future_cn_md_data',
                    tb_name=tb_name,
                    filter_datetime=filter_
                )
                if pretreated_future_cn_md_data_1d_df.empty or contract_info.empty:
                    logger.warning(f"Encountered empty data while making roll calendar for {tb_name}")
                else:
                    main_roll_calendar_df = self.process_main_roll_calendar(
                        pretreated_future_cn_md_data_1d_df,
                        contract_info
                    )
                    yield main_roll_calendar_df, roll_calendar_table_name

    def save_main_roll_calendar(self, df: pd.DataFrame, tb_name: str):
        self.base.insert_dataframe(
            df=df,
            db_name='processed_future_cn_roll_data',
            tb_name=tb_name,
            set_index=['trading_date', 'symbol'],
            partition=['trading_date']
        )

    def continuous_md_no_adjustment(
            self,
            rolling_calendar_df: pd.DataFrame,
            pretreated_future_cn_md_data_df: pd.DataFrame,
            process_type: str
    ):
        """to loc signal process type continuous contract md data from pretreated df"""
        contract_df = rolling_calendar_df[['trading_date', process_type]].rename(
            columns={process_type: 'contract'}
        )
        contract_idx = pd.MultiIndex.from_tuples(
            list(zip(contract_df['trading_date'].tolist(), contract_df['contract'].tolist()))
        )
        raw_idx = pd.MultiIndex.from_tuples(
            list(zip(
                pretreated_future_cn_md_data_df['trading_date'].tolist(),
                pretreated_future_cn_md_data_df['contract'].tolist()
            ))
        )
        pretreated_future_cn_md_data_df.index = raw_idx
        processed_continuous_price_no_adjustment_df = pretreated_future_cn_md_data_df.loc[
            pretreated_future_cn_md_data_df.index.intersection(contract_idx)
        ].reset_index(drop=True)
        processed_continuous_price_no_adjustment_df['process_type'] = process_type
        return processed_continuous_price_no_adjustment_df

    def process_continuous_md_data_no_adjust(
            self, roll_calendar_df: pd.DataFrame, pretreated_future_cn_md_data_df: pd.DataFrame
    ):
        """process main continuous md data with no price adjustment and yield result"""
        if roll_calendar_df.empty and pretreated_future_cn_md_data_df.empty:
            pass
        else:
            for process_type in [i for i in roll_calendar_df.columns if i not in ['symbol', 'trading_date']]:
                processed_continuous_md_no_adjustment_df = self.continuous_md_no_adjustment(
                    rolling_calendar_df=roll_calendar_df,
                    pretreated_future_cn_md_data_df=pretreated_future_cn_md_data_df,
                    process_type=process_type
                )
                yield processed_continuous_md_no_adjustment_df

    def entry_process_main_continuous_md_data_no_adjust(
            self,
            timeframe: str,
            process_from_all: bool = True,
            save_to_all: bool = True,
            start_date: Union[str, None] = None,
            dt_delta: Union[int, None] = None,
            start_with_last: bool = True,
            data_source: str = 'RQ'
    ):
        dt_delta = -10 if dt_delta is None else dt_delta
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d") if start_date is None else start_date
        if process_from_all:
            tb_name = f'all_{timeframe}_{data_source}'
            processed_tb_name = f'all_{timeframe}_main_{data_source}'
            if start_with_last:
                last_df = self.base.read_dataframe(
                    "processed_future_cn_md_data",
                    processed_tb_name,
                    ascending=[('trading_date', False)],
                    filter_row_limit=1
                )
                start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
                filter_ = {
                    "trading_date": {"gte": start_date}
                } if start_date is not None else None
            else:
                filter_ = {
                    "trading_date": {"gte": start_date}
                }
            roll_calendar_df = self.base.read_dataframe(
                db_name='processed_future_cn_roll_data',
                tb_name=f'all_main_{data_source}',
                filter_datetime=filter_
            )
            pretreated_future_cn_md_data_df = self.base.read_dataframe(
                db_name='pretreated_future_cn_md_data',
                tb_name=tb_name,
                filter_datetime=filter_
            )
            for res_df in self.process_continuous_md_data_no_adjust(
                    roll_calendar_df, pretreated_future_cn_md_data_df
            ):
                yield res_df, processed_tb_name
        else:
            tb_list = self.base.get_table_names('pretreated_future_cn_md_data')
            tb_list = [i for i in tb_list if 'all' not in i and timeframe in i and data_source in i]
            for tb_name in tb_list:
                sym_ = tb_name.split('_')[0]
                if save_to_all:
                    processed_tb_name = f'all_{timeframe}_main_{data_source}'
                else:
                    processed_tb_name = f'{sym_}_{timeframe}_main_{data_source}'
                if start_with_last:
                    last_df = self.base.read_dataframe(
                        "processed_future_cn_md_data",
                        processed_tb_name,
                        filter_keyword={'symbol': {'eq': sym_}},
                        ascending=[('trading_date', False)],
                        filter_row_limit=1
                    )
                    start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
                    filter_ = {
                        "trading_date": {"gte": start_date}
                    } if start_date is not None else None
                else:
                    filter_ = {
                        "trading_date": {"gte": start_date}
                    }
                if save_to_all:
                    roll_calendar_df = self.base.read_dataframe(
                        db_name='processed_future_cn_roll_data',
                        filter_keyword={'symbol': {'eq': sym_}},
                        tb_name=f'all_main_{data_source}',
                        filter_datetime=filter_
                    )
                else:
                    roll_calendar_df = self.base.read_dataframe(
                        db_name='processed_future_cn_roll_data',
                        filter_keyword={'symbol': {'eq': sym_}},
                        tb_name=f'{sym_}_main_{data_source}',
                        filter_datetime=filter_
                    )
                pretreated_future_cn_md_data_df = self.base.read_dataframe(
                    db_name='pretreated_future_cn_md_data',
                    tb_name=tb_name,
                    filter_datetime=filter_
                )
                for res_df in self.process_continuous_md_data_no_adjust(
                        roll_calendar_df, pretreated_future_cn_md_data_df
                ):

                    res_df = res_df.dropna(subset=['trading_date'])
                    yield res_df, processed_tb_name

    def save_continuous_main_md_data(self, df: pd.DataFrame, tb_name: str):
        self.base.insert_dataframe(
            df,
            db_name='processed_future_cn_md_data',
            tb_name=tb_name,
            set_index=['symbol', 'datetime', 'process_type'],
            partition=['trading_date']
        )

    def process_pnl_distribute_data(self, start_with_last: bool, exchange: str, start_date: Union[str, None] = None):
        if start_with_last:
            last_df = self.base.read_dataframe(
                "processed_future_cn_md_data",
                f"pnl_distribute_1d_{exchange}",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            filter_ = {
                "trading_date": {"gte": start_date}
            } if start_date is not None else None
        else:
            filter_ = {
                "trading_date": {"gte": start_date}
            }
        md_data = self.base.read_dataframe(
            "pretreated_future_cn_md_data",
            f"all_1d_{exchange}",
            filter_datetime=filter_
        )
        if md_data.empty:
            return
        else:
            res_df = pd.DataFrame()
            for t, v in md_data.groupby('trading_date'):
                pnl = ((v['close'] / v['open']) - 1) * 100
                bins = [-float('inf'), -3, -2, -1, 0, 1, 2, 3, float('inf')]
                labels = ['<=-3%', '(-3%, -2%]', '(-2%, -1%]', '(-1%, 0%]', '(0%, 1%)', '[1%, 2%)', '[2%, 3%)', '>=3%']
                categorized_data = pd.cut(pnl, bins=bins, labels=labels)
                res_cnt = categorized_data.value_counts(sort=False)
                res_cnt = pd.DataFrame(res_cnt.rename(t)).T
                res_df = pd.concat([res_df, res_cnt], axis=0)
            res_df = res_df.reset_index(drop=False, names=['trading_date']).assign(exchange=exchange)
            return res_df

    def save_pnl_distribute_data(self, df: pd.DataFrame, exchange):
        self.base.insert_dataframe(
            df,
            "processed_future_cn_md_data",
            f"pnl_distribute_1d_{exchange}",
            set_index=['trading_date']
        )


if __name__ == "__main__":
    ptr = ProcessFutureCnMdData()
    for e in ['CZCE', 'DCE', 'SHFE', 'GFEX']:
        res = ptr.process_pnl_distribute_data(True, e)
        ptr.save_pnl_distribute_data(res, e)

    # for e in ['GFEX']:
    #     for i in ptr.entry_process_main_continuous_md_data_no_adjust(
    #         timeframe='1d', process_from_all=True,
    #         save_to_all=True, start_with_last=True, data_source=e
    #     ):
    #         ptr.save_continuous_main_md_data(*i)
