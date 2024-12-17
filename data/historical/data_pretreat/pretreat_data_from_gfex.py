import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import utils.database.unified_db_control as udc
from data.data_utils.check_alias import check_symbol_alias

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class PretreatGFEX:
    def __init__(self):
        self.base = udc.UnifiedControl(db_type='base')

    def pretreat_option_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"endTradeDate": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_meta_data",
                "contract_info_GFEX",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        df = df.drop(
            columns=['tradeType', 'variety']
        ).rename(
            columns={
                'contractId': 'raw_contract',
                'endDeliveryDate0': 'delivery_end_date',
                'varietyOrder': 'symbol',
                'startTradeDate': 'listed_date',
                'endTradeDate': 'last_trading_date',
                'unit': 'trade_unit',
                'tick': 'tick_price'
            }
        )
        df['symbol'] = df['symbol'].str.upper()
        df['contract'] = df['raw_contract'].str.upper()
        contract_splits = df['contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)-(?P<par3>[C, P])-(?P<par4>[0-9]+)")
        df['underlying_contract'] = contract_splits['par1'] + contract_splits['par2']
        df['direction'] = contract_splits['par3']
        df['strike_price'] = contract_splits['par4']
        df['exchange'] = 'GFEX'
        return df

    def save_option_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_meta_data", "contract_info_GFEX",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_contract_info_data(
            self,
            df: pd.DataFrame = None,
    ):
        start_date = datetime.now().strftime("%Y-%m-%d")
        fil_ = {"endTradeDate": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_meta_data",
                "contract_info_GFEX",
                filter_datetime=fil_
            )
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        df = df.drop(
            columns=['tradeType', 'variety']
        ).rename(
            columns={
                'contractId': 'raw_contract',
                'endDeliveryDate0': 'delivery_end_date',
                'varietyOrder': 'symbol',
                'startTradeDate': 'listed_date',
                'endTradeDate': 'last_trading_date',
                'unit': 'trade_unit',
                'tick': 'tick_price'
            }
        )
        df['symbol'] = df['symbol'].str.upper()
        df['contract'] = df['raw_contract'].str.upper()
        df['exchange'] = 'GFEX'
        return df

    def save_future_contract_info_data(
            self, df: pd.DataFrame
    ):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_meta_data", "contract_info_GFEX",
            set_index=["contract", "listed_date"],
            partition=['listed_date']
        )

    def pretreat_future_md_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True,
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df = self.base.read_dataframe(
                "pretreated_future_cn_md_data", "all_1d_GFEX",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_md_data",
                "all_1d_GFEX",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())
        df = df.drop(
            columns=['variety', 'impliedVolatility', 'delta', 'diff', 'diff1', 'matchQtySum', 'delivMonth']
        ).rename(
            columns={
                'contract': 'raw_contract',
                'varietyOrder': 'symbol',
                'lastClear': 'pre_settlement',
                'clearPrice': 'settlement',
                'volumn': 'volume',
                'openInterest': 'open_interest',
                'diffI': 'open_interest_chg'
            }
        )
        df['contract'] = df['raw_contract'].str.upper()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        df[[
            'volume', 'open_interest', 'open_interest_chg', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover'
        ]] = df[[
            'volume', 'open_interest', 'open_interest_chg', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover'
        ]].astype(float)
        df['turnover'] = df['turnover'] * 1e4
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df

    def save_future_md_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_future_cn_md_data", "all_1d_GFEX",
            set_index=['contract', 'datetime'],
            partition=['trading_date']
        )

    def pretreat_option_md_data(
            self,
            md_df: pd.DataFrame = None,
            iv_df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df1 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_GFEX",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            last_df2 = self.base.read_dataframe(
                "pretreated_option_cn_md_data", "all_1d_opt_summary_GFEX",
                ascending=[('datetime', False)],
                filter_row_limit=1
            )
            start_date1 = last_df1.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df1.empty else None
            start_date2 = last_df2.iloc[0]['datetime'].strftime("%Y-%m-%d") if not last_df2.empty else None
            start_date = None if start_date1 is None or start_date2 is None else min(start_date1, start_date2)
            fil_ = {"datetime": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"datetime": {"gte": start_date}}
        if md_df is None:
            md_df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_GFEX",
                filter_datetime=fil_
            )
            iv_df = self.base.read_dataframe(
                "raw_option_cn_md_data",
                "all_1d_iv_GFEX",
                filter_datetime=fil_
            )
        else:
            pass
        md_df = md_df.applymap(lambda x: x if type(x) != str else x.strip())
        iv_df = iv_df.applymap(lambda x: x if type(x) != str else x.strip())

        iv_df['seriesId'] = iv_df['seriesId'].str.upper()
        iv_df = iv_df.set_index(['datetime', 'seriesId'])['hisVolatility']

        md_df['raw_contract'] = md_df['contract']
        md_df['contract'] = md_df['raw_contract'].str.upper()
        contract_splits = md_df['contract'].str.extract("(?P<par1>[A-Z]+)(?P<par2>[0-9]+)-(?P<par3>[C, P])-(?P<par4>[0-9]+)")
        md_df['contract'] = md_df['contract'].apply(
            lambda x: check_symbol_alias(re.findall("[A-Z]+", x)[0]) + x[len(re.findall("[A-Z]+", x)[0]):]
        )

        md_df['direction'] = contract_splits['par3']
        md_df['strike_price'] = contract_splits['par4']
        md_df['underlying_contract'] = md_df['contract'].apply(lambda x: x.split('-')[0])
        md_df['symbol'] = md_df['underlying_contract'].apply(lambda x: x[:-4])
        md_df.set_index(['datetime', 'underlying_contract'], drop=False, inplace=True)
        md_df['iv'] = iv_df.loc[iv_df.index.intersection(md_df.index)].reindex(md_df.index)
        md_df.reset_index(drop=True, inplace=True)
        md_df = md_df.rename(
            columns={
                'lastClear': 'pre_settlement',
                'clearPrice': 'settlement',
                'volumn': 'volume',
                'openInterest': 'open_interest',
                'diffI': 'open_interest_chg',
                'matchQtySum': 'exec_volume',
            }
        ).drop(
            columns=['variety', 'impliedVolatility', 'diff', 'diff1', 'delivMonth', 'varietyOrder']
        ).fillna(0)
        md_df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]] = md_df[[
            'volume', 'open_interest', 'open_interest_chg', 'exec_volume', 'pre_settlement', 'open', 'high', 'low',
            'close', 'settlement', 'turnover', 'iv', 'delta'
        ]].astype(float)
        md_df['turnover'] = md_df['turnover'] * 1e4
        md_df['datetime'] = pd.to_datetime(md_df['datetime'])
        md_df['trading_date'] = pd.to_datetime(md_df['trading_date'])
        opt_df = md_df[[
            'raw_contract', 'contract', 'underlying_contract', 'direction', 'strike_price', 'symbol', 'datetime',
            'trading_date', 'delta', 'iv', 'exec_volume'
        ]].copy()
        md_df.drop(columns=['delta', 'iv', 'exec_volume'], inplace=True)
        return md_df, opt_df

    def save_option_md_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_GFEX",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def save_option_summary_data(self, df: pd.DataFrame):
        self.base.insert_dataframe(
            df, "pretreated_option_cn_md_data", "all_1d_opt_summary_GFEX",
            set_index=['underlying_contract', 'direction', 'strike_price', 'datetime'],
            partition=['trading_date']
        )

    def pretreat_future_position_rank_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df = self.base.read_dataframe(
                "pretreated_future_cn_trade_data", "position_rank_by_contract_GFEX",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_future_cn_trade_data",
                "position_rank_GFEX",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df.rename(
            columns={
                'contract': 'raw_contract',
                'contractId': 'rank',
                'vol_abbr': 'broker_vol',
                'vol_todayQty': 'vol',
                'vol_qtySub': 'vol_chg',
                'buy_abbr': 'broker_long',
                'buy_todayQty': 'long',
                'buy_qtySub': 'long_chg',
                'sell_abbr': 'broker_short',
                'sell_todayQty': 'short',
                'sell_qtySub': 'short_chg',
            }
        )
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)

        df['contract'] = df['raw_contract'].str.upper()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])
        return df

    def save_future_position_rank_data(self, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfc, "pretreated_future_cn_trade_data", "position_rank_by_contract_GFEX",
            set_index=['contract', 'trading_date', 'rank'], partition=['trading_date']
        )

    def pretreat_option_position_rank_data(
            self,
            df: pd.DataFrame = None,
            start_with_last: bool = True
    ):
        dt_delta = -5
        start_date = (datetime.now() + timedelta(dt_delta)).strftime("%Y-%m-%d")
        if start_with_last:
            last_df = self.base.read_dataframe(
                "pretreated_option_cn_trade_data", "position_rank_by_contract_GFEX",
                ascending=[('trading_date', False)],
                filter_row_limit=1
            )
            start_date = last_df.iloc[0]['trading_date'].strftime("%Y-%m-%d") if not last_df.empty else None
            fil_ = {"trading_date": {"gte": start_date}} if start_date is not None else None
        else:
            fil_ = {"trading_date": {"gte": start_date}}
        if df is None:
            df = self.base.read_dataframe(
                "raw_option_cn_trade_data",
                "position_rank_GFEX",
                filter_datetime=fil_
            )
        else:
            pass
        df = df.applymap(lambda x: x if type(x) != str else x.strip())

        df = df.rename(
            columns={
                'contract': 'raw_contract',
                'memberId': 'rank',
                'vol_abbr': 'broker_vol',
                'vol_todayQty': 'vol',
                'vol_qtySub': 'vol_chg',
                'buy_abbr': 'broker_long',
                'buy_todayQty': 'long',
                'buy_qtySub': 'long_chg',
                'sell_abbr': 'broker_short',
                'sell_todayQty': 'short',
                'sell_qtySub': 'short_chg',
                'cpFlag': 'direction'
            }
        )
        df[['broker_vol', 'broker_long', 'broker_short']] = df[['broker_vol', 'broker_long', 'broker_short']].applymap(
            lambda x: x.strip('（代客）') if isinstance(x, str) else x)
        df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]] = df[[
            'vol', 'vol_chg', 'long', 'long_chg', 'short', 'short_chg'
        ]].astype(float)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df['rank'] = df['rank'].astype(int)

        df['contract'] = df['raw_contract'].str.upper()
        df['contract'] = [f"{check_symbol_alias(contract_[:-4])}{contract_[-4:]}" for contract_ in df['contract']]
        df['symbol'] = df['contract'].apply(lambda x: x[:-4])

        return df

    def save_option_position_rank_data(self, dfc: pd.DataFrame):
        self.base.insert_dataframe(
            dfc, "pretreated_option_cn_trade_data", "position_rank_by_contract_GFEX",
            set_index=['contract', 'direction', 'trading_date', 'rank'], partition=['trading_date']
        )


if __name__ == "__main__":
    ptr = PretreatGFEX()
    df = ptr.pretreat_option_contract_info_data()
    ptr.save_option_contract_info_data(df)
    df = ptr.pretreat_future_contract_info_data()
    ptr.save_future_contract_info_data(df)
    mdf, odf = ptr.pretreat_option_md_data()
    ptr.save_option_md_data(mdf)
    ptr.save_option_summary_data(odf)
    df = ptr.pretreat_future_md_data()
    ptr.save_future_md_data(df)

    res = ptr.pretreat_future_position_rank_data(start_with_last=True)
    ptr.save_future_position_rank_data(res)
    res = ptr.pretreat_option_position_rank_data(start_with_last=True)
    ptr.save_option_position_rank_data(res)