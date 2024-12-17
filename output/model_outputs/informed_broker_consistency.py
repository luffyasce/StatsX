import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import dataframe_image
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from model.tool.calculus_layers.pandas_calculus import ScalingCalculus
from utils.tool.configer import Config
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# config
MARKET_VALUE_COMP_DAYS = 5

ABS_RTN_STD_ROLL_WINDOW = 20


class IBConsistency:
    def __init__(self, date: datetime, PERIOD: int = 60):
        self.dt = date.strftime("%Y%m%d%H%M%S")
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.start = self.date + timedelta(days=-PERIOD)
        self.conf = Config()
        self.udc = UnifiedControl(db_type='base')
        self.rds = Redis()

        self.exchange_ls = self.conf.exchange_list

        self.zscore_ls = [5, 10, 15, 20]

    def save_model_complete_dt(self):
        self.rds.set_hash(db=1, name='model_dt', k=self.__class__.__name__, v=self.dt)

    def file_location(self, *args, filename: str):
        folder_pth = os.path.join(
            self.conf.path, os.sep.join(['front_end', 'page', 'static', 'model', 'visual_outputs', 'informed_broker_consistency', *args])
        )
        if not os.path.exists(folder_pth):
            os.makedirs(folder_pth)
        return os.path.join(
            folder_pth, filename
        )

    @property
    def md(self):
        mdf = pd.DataFrame()
        for s in self.exchange_ls:
            mdx = self.udc.read_dataframe(
                "processed_future_cn_md_data", f"all_1d_main_{s}",
                filter_keyword={'process_type': {'eq': 'O_NM_N'}},
                filter_datetime={
                    'trading_date': {'gte': (self.start + timedelta(days=-ABS_RTN_STD_ROLL_WINDOW)).strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
                }
            )
            mdf = pd.concat([mdf, mdx], axis=0)
        return mdf

    @property
    def iv_history_range_data(self):
        ivx = self.udc.read_dataframe(
            'pretreated_option_cn_md_data', 'all_1d_iv_range_DIY',
            filter_datetime={
                'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
            }
        )
        if not ivx.empty:
            ivx = ivx.set_index(['trading_date', 'underlying_contract'])
        return ivx

    @property
    def total_future_market_value(self):
        mdf = pd.DataFrame()
        mult_s = pd.Series()
        for s in self.exchange_ls:
            fut_multiplier = self.udc.read_dataframe(
                "processed_future_cn_meta_data", f"spec_info_{s}"
            ).set_index('symbol')['trade_unit']
            mdx = self.udc.read_dataframe(
                "pretreated_future_cn_md_data", f"all_1d_{s}",
                filter_datetime={
                    'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
                }
            )
            mdf = pd.concat([mdf, mdx], axis=0)
            mult_s = pd.concat([mult_s, fut_multiplier], axis=0)
        mdf = mdf[mdf['symbol'].isin(mult_s.index)].copy()
        res_fut = pd.DataFrame()
        for sym, mdi in mdf.groupby('symbol'):
            mdi['future_market_value'] = mdi['close'] * mdi['open_interest'] * mult_s.loc[sym]
            res_i = mdi.groupby('trading_date')[['future_market_value']].sum()
            res_fut = pd.concat([res_fut, res_i.reset_index(drop=False).assign(symbol=sym)], axis=0)
        return res_fut

    @property
    def total_opt_market_value(self):
        mdf = pd.DataFrame()
        mult_s = pd.Series()
        for s in self.exchange_ls:
            opt_multiplier = self.udc.read_dataframe(
                "processed_option_cn_meta_data", f"spec_info_{s}"
            ).set_index('symbol')['trade_unit']
            mdx = self.udc.read_dataframe(
                "pretreated_option_cn_md_data", f"all_1d_{s}",
                filter_datetime={
                    'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
                }
            )
            mdf = pd.concat([mdf, mdx], axis=0)
            mult_s = pd.concat([mult_s, opt_multiplier], axis=0)
        mdf = mdf[mdf['symbol'].isin(mult_s.index)].copy()
        res_opt = pd.DataFrame()
        for sym, mdi in mdf.groupby('symbol'):
            mdi['option_market_value'] = mdi['close'] * mdi['open_interest'] * mult_s.loc[sym]
            res_c = mdi[mdi['direction'] == 'C'].groupby('trading_date')['option_market_value'].sum()
            res_p = mdi[mdi['direction'] == 'P'].groupby('trading_date')['option_market_value'].sum()
            res_i = pd.concat([res_c.rename("call_market_value"), res_p.rename("put_market_value")], axis=1)
            res_opt = pd.concat(
                [
                    res_opt,
                    res_i.reset_index(drop=False).assign(symbol=sym),
                ],
                axis=0
            )
        return res_opt

    @property
    def position_main_contract(self):
        df = self.udc.read_dataframe(
            "processed_future_cn_model_data", "valid_position_by_main_contract_SUM",
            filter_datetime={
                'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
            }
        )
        return df

    @property
    def position_symbol(self):
        df = self.udc.read_dataframe(
            "processed_future_cn_model_data", "valid_position_by_symbol_SUM",
            filter_datetime={
                'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
            }
        )
        return df

    @property
    def term_structure(self):
        tdf = pd.DataFrame()
        for s in self.exchange_ls:
            tdx = self.udc.read_dataframe(
                "raw_future_cn_model_data", f"term_structure_{s}",
                filter_datetime={
                    'trading_date': {'gte': self.start.strftime('%Y-%m-%d'), 'lte': self.date.strftime('%Y-%m-%d')}
                }
            )
            tdf = pd.concat([tdf, tdx], axis=0)
        return tdf

    @staticmethod
    def __calc_fl__(factor: pd.Series, param_ls: list, tag: str):
        f_lvl = []
        for i in param_ls:
            ri = ScalingCalculus.zscore(factor, i)
            f_lvl.append(ri)
        fl = pd.concat(f_lvl, axis=1).mean(axis=1).sort_index(ascending=True)
        res = pd.concat([factor.rename(f'fac_{tag}'), fl.rename(f'fl_{tag}')], axis=1)
        return res

    def consistency_calc(self, pos_data: pd.Series):

        p_data = pos_data.copy().sort_index(ascending=True)
        informed_factor = p_data.loc[:, p_data.columns.str.startswith('informed')].iloc[:, 0]
        uninformed_factor = p_data.loc[:, p_data.columns.str.startswith('uninformed')].iloc[:, 0]

        informed_fl = self.__calc_fl__(informed_factor, self.zscore_ls, 'info')
        uninformed_fl = self.__calc_fl__(uninformed_factor, self.zscore_ls, 'uninfo')
        # 场景1： informed broker increasing position
        senario_1 = pd.Series(
            np.where(
                informed_fl.prod(axis=1) > 0,
                np.sign(informed_fl['fac_info']),
                0
            ),
            index=informed_fl.index
        )
        # # 场景2： informed broker decreasing position hugely, and uninformed broker did the exact opposite.
        # cat_df = pd.concat([informed_fl, uninformed_fl], axis=1)
        # senario_2 = pd.Series(
        #     np.where(
        #         ((cat_df[['fac_info', 'fl_info']].prod(axis=1) <= 0) & (cat_df['fl_info'].abs() > 1.5)) & (
        #                 (cat_df[['fac_uninfo', 'fl_uninfo']].prod(axis=1) <= 0) & (cat_df['fl_uninfo'].abs() > 1.5)
        #         ) & (cat_df[['fac_info', 'fac_uninfo']].prod(axis=1) < 0),
        #         -1 * np.sign(cat_df['fac_info']),
        #         0
        #     ),
        #     index=cat_df.index
        # )
        # consistency = pd.concat([senario_1, senario_2], axis=1).fillna(0).sum(axis=1).apply(np.sign)

        # 暂时只考虑场景1，因为1. 平仓的原因多种多样，但开仓一定具备信息；2. 非知情者的行为并不一定是错误的，而是不可确定的。
        consistency = senario_1
        return consistency

    def __calc_common_consistency__(self, factor_s: pd.Series):
        fac = self.__calc_fl__(factor_s, self.zscore_ls, 'common_s')
        consist_t = pd.Series(
            np.where(
                fac.prod(axis=1) > 0,
                np.sign(fac['fac_common_s']),
                0
            ),
            index=fac.index
        )
        return consist_t

    def __calc_md_dow(self, sample: pd.DataFrame):
        sample.sort_index(ascending=True, inplace=True)
        c1 = np.sign(sample['high'].diff())
        c2 = np.sign(sample['low'].diff())
        consistency_ = np.sign(c1 + c2)
        return consistency_

    def __calc_opt_mkt_diff_factor(self, sample: pd.Series):
        diff = sample.diff()
        shift_s = sample.shift()
        factor = pd.Series(
            np.where(
                ((diff * sample >= 0) & (sample * shift_s >= 0)), np.sign(diff + sample), 0
            ),
            index=diff.index
        )
        return factor

    def record_consistency_results(self, rec_name: str, res: int, symbol: str):
        self.rds.set_hash(db=1, name=rec_name, k=symbol, v=res)

    @staticmethod
    def __calc_d__(row):
        if 1 in row.values and -1 in row.values:
            return np.nan
        else:
            return np.sign(row.sum())

    def generate_output(self):
        total_future_market_value = self.total_future_market_value
        total_opt_market_value = self.total_opt_market_value
        position_main_contract = self.position_main_contract
        position_symbol = self.position_symbol
        term_structure = self.term_structure
        iv_range_data = self.iv_history_range_data

        for sym, mdf in self.md.groupby('symbol'):
            mdf_a = mdf.set_index('trading_date').sort_index(ascending=True)
            rtn_abs = (mdf_a['close'] / mdf_a['open'] - 1).abs()
            rtn_abs_roll_std = rtn_abs.rolling(ABS_RTN_STD_ROLL_WINDOW).std()

            mdf = mdf[mdf['trading_date'] >= self.start].copy()
            mdf = mdf.set_index(['trading_date', 'contract'])
            mdf[['call_avg', 'put_avg']] = iv_range_data.loc[iv_range_data.index.intersection(mdf.index)][['call_avg', 'put_avg']].reindex(mdf.index)
            mdf['cp_iv_gap'] = mdf['call_avg'] - mdf['put_avg']
            mdf = mdf.reset_index(drop=False)

            mdf = mdf.set_index('trading_date').sort_index(ascending=True)
            mdf[f'abs_rtn_std_{ABS_RTN_STD_ROLL_WINDOW}'] = rtn_abs_roll_std.loc[rtn_abs_roll_std.index.intersection(mdf.index)].reindex(mdf.index)

            md_dow = self.__calc_md_dow(mdf)

            mdf = mdf.loc[:, ['close', 'open_interest', 'call_avg', 'put_avg', 'cp_iv_gap', f'abs_rtn_std_{ABS_RTN_STD_ROLL_WINDOW}']]
            fut_mkt_val = total_future_market_value[
                total_future_market_value['symbol'] == sym
            ].set_index('trading_date').sort_index(ascending=True)[['future_market_value']]
            base_c = fut_mkt_val.fillna(0).iloc[-min(MARKET_VALUE_COMP_DAYS, len(fut_mkt_val))]['future_market_value']
            fut_mkt_val_chg = np.nan if base_c == 0 else (
                fut_mkt_val.loc[fut_mkt_val.index.max()]['future_market_value'] /
                base_c
            ) - 1
            opt_mkt_val = total_opt_market_value[
                total_opt_market_value['symbol'] == sym
            ]
            if opt_mkt_val.empty:
                opt_mkt_val = pd.DataFrame(dtype=float, columns=['call_market_value', 'put_market_value'])
                opt_call_mkt_val_chg = 0
                opt_put_mkt_val_chg = 0
            else:
                opt_mkt_val = opt_mkt_val.set_index('trading_date').sort_index(ascending=True)[[
                    'call_market_value', 'put_market_value'
                ]]
                base_call = opt_mkt_val.fillna(0).iloc[-min(MARKET_VALUE_COMP_DAYS, len(opt_mkt_val))]['call_market_value']
                base_put = opt_mkt_val.fillna(0).iloc[-min(MARKET_VALUE_COMP_DAYS, len(opt_mkt_val))]['put_market_value']
                opt_call_mkt_val_chg = np.nan if base_call == 0 else (
                    opt_mkt_val.loc[opt_mkt_val.index.max()]['call_market_value'] /
                    base_call
                ) - 1
                opt_put_mkt_val_chg = np.nan if base_put == 0 else (
                    opt_mkt_val.loc[opt_mkt_val.index.max()]['put_market_value'] /
                    base_put
                ) - 1
            snap_mkt_val_chg_result = pd.Series({
                'future_mkt_val_chg': fut_mkt_val_chg,
                'call_mkt_val_chg': opt_call_mkt_val_chg,
                'put_mkt_val_chg': opt_put_mkt_val_chg,
            })
            pos_main_df = position_main_contract[
                position_main_contract['symbol'] == sym
            ].copy().set_index('trading_date').sort_index(ascending=True).loc[
                :, ['valid_net_pos', 'informed_net_pos', 'uninformed_net_pos']
            ].rename(
                columns={
                    'valid_net_pos': 'valid_net_pos_main',
                    'informed_net_pos': 'informed_net_pos_main',
                    'uninformed_net_pos': 'uninformed_net_pos_main'
                }
            )
            pos_main_df['consistency_main'] = self.consistency_calc(pos_main_df[['informed_net_pos_main', 'uninformed_net_pos_main']])
            pos_symbol = position_symbol[
                position_symbol['symbol'] == sym
            ].copy().set_index('trading_date').sort_index(ascending=True).loc[
                 :, ['valid_net_pos', 'informed_net_pos', 'uninformed_net_pos']
            ].rename(
                columns={
                    'valid_net_pos': 'valid_net_pos_symbol',
                    'informed_net_pos': 'informed_net_pos_symbol',
                    'uninformed_net_pos': 'uninformed_net_pos_symbol'
                }
            )
            pos_symbol['consistency_symbol'] = self.consistency_calc(pos_symbol[['informed_net_pos_symbol', 'uninformed_net_pos_symbol']])
            term_struct_df = term_structure[
                term_structure['symbol'] == sym
            ].copy().set_index('trading_date').sort_index(ascending=True).loc[:, ['term_structure_slope']]

            res_df = pd.concat([mdf, fut_mkt_val, opt_mkt_val, pos_main_df, pos_symbol, term_struct_df], axis=1)
            res_df['option_mkt_value_diff'] = res_df['call_market_value'] - res_df['put_market_value']
            # res_df['consistency_option_mkt_value_diff'] = np.sign(res_df['option_mkt_value_diff'])
            # consistent_res_opt = res_df['consistency_option_mkt_value_diff'].fillna(0).apply(np.sign)
            # res_df['option_mkt_value_diff_factor'] = self.__calc_common_consistency__(res_df['option_mkt_value_diff'])
            res_df['option_mkt_value_diff_factor'] = self.__calc_opt_mkt_diff_factor(res_df['option_mkt_value_diff'])

            """"#####################################剔除极小值避免影响因子#############################################"""
            res_df['main_bar'] = res_df['informed_net_pos_main'] / res_df['open_interest']
            res_df['sym_bar'] = res_df['informed_net_pos_symbol'] / res_df['open_interest']
            res_df['opt_bar'] = res_df['option_mkt_value_diff'] / res_df[['call_market_value', 'put_market_value']].mean(axis=1)

            bar_opt = 0.02
            bar_und = 0.02

            res_df['option_mkt_value_diff_factor'] = np.where(
                res_df['opt_bar'].abs() <= bar_opt, 0, res_df['option_mkt_value_diff_factor']
            )
            res_df['consistency_main'] = np.where(
                res_df['main_bar'].abs() <= bar_und, 0, res_df['consistency_main']
            )
            res_df['consistency_symbol'] = np.where(
                res_df['sym_bar'].abs() <= bar_und, 0, res_df['consistency_symbol']
            )
            res_df['option_mkt_value_diff'] = np.where(
                res_df['opt_bar'].abs() <= bar_opt, 0, res_df['option_mkt_value_diff']
            )
            res_df['informed_net_pos_main'] = np.where(
                res_df['main_bar'].abs() <= bar_und, 0, res_df['informed_net_pos_main']
            )
            res_df['informed_net_pos_symbol'] = np.where(
                res_df['sym_bar'].abs() <= bar_und, 0, res_df['informed_net_pos_symbol']
            )

            consistent_res_opt = res_df['option_mkt_value_diff_factor'].fillna(0).apply(np.sign)

            consistent_res_und = res_df[['consistency_main', 'consistency_symbol']].fillna(0).sum(axis=1)

            """"################################ PURE BROKER POSITION CONSISTENCY ##################################"""
            # 目的是为了规避option因子的滞后性，以及其可能存在的，对于行情没有预测性，而是单纯跟随行情变化的情况进行规避。
            # consistent_res_s = consistent_res_und.apply(np.sign)

            """"################################## SYNTHETIC CONSISTENCY #######################################"""
            # 目的是为了规避broker factor信号出现失效或延迟时对交易的干扰。尽量引入一个盘面的稳定指标。
            # consistent_res_s = consistent_res_und.apply(np.sign) + consistent_res_opt

            """"############################ SYNTHETIC CONSISTENCY WITH STABLIZER #################################"""
            # 合成信号之后，再用原始因子规范一遍信号。
            consistent_res_s = (consistent_res_und.apply(np.sign) + consistent_res_opt).apply(np.sign)
            stb_opt = res_df['option_mkt_value_diff'].apply(np.sign)
            stb_und_main = res_df['informed_net_pos_main'].apply(np.sign)
            stb_und_sym = res_df['informed_net_pos_symbol'].apply(np.sign)
            stb_und = (stb_und_sym + stb_und_main).apply(np.sign)
            consistent_res_s = pd.Series(
                np.where(
                    stb_und * stb_opt <= 0, consistent_res_s, consistent_res_s * 2
                ),
                index=consistent_res_s.index
            )

            # direc_und = res_df[['valid_net_pos_main', 'valid_net_pos_symbol']].fillna(0).apply(np.sign).sum(axis=1).apply(np.sign)
            # direc_opt = res_df['option_mkt_value_diff'].apply(np.sign)
            # direc_md = md_dow
            # direc_iv = res_df['cp_iv_gap'].apply(np.sign)
            #
            # direc_indicator = pd.concat([direc_und, direc_opt, direc_md, direc_iv], axis=1).apply(self.__calc_d__, axis=1)

            # consistent_res_s = consistent_res_und.apply(np.sign) + consistent_res_opt
            # consistent_res_s = consistent_res_und
            # consistent_res_s = pd.Series(
            #     np.where(consistent_res_und == 0, consistent_res_opt, consistent_res_und),
            #     index=consistent_res_und.index
            # )

            # res_df['direction_indicator'] = direc_indicator
            res_df['consistency'] = consistent_res_s

            consist_x = np.sign(consistent_res_s)
            consist_x_group = consist_x.ne(consist_x.shift()).cumsum()
            group_sizes = consist_x_group.groupby(consist_x_group.values).size()
            recent_same_sign_days = group_sizes.iloc[-1]
            consist_int = consistent_res_s.loc[
                res_df.index.max()
            ]
            opt_mkd_sign = np.sign(res_df['option_mkt_value_diff'].fillna(0).loc[res_df.index.max()])
            # latest_direct_indicator = np.sign(res_df['direction_indicator'].fillna(0).loc[res_df.index.max()])
            if len(res_df) <= 1:
                prev_consist_int = 0
            else:
                prev_consist_int = consistent_res_s.loc[
                    res_df[res_df.index < res_df.index.max()].index.max()
                ]
            self.record_consistency_results('consistency', consist_int, sym)
            self.record_consistency_results("prev_consistency", prev_consist_int, sym)
            self.record_consistency_results("recent_direction_days_cnt", int(recent_same_sign_days), sym)
            self.record_consistency_results("opt_mkd_sign", opt_mkd_sign, sym)
            # self.record_consistency_results("latest_direction_indicator", latest_direct_indicator, sym)
            yield sym, res_df, snap_mkt_val_chg_result

    def plot_and_save(self):
        mkt_val_chg_df = pd.DataFrame()
        for s, df, snap in self.generate_output():
            mkt_val_chg_df = pd.concat([mkt_val_chg_df, snap.rename(s)], axis=1)
            df.index = df.index.to_series().astype(str)

            fig, (ax0, ax3, ax1, ax7, ax2, ax4, ax5, ax6) = plt.subplots(
                nrows=8, ncols=1, gridspec_kw={'height_ratios': [1, 1, 1, 1, 1, 1, 1, 1]},
                sharex='all', figsize=(16, 28)
            )
            df[['close', f'abs_rtn_std_{ABS_RTN_STD_ROLL_WINDOW}']].plot(kind='line', ax=ax0, secondary_y='close')
            df[['future_market_value', 'call_market_value', 'put_market_value']].plot(kind='line', secondary_y='future_market_value', ax=ax1)
            df[['cp_iv_gap']].plot(kind='bar', ax=ax7)
            df[['call_avg', 'put_avg']].plot(kind='line', secondary_y=['call_avg', 'put_avg'], ax=ax7)
            df['option_mkt_value_diff'].plot(kind='bar', secondary_y='future_market_value', color='orange', ax=ax1)
            df[['valid_net_pos_symbol', 'valid_net_pos_main']].plot(kind='bar', ax=ax2)
            # df[['direction_indicator', 'consistency']].plot(ax=ax3, kind='bar')
            df[['consistency']].plot(ax=ax3, kind='bar')
            df[['informed_net_pos_symbol', 'uninformed_net_pos_symbol']].plot(ax=ax4, kind='bar')
            df[['informed_net_pos_main', 'uninformed_net_pos_main']].plot(ax=ax5, kind='bar')
            df[['term_structure_slope']].plot.area(ax=ax6, stacked=False)
            ax0.legend(loc='upper left')
            ax1.legend(loc='upper left')
            ax2.legend(loc='upper left')
            ax3.legend(loc='upper left')
            ax4.legend(loc='upper left')
            ax5.legend(loc='upper left')
            ax6.legend(loc='upper left')
            ax7.legend(loc='upper left')
            ax0.title.set_text(f"{s} main intraday abs rtn std rolling {ABS_RTN_STD_ROLL_WINDOW} days")
            ax1.title.set_text(f"Mkt Value")
            ax2.title.set_text("valid_X")
            ax3.title.set_text("Consistency")
            ax4.title.set_text("Position_Symbol")
            ax5.title.set_text("Position_Main")
            ax6.title.set_text("TermStructure")
            ax7.title.set_text("Call Put IV Avg Gap")
            ax0.grid(True)
            ax1.grid(True)
            ax2.grid(True)
            ax3.grid(True)
            ax4.grid(True)
            ax5.grid(True)
            ax6.grid(True)
            ax7.grid(True)
            plt.xticks(np.arange(0, len(df), 5))
            plt.tight_layout()
            plt.savefig(
                self.file_location(self.dt, filename=f"{s}.jpeg")
            )
            plt.close()
        mkt_val_chg_df = mkt_val_chg_df.fillna(0).T.sort_values(by='future_mkt_val_chg', ascending=False)
        mkt_val_chg_df.to_csv(self.file_location(self.dt, filename="snap_mkt_chg.csv"))
        self.save_model_complete_dt()


if __name__ == "__main__":
    f = IBConsistency(datetime(2024, 11, 8))
    f.plot_and_save()
