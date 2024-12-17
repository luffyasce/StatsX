import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import signal
import websockets
import threading
from flask import Flask, request, render_template, jsonify, send_file
from flask_cors import CORS
from gevent.pywsgi import WSGIServer
from utils.tool.encodes import JsonEncoder
from utils.tool.logger import log
from utils.tool.configer import Config
from utils.tool.network import get_local_ip
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from model.live.sub.stake_mapping import StakeMap
from model.live.sub.md_assess import MdAssess
from model.live.sub.order_archive import OrderArchive
from data.historical.data_pretreat.pretreat_data_from_local import PretreatLocal
import infra.math.option_calc as oc

RISK_FREE_RATE = 0.02

config = Config()
pth = config.path
exchange_list = config.exchange_list

temp_folder_path = os.path.join(pth, os.sep.join(['front_end', 'page', 'templates']))
static_folder_path = os.path.join(pth, os.sep.join(['front_end', 'page', 'static']))

conf = config.get_conf

logger = log(__name__, "infra", warning_only=False)


rds = Redis()

app = Flask(__name__, template_folder=temp_folder_path, static_folder=static_folder_path)
CORS(app)

stake_mapper = StakeMap()
md_assess = MdAssess()
order_archive = OrderArchive()
pretLocal = PretreatLocal()
hist_md_buf = {}
main_buf = {}
spot_buf = {}
meta_buf = None


# Lock to ensure only one thread accesses the /stakes/<opt_contract> endpoint at a time
thread_lock = threading.Lock()


def read_dataframe_from_rds(db: int, k):
    return rds.decode_dataframe(rds.get_key(db=db, k=k, decode=False))


def read_str_from_rds(db: int, k):
    return rds.get_key(db=db, k=k, decode=True)


def read_dataframe_from_db(**kwargs):
    db = UnifiedControl(db_type='base')
    return db.read_dataframe(**kwargs)


def init_meta_buffer():
    res_df = pd.DataFrame()
    for e in config.exchange_list:
        df = read_dataframe_from_db(
            db_name='processed_option_cn_meta_data', tb_name=f'contract_info_{e}',
            filter_datetime={"last_trading_date": {'gte': datetime.now().strftime('%Y-%m-%d')}},
            filter_columns=['contract', 'last_trading_date']
        )
        res_df = pd.concat([res_df, df], axis=0)
    res_df = res_df.sort_values(by='last_trading_date', ascending=True).drop_duplicates(subset=['contract'], keep='last')
    res_df['exp_days'] = (res_df['last_trading_date'] - md_assess.live_source.this_trading_date).dt.days
    res_df = res_df.set_index('contract')['exp_days']
    return res_df


def get_spot_md_data(symbol):
    spot_s = md_assess.get_spot_hist_md(symbol)
    spot_buf[symbol] = spot_s
    return spot_s


def get_hist_md_data(opt_contract, exchange):
    hist_daily_md = md_assess.get_hist_md(opt_contract, exchange)
    hist_md_buf[opt_contract] = hist_daily_md
    return hist_daily_md


def get_und_hist_md_data(contract, exchange):
    hist_daily_md = md_assess.get_und_hist_md(contract, exchange)
    hist_md_buf[contract] = hist_daily_md
    return hist_daily_md


def get_filtered_order_mapping(contract: str, start_date: datetime):
    td = md_assess.live_source.this_trading_date
    hist_fo = md_assess.base.read_dataframe(
        db_name="pretreated_future_cn_model_data",
        tb_name="filtered_orders_DIY",
        filter_datetime={'trading_date': {'gte': start_date.strftime('%Y-%m-%d')}},
        filter_keyword={'contract': {'eq': contract}}
    )
    if not hist_fo.empty:
        hist_fo = hist_fo.groupby('last')['rm_stake'].sum()
    curr_fo = md_assess.origin.read_dataframe(
        db_name="origin_future_cn_model_data",
        tb_name="filtered_orders_DIY",
        filter_datetime={'trading_date': {'eq': td.strftime('%Y-%m-%d')}},
        filter_keyword={'contract': {'eq': contract}}
    )
    if not curr_fo.empty:
        curr_fo['real_money'] = curr_fo['money_delta'] * curr_fo['information_ratio'].abs()
        curr_fo = pretLocal._pret_single_day_filter_order_(curr_fo)
    fo_s = pd.concat([hist_fo, curr_fo], axis=1).fillna(0).sum(axis=1)
    fo_df = pd.DataFrame(fo_s.rename('stake')).reset_index(names=['price']).sort_values(by='price', ascending=False)
    return fo_df


@app.route("/localChart/<key>")
def chart_page(key):
    return render_template("localChart.html", key=key)


@app.route("/localChartUnd/<key>")
def und_chart_page(key):
    return render_template("localChartUnd.html", key=key)


@app.route("/vipPosition/<val>")
def vip_pos_page(val):
    return render_template('vipPosition.html', key=val)


@app.route("/optSymMktSize")
def sentiment_page():
    return render_template('optSymMktSize.html')


@app.route("/mktSentiment")
def sym_mkt_size_page():
    return render_template('sentiment.html')


@app.route("/topSymbols")
def live_top_symbols_page():
    return render_template('topSymbols.html')


@app.route("/selected_vip_position/<val>")
def get_selected_vip_position(val):
    brokers = [i.strip().upper() for i in val.split(' ') if i.strip()]
    df = read_dataframe_from_rds(db=1, k='wl_struct').reset_index(drop=False)
    df = df[df['broker'].str.contains('|'.join(brokers))].copy()
    if not df.empty:
        df.sort_values(by=['symbol', 'contract', 'broker'])
    return df.to_json(orient="records")


@app.route("/VIP_position_hist", methods=['POST'])
def get_hist_vip_pos():
    symbol = request.json.get('symbol', '')
    broker_ls = [i.strip().upper() for i in request.json.get('broker', "").split(' ') if i.strip()]
    start_date = (datetime.now() + timedelta(days=-60)).strftime('%Y-%m-%d')
    for e in config.exchange_list:
        df = read_dataframe_from_db(
            db_name="pretreated_future_cn_model_data",
            tb_name=f"valid_position_by_contract_{e}",
            filter_datetime={'trading_date': {'gte': start_date}},
            filter_keyword={'broker': {'in': broker_ls}, 'symbol': {'eq': symbol}},
        )
        if df.empty:
            continue
        else:
            break
    if df.empty:
        return df.to_json(orient='records')
    else:
        df = df.groupby('contract').apply(
            lambda x: x.groupby('trading_date')[['net_pos']].sum()
        ).reset_index(drop=False, names=['contract', 'trading_date']).pivot(
            index='trading_date', columns='contract', values='net_pos'
        ).reset_index(drop=False)
        df['trading_date'] = df['trading_date'].dt.strftime('%Y-%m-%d')
        return df.to_json(orient='records')


@app.route("/vip_position/<key>")
def get_vip_position_table(key):
    contract = key.split('-')[0]
    symbol = contract[:-4]
    df = read_dataframe_from_rds(db=1, k='wl_struct').reset_index(drop=False)
    df = df[df['symbol'] == symbol].copy()
    return df.to_json(orient="records")


@app.route("/vip_option_position/<key>")
def get_vip_opt_pos(key):
    contract = key.split('-')[0]
    direction = key.split('-')[1]
    symbol = contract[:-4]
    exchange = md_assess.live_source.get_exchange_info(symbol)
    td = md_assess.live_source.prev_trade_date(1)
    vip_list = read_dataframe_from_rds(1, "vip_ranking")
    vip_list = vip_list[vip_list['information_score'] > 0]['broker'].tolist()
    df = read_dataframe_from_db(
        db_name="processed_option_cn_trade_data",
        tb_name=f"net_position_by_contract_{exchange}",
        filter_datetime={'trading_date': {'eq': td.strftime('%Y-%m-%d')}},
        filter_keyword={'contract': {'eq': contract}, 'direction': {'eq': direction}, 'broker': {'in': vip_list}}
    )
    if not df.empty:
        df['trading_date'] = df['trading_date'].dt.strftime("%Y-%m-%d")
        df.sort_values(by=['net_pos'], ascending=False, inplace=True)
    return df.to_json(orient="records")


@app.route("/vip_position_sym/<key>")
def get_symbol_vip_position_table(key):
    key = key if len(key) <= 2 else key[:-4]
    df = read_dataframe_from_rds(db=1, k='wl_struct').reset_index(drop=False)
    df = df[df['symbol'] == key].copy()
    return df.to_json(orient="records")


@app.route("/valid_pos_by_contract/<key>")
def get_valid_pos_by_contract(key):
    if len(key) <= 2:
        # key is symbol
        symbol = key
        exchange = md_assess.live_source.get_exchange_info(symbol)
        main_contract_df = read_dataframe_from_db(
            db_name="processed_future_cn_roll_data",
            tb_name=f"all_main_{exchange}",
            filter_keyword={'symbol': {'eq': symbol}},
            ascending=[('trading_date', False)],
            filter_row_limit=1,
            filter_columns=['O_NM_N']
        )
        if main_contract_df.empty:
            return jsonify(None)
        contract = main_contract_df.iloc[0]['O_NM_N']
    else:
        und_contract = key.split('-')[0]        # key can be underlying contract or option contract
        symbol = und_contract[:-4]
        exchange = md_assess.live_source.get_exchange_info(symbol)
        contract = und_contract
    td = md_assess.live_source.prev_trade_date(1)

    pos_df = read_dataframe_from_db(
        db_name="pretreated_future_cn_model_data",
        tb_name=f"valid_position_by_contract_{exchange}",
        filter_datetime={'trading_date': {'eq': td.strftime('%Y-%m-%d')}},
        filter_keyword={'contract': {'eq': contract}}
    )
    if not pos_df.empty:
        pos_df['trading_date'] = pos_df['trading_date'].dt.strftime("%Y-%m-%d")
        pos_df.sort_values(by=['net_pos', 'net_pos_corr'], ascending=[False, False], inplace=True)
    return pos_df.to_json(orient="records")


@app.route("/vip_option_position_und/<key>")
def get_vip_opt_pos_und(key):
    if len(key) <= 2:
        # key is symbol
        symbol = key
        exchange = md_assess.live_source.get_exchange_info(symbol)
        main_contract_df = read_dataframe_from_db(
            db_name="processed_future_cn_roll_data",
            tb_name=f"all_main_{exchange}",
            filter_keyword={'symbol': {'eq': symbol}},
            ascending=[('trading_date', False)],
            filter_row_limit=1,
            filter_columns=['O_NM_N']
        )
        if main_contract_df.empty:
            return jsonify(None)
        contract = main_contract_df.iloc[0]['O_NM_N']
    else:
        symbol = key[:-4]
        exchange = md_assess.live_source.get_exchange_info(symbol)
        contract = key
    td = md_assess.live_source.prev_trade_date(1)
    vip_list = read_dataframe_from_rds(1, "vip_ranking")
    vip_list = vip_list[vip_list['information_score'] > 0]['broker'].tolist()
    df = read_dataframe_from_db(
        db_name="processed_option_cn_trade_data",
        tb_name=f"net_position_by_contract_{exchange}",
        filter_datetime={'trading_date': {'eq': td.strftime('%Y-%m-%d')}},
        filter_keyword={'contract': {'eq': contract}, 'broker': {'in': vip_list}}
    )
    if not df.empty:
        df['trading_date'] = df['trading_date'].dt.strftime("%Y-%m-%d")
        df.sort_values(by=['direction', 'net_pos'], ascending=[True, False], inplace=True)
    return df.to_json(orient="records")


def search_opt_expiry(opt_contract):
    global meta_buf
    if meta_buf is None:
        meta_buf = init_meta_buffer()
    return meta_buf.loc[opt_contract]


def get_stakes_data(opt_contract):
    opt_md = stake_mapper.get_md(opt_contract)

    start_trading_date = opt_md['trading_date'].min()
    exchange = opt_md.iloc[0]['exchange']
    opt_md = opt_md.groupby('datetime_minute')[['last', 'open_interest']].last().rename(
        columns={'last': 'opt_last', 'open_interest': 'opt_oi'}
    )
    opt_iv = md_assess.live_source.get_tick_iv_data_by_option_contract(opt_contract, start_trading_date)
    opt_md['iv'] = opt_iv.loc[opt_md.index.intersection(opt_iv.index)].reindex(opt_md.index)
    opt_md_last = opt_md.loc[opt_md.index.max(), 'opt_last']

    contract = opt_contract.split('-')[0]
    md, stake = stake_mapper.start_mapping(contract)
    md_last = md.loc[md.index.max(), 'last']
    stake = stake.sort_index(ascending=False).reset_index(drop=False)

    iv_last = opt_md.loc[opt_md.index.max(), 'iv']
    test_price_s = pd.Series(dtype=float)

    try:
        exp = search_opt_expiry(opt_contract)
        opt_direc = opt_contract.split('-')[1]
        strike = float(opt_contract.split('-')[2])
        opt_formular = oc.baw_price_call if opt_direc == 'C' else oc.baw_price_put
        r_est = (iv_last / 15)
        r_max = round(md_last * (1 + r_est))
        r_min = round(md_last * (1 - r_est))
        inter_ = round((r_max - r_min) / 20)
        for p in range(r_min, r_max, inter_):
            opt_p = opt_formular(p, strike, exp, iv_last, RISK_FREE_RATE, 0)
            test_price_s[p] = opt_p
    except:
        opt_price_test = pd.DataFrame()
    else:
        opt_price_test = pd.DataFrame(test_price_s.rename('opt_price')).reset_index(names=['spot_price'])

    und_bo_cumsum = stake_mapper.big_order_cumsum(contract)
    opt_bo_cumsum = stake_mapper.big_order_cumsum(opt_contract)

    md = pd.concat([md, und_bo_cumsum], axis=1).sort_index(ascending=True).reset_index(drop=False, names=['datetime_minute'])
    md['datetime_minute'] = md['datetime_minute'].dt.strftime('%m%d %H:%M')
    md.fillna(method='pad', inplace=True)

    opt_md = pd.concat([opt_md, opt_bo_cumsum], axis=1).sort_index(ascending=True).reset_index(drop=False, names=['datetime_minute'])
    opt_md['datetime_minute'] = opt_md['datetime_minute'].dt.strftime('%m%d %H:%M')
    opt_md.fillna(method='pad', inplace=True)

    hist_daily_md = hist_md_buf.get(
        opt_contract, get_hist_md_data(opt_contract, exchange)
    )
    hist_daily_md.loc[stake_mapper.live_source.this_trading_date] = {'und_close': md_last, 'opt_close': opt_md_last}
    rm_daily_df = stake_mapper.big_order_daily_sum(contract, hist_daily_md.index.min())
    hist_daily_md[rm_daily_df.columns.tolist()] = rm_daily_df.loc[rm_daily_df.index.intersection(hist_daily_md.index)].reindex(hist_daily_md.index)

    spot_s = spot_buf.get(
        contract[:-4], get_spot_md_data(contract[:-4])
    )
    hist_daily_md['spot'] = spot_s.loc[hist_daily_md.index.intersection(spot_s.index)].reindex(hist_daily_md.index)

    assess_tick_sample_df = md_assess.get_tick_data(contract)
    skew_data = md_assess.calculate_tick_return_skewness(assess_tick_sample_df.copy())
    skew_data = skew_data.assign(baseline=0)
    cp_data = md_assess.call_put_cap(contract)
    md_assess_data = pd.concat([skew_data, cp_data], axis=1).sort_index(ascending=True).reset_index(
        drop=False, names=['datetime_minute']
    )
    md_assess_data['datetime_minute'] = md_assess_data['datetime_minute'].dt.strftime("%m%d %H:%M")

    rdf = read_dataframe_from_rds(1, "raw_options").reset_index(drop=True)
    rdf_contract = rdf[rdf['underlying'] == contract][['iv', 'Mkt.Value', 'contract']].copy()
    curr_iv = np.nan if opt_contract not in rdf_contract['contract'].tolist() else rdf_contract[rdf_contract['contract'] == opt_contract].iloc[0]['iv']
    rdf_contract = rdf_contract.assign(
        strike=rdf_contract['contract'].str.split('-').str.get(-1)
    ).drop(columns=['contract']).rename(
        columns={'Mkt.Value': 'mktVal'}
    )
    rdf_contract['mktVal'] = rdf_contract['mktVal'] / 1e6
    rdf_contract['strike'] = rdf_contract['strike'].astype(int)
    rdf_contract = rdf_contract.sort_values(by='strike', ascending=True)

    hist_iv = read_dataframe_from_db(
        db_name="pretreated_option_cn_md_data", tb_name=f"all_1d_opt_summary_{exchange}",
        filter_keyword={"contract": {'eq': opt_contract}},
        filter_datetime={'trading_date': {'in': hist_daily_md.index.to_series().dt.strftime("%Y-%m-%d").tolist()}},
    )
    if hist_iv.empty:
        pass
    else:
        hist_iv = hist_iv.set_index('trading_date')[['iv']]
        hist_iv.loc[stake_mapper.live_source.this_trading_date] = {'iv': curr_iv}
        hist_daily_md = pd.concat([hist_daily_md, hist_iv], axis=1)

    hist_daily_md = hist_daily_md.sort_index(ascending=True).reset_index(drop=False, names=['trading_date'])
    hist_days = hist_daily_md['trading_date']
    hist_daily_md['trading_date'] = hist_daily_md['trading_date'].dt.strftime("%Y%m%d")

    udata = order_archive.get_archived_order_data(contract)
    if udata is None:
        adf, sdf = pd.DataFrame(), pd.DataFrame()
    else:
        adf, sdf = udata
        adf['datetime_minute'] = adf['datetime_minute'].dt.strftime('%m%d %H:%M')
        sdf['datetime_minute'] = sdf['datetime_minute'].dt.strftime('%m%d %H:%M')

    odata = order_archive.get_archived_order_data(opt_contract)
    if odata is None:
        aodf, sodf = pd.DataFrame(), pd.DataFrame()
    else:
        aodf, sodf = odata
        aodf['datetime_minute'] = aodf['datetime_minute'].dt.strftime('%m%d %H:%M')
        sodf['datetime_minute'] = sodf['datetime_minute'].dt.strftime('%m%d %H:%M')

    # call put iv
    cp_iv_df = md_assess.generate_call_put_iv_data(underlying_contract=contract)
    if not cp_iv_df.empty:
        cp_iv_df['trading_date'] = cp_iv_df['trading_date'].dt.strftime("%Y%m%d")

    hist_fo_data = get_filtered_order_mapping(contract, hist_days.min())

    return {
        'price_data': md.to_json(orient='records', date_format='iso'),
        'opt_price_data': opt_md.to_json(orient='records', date_format='iso'),
        'stake_data': stake.to_json(orient='records', date_format='iso'),
        'md_assess': md_assess_data.to_json(orient='records', date_format='iso'),
        'iv_curve': rdf_contract.to_json(orient='records'),
        'hist_daily_md': hist_daily_md.to_json(orient='records', date_format='iso'),
        'und_act_vol': adf.to_json(orient='records', date_format='iso'),
        'und_swap_vol': sdf.to_json(orient='records', date_format='iso'),
        'opt_act_vol': aodf.to_json(orient='records', date_format='iso'),
        'opt_swap_vol': sodf.to_json(orient='records', date_format='iso'),
        'cp_iv_data': cp_iv_df.to_json(orient='records', date_format='iso'),
        'hist_fo_data': hist_fo_data.to_json(orient='records', date_format='iso'),
        'opt_price_test': opt_price_test.to_json(orient='records', date_format='iso'),
    }


def tread_get_stakes_data(opt_contract):
    with thread_lock:
        d = get_stakes_data(opt_contract)
        return d


@app.route("/stakes/<opt_contract>", methods=['GET'])
def stakes(opt_contract):
    data = tread_get_stakes_data(opt_contract)
    return jsonify(data)


def get_future_contract_stakes_data(symbol):
    if len(symbol) > 2:
        # 如果传入的参数是contract
        assume_contract = symbol
        symbol = symbol[:-4]
    else:
        assume_contract = None

    if symbol in main_buf.keys():
        main_contract, exchange = main_buf[symbol]
    else:
        exchange = stake_mapper.live_source.get_exchange_info(symbol=symbol)
        main_contract_df = read_dataframe_from_db(
            db_name="processed_future_cn_roll_data",
            tb_name=f"all_main_{exchange}",
            filter_keyword={'symbol': {'eq': symbol}},
            ascending=[('trading_date', False)],
            filter_row_limit=1,
            filter_columns=['O_NM_N']
        )
        if main_contract_df.empty:
            return
        main_contract = main_contract_df.iloc[0]['O_NM_N']
        main_buf[symbol] = (main_contract, exchange)

    # 若没指定 则用主力合约，否则用指定合约
    contract = assume_contract if assume_contract is not None else main_contract

    md, stake = stake_mapper.start_mapping(contract)
    md_last = md.loc[md.index.max(), 'last']
    stake = stake.sort_index(ascending=False).reset_index(drop=False)

    und_bo_cumsum = stake_mapper.big_order_cumsum(contract)

    md = pd.concat([md, und_bo_cumsum], axis=1).sort_index(ascending=True).reset_index(drop=False, names=['datetime_minute'])
    md['datetime_minute'] = md['datetime_minute'].dt.strftime('%m%d %H:%M')
    md.fillna(method='pad', inplace=True)

    hist_daily_md = hist_md_buf.get(
        contract, get_und_hist_md_data(contract, exchange)
    )
    hist_daily_md.loc[stake_mapper.live_source.this_trading_date] = {'und_close': md_last}
    rm_daily_df = stake_mapper.big_order_daily_sum(contract, hist_daily_md.index.min())
    hist_daily_md[rm_daily_df.columns.tolist()] = rm_daily_df.loc[rm_daily_df.index.intersection(hist_daily_md.index)].reindex(hist_daily_md.index)

    spot_s = spot_buf.get(
        symbol, get_spot_md_data(symbol)
    )
    hist_daily_md['spot'] = spot_s.loc[hist_daily_md.index.intersection(spot_s.index)].reindex(hist_daily_md.index)

    hist_daily_md = hist_daily_md.sort_index(ascending=True).reset_index(drop=False, names=['trading_date'])
    hist_days = hist_daily_md['trading_date']
    hist_daily_md['trading_date'] = hist_daily_md['trading_date'].dt.strftime("%Y%m%d")

    assess_tick_sample_df = md_assess.get_tick_data(contract)
    skew_data = md_assess.calculate_tick_return_skewness(assess_tick_sample_df.copy())
    skew_data = skew_data.assign(baseline=0)

    md_assess_data = skew_data.sort_index(ascending=True).reset_index(
        drop=False, names=['datetime_minute']
    )
    md_assess_data['datetime_minute'] = md_assess_data['datetime_minute'].dt.strftime("%m%d %H:%M")

    archived_data = order_archive.get_archived_order_data(contract)
    if archived_data is None:
        adf, sdf = pd.DataFrame(), pd.DataFrame()
    else:
        adf, sdf = archived_data
        adf['datetime_minute'] = adf['datetime_minute'].dt.strftime('%m%d %H:%M')
        sdf['datetime_minute'] = sdf['datetime_minute'].dt.strftime('%m%d %H:%M')

    hist_fo_data = get_filtered_order_mapping(contract, hist_days.min())

    return {
        'contract': contract,
        'price_data': md.to_json(orient='records', date_format='iso'),
        'stake_data': stake.to_json(orient='records', date_format='iso'),
        'md_assess': md_assess_data.to_json(orient='records', date_format='iso'),
        'hist_daily_md': hist_daily_md.to_json(orient='records', date_format='iso'),
        'act_vol': adf.to_json(orient='records', date_format='iso'),
        'swap_vol': sdf.to_json(orient='records', date_format='iso'),
        'hist_fo_data': hist_fo_data.to_json(orient='records', date_format='iso'),
    }


def tread_get_future_contract_stakes_data(param):
    with thread_lock:
        d = get_future_contract_stakes_data(param)
        return d


@app.route("/stakes_sym/<param>", methods=['GET'])
def stakes_symbol(param):
    data = tread_get_future_contract_stakes_data(param)
    return jsonify(data)


hist_sym_market_size_buff = None


def get_sym_mkt_size(hist_cnt: int, opt_only: bool):
    global hist_sym_market_size_buff
    if hist_sym_market_size_buff is None:
        hist_sym_market_size_buff = md_assess.hist_sym_market_size(hist_cnt)
    live_data = md_assess.live_sym_market_size()
    res = pd.concat([hist_sym_market_size_buff.copy(), live_data], axis=0).sort_index(ascending=True)
    if opt_only:
        res = res[res.index.get_level_values(0).isin(md_assess.option_symbols)].copy()
    mkt_res = res[['mkt_cap']].reset_index(drop=False).pivot_table(index='trading_date', columns='symbol', values='mkt_cap')
    oi_res = res[['open_interest']].reset_index(drop=False).pivot_table(index='trading_date', columns='symbol', values='open_interest')
    # base_ref_day
    base_ref_day = 5
    mkt_res = mkt_res / mkt_res.iloc[-base_ref_day] - 1
    oi_res = oi_res / oi_res.iloc[-base_ref_day] - 1
    return mkt_res, oi_res


def threading_get_opt_sym_mkt_size(hist_cnt):
    with thread_lock:
        res = get_sym_mkt_size(hist_cnt, opt_only=True)
        return res


@app.route("/opt_sym_mkt_size", methods=['GET'])
def get_opt_sym_mkt_size_data():
    hist_cnt = 20
    mkt_res, oi_res = threading_get_opt_sym_mkt_size(hist_cnt)
    mkt_res.reset_index(drop=False, inplace=True)
    mkt_res['trading_date'] = mkt_res['trading_date'].dt.strftime('%Y-%m-%d')
    oi_res.reset_index(drop=False, inplace=True)
    oi_res['trading_date'] = oi_res['trading_date'].dt.strftime('%Y-%m-%d')
    return {'mkt': mkt_res.to_json(orient='records'), 'oi': oi_res.to_json(orient='records')}


hist_pnl_dist_data = None


def get_pnl_dist(hist_cnt: int = 90):
    global hist_pnl_dist_data
    if hist_pnl_dist_data is None:
        start_dt = (datetime.now() + timedelta(-hist_cnt))
        res = pd.DataFrame()
        for e in exchange_list:
            t = read_dataframe_from_db(
                db_name='processed_future_cn_md_data',
                tb_name=f'pnl_distribute_1d_{e}',
                filter_datetime={'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}
            )
            res = pd.concat([res, t], axis=0)
        hist_pnl_dist_data = res.drop(columns=['exchange']).groupby('trading_date').sum()
    return hist_pnl_dist_data


def threading_get_pnl_distribute():
    with thread_lock:
        current_pnl_dist = read_dataframe_from_rds(1, 'today_pnl_distribute')['cnt']
        pnl_dist = get_pnl_dist()
        pnl_dist.loc[md_assess.live_source.this_trading_date] = current_pnl_dist
        return pnl_dist


@app.route("/pnl_dist", methods=['GET'])
def route_get_pnl_dist():
    r = threading_get_pnl_distribute()
    r = r.reset_index(drop=False, names=['trading_date'])
    r['trading_date'] = r['trading_date'].dt.strftime('%Y-%m-%d')
    return r.to_json(orient='records')


@app.route("/current_pnl_dist", methods=['GET'])
def route_current_pnl_dist():
    current_pnl_dist = read_dataframe_from_rds(1, 'today_pnl_distribute')
    current_pnl_dist['cnt'] = (current_pnl_dist['cnt'] / current_pnl_dist['cnt'].sum()) * 100
    current_pnl_dist = current_pnl_dist.reset_index(drop=False, names=['range'])
    return current_pnl_dist.to_json(orient='records')


def start_rest_server():
    ip = get_local_ip()
    logger.info(f"Server Running @ {ip}...")
    server = WSGIServer(
        ("0.0.0.0", 15168), app
    )
    server.serve_forever()


if __name__ == '__main__':
    start_rest_server()
