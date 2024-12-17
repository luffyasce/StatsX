import io
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import signal
import websockets
import threading
from flask import Flask, request, render_template, jsonify, send_file, url_for
from flask_cors import CORS
from gevent.pywsgi import WSGIServer
from utils.tool.encodes import JsonEncoder
from utils.tool.logger import log
from utils.tool.configer import Config
from utils.database.unified_db_control import UnifiedControl
from utils.buffer.redis_handle import Redis
from utils.tool.network import get_local_ip
from model.live.sub.stake_mapping import StakeMap
from model.live.sub.md_assess import MdAssess
from model.live.sub.oi_vol_assess import OiVolAssess
from infra.math.option_calc import baw_price_call, baw_price_put

config = Config()
pth = config.path
temp_folder_path = os.path.join(pth, os.sep.join(['front_end', 'page', 'templates']))
static_folder_path = os.path.join(pth, os.sep.join(['front_end', 'page', 'static']))

conf = config.get_conf

logger = log(__name__, "infra", warning_only=False)

# Lock to ensure only one thread accesses the /stakes/<opt_contract> endpoint at a time
thread_lock = threading.Lock()

rds = Redis()

app = Flask(__name__, template_folder=temp_folder_path, static_folder=static_folder_path)
CORS(app)

stake_mapper = StakeMap()
md_assess = MdAssess()
oiVol = OiVolAssess()


def model_output_file_location(*args, filename: str):
    x_args = '/'.join(args)
    fp = url_for('static', filename=f'model/{x_args}/{filename}')
    return fp


def model_temp_data_location(*args, filename: str):
    x_args = '/'.join(args)
    folder_pth = os.path.join(
        pth,
        os.sep.join(['front_end', 'page', 'static', 'model', *args])
    )
    fp = os.path.join(folder_pth, filename)
    return fp


# configs


def read_dataframe_from_rds(db: int, k):
    return rds.decode_dataframe(rds.get_key(db=db, k=k, decode=False))


def read_str_from_rds(db: int, k):
    return rds.get_key(db=db, k=k, decode=True)


def read_dataframe_from_db(**kwargs):
    db = UnifiedControl(db_type='base')
    return db.read_dataframe(**kwargs)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/save_top_symbols", methods=['POST'])
def save_top_symbols():
    sym_str = request.json.get('topSymbols', '')
    rds.set_key(db=1, k='top_symbols', v=sym_str)
    return jsonify(1)


@app.route("/opt_calc", methods=['POST'])
def calc_opt_price_baw():
    typ_ = request.json.get('type', 0)
    und = request.json.get('undP')
    strike = request.json.get('strike')
    rate = request.json.get('rate')
    vol = request.json.get('vol')
    exp = request.json.get('exp')

    if typ_ == 1:
        r = baw_price_call(und, strike, exp, vol, rate, 0)
    elif typ_ == -1:
        r = baw_price_put(und, strike, exp, vol, rate, 0)
    else:
        r = None
    return jsonify(r)


@app.route("/targets", methods=['GET'])
def get_all_targets():
    c = rds.get_hash(db=1, name='consistency', decode=True)
    c_s = pd.Series(c).astype(float).sort_index(ascending=True)
    rm_status = read_dataframe_from_rds(1, 'main_contract_rm_status')
    res = pd.concat([rm_status, c_s.rename('consistency')], axis=1).dropna(subset='consistency').fillna(0)
    return jsonify(res.to_dict(orient='index'))


@app.route("/prev_targets", methods=['GET'])
def get_all_prev_targets():
    c = rds.get_hash(db=1, name='prev_consistency', decode=True)
    c_s = pd.Series(c).astype(float).sort_index(ascending=True)
    return jsonify(c_s.to_dict())


@app.route("/recent_direction_days_cnt", methods=['GET'])
def get_recent_direction_days_cnt():
    c = rds.get_hash(db=1, name='recent_direction_days_cnt', decode=True)
    c_s = pd.Series(c).astype(float).sort_index(ascending=True)
    return jsonify(c_s.to_dict())


@app.route("/opt_mkd_direction", methods=['GET'])
def get_opt_mkd_direction():
    c = rds.get_hash(db=1, name='opt_mkd_sign', decode=True)
    c_s = pd.Series(c).astype(float).sort_index(ascending=True)
    return jsonify(c_s.to_dict())


# @app.route("/direction_indicator", methods=['GET'])
# def get_direction_indicator():
#     c = rds.get_hash(db=1, name='latest_direction_indicator', decode=True)
#     c_s = pd.Series(c).astype(float).sort_index(ascending=True)
#     return jsonify(c_s.to_dict())


@app.route("/option_symbols", methods=['GET'])
def get_option_symbols():
    res = md_assess.option_symbols
    return jsonify(res)


@app.route("/exchange_earliest_expiry", methods=['GET'])
def get_exchange_earliest_expiry():
    res = {}
    current = md_assess.live_source.this_trading_date
    for e in md_assess.exchange_list:
        t = md_assess.base.read_dataframe(
                db_name="processed_option_cn_meta_data",
                tb_name=f"contract_info_{e}",
                filter_datetime={'last_trading_date': {'gte': current.strftime('%Y-%m-%d')}},
                ascending=[('last_trading_date', True)],
                filter_columns=['last_trading_date'],
                filter_row_limit=1
            )
        date_ = t.iloc[0]['last_trading_date']
        und_df = md_assess.base.read_dataframe(
            db_name="processed_option_cn_meta_data",
            tb_name=f"contract_info_{e}",
            filter_datetime={'last_trading_date': {'eq': date_.strftime('%Y-%m-%d')}},
            filter_columns=['underlying_contract'],
        )
        contract_ = und_df['underlying_contract'].drop_duplicates().sort_values(ascending=True).tolist()
        res[e] = {'dt': (date_ - current).days, 'contracts': contract_}
    return jsonify(res)


@app.route("/image/<key>", methods=['GET'])
def get_image_ibconsistency(key):
    model_name = 'IBConsistency'
    model_dt = rds.get_hash(db=1, name='model_dt', k=model_name, decode=True)
    fp_consistency = model_output_file_location('visual_outputs', 'informed_broker_consistency', model_dt, filename=f"{key}.jpeg")
    fp_iv = model_output_file_location('visual_outputs', 'iv_position', filename=f"{key}.jpeg")

    html = f"""
        <html>
        <body>
            <img src="{fp_consistency}" alt="Consistency">
            <img src="{fp_iv}" alt="IV Status">
        </body>
        </html>
        """

    # 发送这个HTML页面
    return html


@app.route("/position_pie", methods=['GET'])
def get_image_pospie():
    model_name = 'TotIBPosStructure'
    model_dt = rds.get_hash(db=1, name='model_dt', k=model_name, decode=True)
    fp_ = model_output_file_location('visual_outputs', 'total_position_structure', model_dt, filename="net_total.jpeg")

    html = f"""
            <html>
            <body>
                <img src="{fp_}" alt="img">
            </body>
            </html>
            """

    # 发送这个HTML页面
    return html


@app.route("/position_table", methods=['GET'])
def get_image_postab():
    model_name = 'TotIBPosStructure'
    model_dt = rds.get_hash(db=1, name='model_dt', k=model_name, decode=True)
    fp_ = model_temp_data_location('visual_outputs', 'total_position_structure', model_dt, filename="position_detail.csv")

    try:
        table_data = pd.read_csv(fp_)
    except FileNotFoundError:
        html_table = None
    else:
        html_table = table_data.to_html(classes='table table-striped')

    # 渲染到 HTML 模板
    return render_template('table_data.html', html_table=html_table)


@app.route("/VIP_position", methods=['GET'])
def get_image_vippos():
    model_name = 'TotIBPosStructure'
    model_dt = rds.get_hash(db=1, name='model_dt', k=model_name, decode=True)
    fp_ = model_temp_data_location('visual_outputs', 'total_position_structure', model_dt, filename="wl_struct.csv")

    try:
        table_data = pd.read_csv(fp_)
    except FileNotFoundError:
        html_table = None
    else:
        html_table = table_data.to_html(classes='table table-striped')

    # 渲染到 HTML 模板
    return render_template('table_data.html', html_table=html_table)


@app.route("/pos_overview", methods=['GET'])
def get_image_ivpos():
    fp_iv = model_output_file_location('visual_outputs', 'iv_position', filename="iv_position_overview.jpeg")
    fp_price = model_output_file_location('visual_outputs', 'price_position', filename="price_position_overview.jpeg")

    html = f"""
            <html>
            <body>
                <img src="{fp_iv}" alt="img">
                <img src="{fp_price}" alt="img">
            </body>
            </html>
            """

    # 发送这个HTML页面
    return html


@app.route("/VIP_ranking", methods=['GET'])
def get_vip_ranking():
    res = read_dataframe_from_rds(1, "vip_ranking")
    return res.to_json(orient="records")


@app.route("/oi_targets", methods=['GET'])
def get_oi_targets():
    res = read_dataframe_from_rds(1, "oi_targets")
    global buff_this_mkt_val_index
    if buff_this_mkt_val_index is not None:
        res = res[res['symbol'].isin(buff_this_mkt_val_index)].copy()
    contracts = res['contract'].tolist()
    symbols = res['symbol'].drop_duplicates().tolist()
    return jsonify(
        {
            'contracts': contracts,
            'symbols': symbols
        }
    )


@app.route("/hist_mkt_val_chg", methods=['GET'])
def get_image_mktvalchgsnap():
    model_name = 'IBConsistency'
    model_dt = rds.get_hash(db=1, name='model_dt', k=model_name, decode=True)
    fp_ = model_temp_data_location('visual_outputs', 'informed_broker_consistency', model_dt, filename="snap_mkt_chg.csv")

    try:
        table_data = pd.read_csv(fp_)
    except FileNotFoundError:
        html_table = None
    else:
        html_table = table_data.to_html(classes='table table-striped')

    # 渲染到 HTML 模板
    return render_template('table_data.html', html_table=html_table)


hist_whole_market_size_buff = None


def get_whole_mkt_size(hist_cnt: int):
    global hist_whole_market_size_buff
    if hist_whole_market_size_buff is None:
        hist_whole_market_size_buff = md_assess.hist_whole_market_size(hist_cnt)
    live_data = md_assess.live_whole_market_size()
    res = hist_whole_market_size_buff.copy()
    res.loc[md_assess.live_source.this_trading_date] = live_data
    return res


@app.route("/hist_avg_iv", methods=['GET'])
def get_hist_iv():
    hist_cnt = 255
    mkt_data = get_whole_mkt_size(hist_cnt)
    prev_n_ls = md_assess.live_source.prev_n_trading_date(hist_cnt)
    start_dt = None if len(prev_n_ls) == 0 else min(prev_n_ls)
    filt_ = None if start_dt is None else {'trading_date': {'gte': start_dt.strftime('%Y-%m-%d')}}

    df = read_dataframe_from_db(
        db_name="pretreated_option_cn_md_data",
        tb_name="all_1d_iv_range_DIY",
        filter_datetime=filt_
    )
    if not df.empty:
        df = df.groupby('trading_date')[['call_avg', 'put_avg']].mean() * 100
        today_option_df = read_dataframe_from_rds(1, "raw_options").reset_index(drop=True)
        today_call = today_option_df[today_option_df['contract'].str.contains('-C-')]['iv'].mean()
        today_put = today_option_df[today_option_df['contract'].str.contains('-P-')]['iv'].mean()
        df.loc[md_assess.live_source.this_trading_date] = {
            'call_avg': today_call,
            'put_avg': today_put,
        }
        df = pd.concat([df, mkt_data], axis=1)
        df['turnover_rate'] = df['turnover'] / df['mkt_cap']
        df = df.reset_index(drop=False, names=['trading_date'])
        df['trading_date'] = df['trading_date'].dt.strftime('%Y-%m-%d')
        df.sort_values(by='trading_date', ascending=True, inplace=True)
    return df.to_json(orient='records')


buff_this_mkt_val_index = None


def threading_this_mkt_val_data():
    with thread_lock:
        res = md_assess.this_mkt_val_chg()
        res_iv = md_assess.iv_pos()
        res = pd.concat([res, res_iv], axis=1).fillna(0)
        global buff_this_mkt_val_index
        buff_this_mkt_val_index = res[res['future_chg'] > 0].index
        return res


@app.route("/this_mkt_val_chg", methods=['GET'])
def get_this_mkt_val_chg_data():
    res = threading_this_mkt_val_data()
    if res is None:
        return
    return res.reset_index(drop=False, names=['symbol']).to_json(orient="records")


# hist_oi_status_df = None
#
#
# def threading_get_oi_high_contracts():
#     with thread_lock:
#         df = md_assess.get_oi_high_contracts()
#         global hist_oi_status_df
#         if hist_oi_status_df is None:
#             hist_oi_status_df = df
#         else:
#             df = df.combine_first(hist_oi_status_df)
#             hist_oi_status_df = df
#         max_month = (datetime.now() + timedelta(days=185)).strftime('%y%m')
#         df = df[[i for i in df.columns if i <= max_month]].copy()
#         df = df[df.applymap(lambda x: str(x).startswith('L-') or str(x).endswith('-S') if isinstance(x, str) else False).any(axis=1)].copy()
#         df = df.sort_index(ascending=True).T.sort_index(ascending=True).reset_index(drop=False, names=['contract'])
#         return df
#
#
# @app.route("/oi_high_targets", methods=['GET'])
# def get_oi_high():
#     df = threading_get_oi_high_contracts()
#     return df.to_json(orient='records')


def threading_get_future_target_table_data():
    rmu_df = read_dataframe_from_rds(1, "raw_bo")
    rmu_df = rmu_df[~rmu_df.index.to_series().str.contains('-')].copy()
    with thread_lock:
        c = rds.get_hash(db=1, name='consistency', decode=True)
        c_s = pd.Series(c).astype(float).sort_index(ascending=True)
        t, m = md_assess.analyse_future_targets(c_s)
        spot_s, spot_t = md_assess.get_current_standard_spot()
        if spot_s is None:
            t['spot'] = np.nan
        else:
            t['spot'] = [spot_s.loc[i] if i in spot_s.index else np.nan for i in t['symbol']]
        return {
            'tick': t.sort_values(by=['exchange', 'symbol'], ascending=[False, True]).to_json(orient='records'),
            'main_list': m,
            'rmu': rmu_df.to_json(orient='index'),
            'spot_dt': spot_t
        }


@app.route("/future_targets", methods=['GET'])
def get_future_target_table():
    data = threading_get_future_target_table_data()
    return jsonify(data)


def threading_get_current_price_position():
    with thread_lock:
        df = md_assess.price_positions()
        return df


def threading_get_current_extreme_price_cnt():
    with thread_lock:
        s = md_assess.get_extreme_price_cnt_data().sort_index(ascending=True).iloc[-1]
        return s


def threading_get_current_extreme_position_cnt():
    with thread_lock:
        s = md_assess.get_extreme_position_cnt_data().sort_index(ascending=True).iloc[-1]
        return s


@app.route("/oi_increasing_symbols", methods=['GET'])
def get_oi_inc_symbols():
    res = threading_get_current_extreme_position_cnt()
    rls = res[res > 0].index.tolist()
    return jsonify(rls)


@app.route("/price_positions", methods=['GET'])
def get_current_price_position():
    df = threading_get_current_price_position()
    ex_price_s = threading_get_current_extreme_price_cnt()
    ex_pos_s = threading_get_current_extreme_position_cnt()
    df['ex_price_cnt'] = ex_price_s.loc[ex_price_s.index.intersection(df.index)].reindex(df.index)
    df['ex_position_cnt'] = ex_pos_s.loc[ex_pos_s.index.intersection(df.index)].reindex(df.index)
    df.sort_values(by=['ex_price_cnt', 'range'], ascending=[False, True], inplace=True)
    df.reset_index(drop=False, names=['symbol'], inplace=True)
    return df.to_json(orient="records")


@app.route("/sym_targets/<key>", methods=['GET'])
def get_sym_targets(key):
    df = read_dataframe_from_rds(1, "raw_options").reset_index(drop=True)
    df = df[df['symbol'] == key.upper()].copy().drop(
        columns=['symbol']
    ).assign(
        strike=df['contract'].apply(lambda x: float(x.split('-')[-1])),
        direc=df['contract'].apply(lambda x: x.split('-')[1]),
    ).sort_values(
        by=['exchange', 'underlying', 'direc', 'strike'], ascending=[False, True, True, False]
    ).drop(columns=['strike', 'direc'])
    # cat rm
    df['contract'] = df['contract'] + '$' + df['rm1'].astype(str) + '#' + df['rm2'].astype(str) + '#' + df[
        'rmu1'].astype(str) + '#' + df['rmu2'].astype(str)
    df.drop(columns=['rm1', 'rm2', 'rmu1', 'rmu2'], inplace=True)
    return df.to_json(orient="records")


@app.route("/live_targets", methods=['GET'])
def get_live_table():
    df = read_dataframe_from_rds(1, "option_targets").reset_index(drop=False).drop(
        columns=['symbol']
    ).sort_values(
        by=['exchange', 'underlying', 'contract'], ascending=[False, True, True]
    )
    # cat rm
    df['contract'] = df['contract'] + '$' + df['rm1'].astype(str) + '#' + df['rm2'].astype(str) + '#' + df[
        'rmu1'].astype(str) + '#' + df['rmu2'].astype(str)
    df.drop(columns=['rm1', 'rm2', 'rmu1', 'rmu2'], inplace=True)
    return df.to_json(orient="records")


@app.route("/quest_targets", methods=['POST'])
def get_quest_table():
    contract = [i.strip().upper() for i in request.json.get('quest', "").split(' ') if i.strip()]
    df = read_dataframe_from_rds(1, "raw_options").reset_index(drop=True)
    df = df[df['contract'].isin(list(set(contract)))].copy().drop(
        columns=['symbol']
    ).sort_values(
        by=['exchange', 'underlying', 'contract'], ascending=[False, True, True]
    )
    # cat rm
    df['contract'] = df['contract'] + '$' + df['rm1'].astype(str) + '#' + df['rm2'].astype(str) + '#' + df[
        'rmu1'].astype(str) + '#' + df['rmu2'].astype(str)
    df.drop(columns=['rm1', 'rm2', 'rmu1', 'rmu2'], inplace=True)
    return df.to_json(orient="records")


@app.route("/filter_options", methods=['POST'])
def filter_raw_options():
    optType = request.json.get('optType')
    oiStat = request.json.get('oiStat')
    mdStat = request.json.get('mdStat')
    premStat = request.json.get('premStat')
    undLsStat = request.json.get('undLsStat')
    optLsStat = request.json.get('optLsStat')
    undMvStat = request.json.get('undMvStat')
    optMvStat = request.json.get('optMvStat')
    pDelta = request.json.get('pDelta')
    mktVal = request.json.get('mktVal')
    ratioL = request.json.get('ratioL')
    ratioH = request.json.get('ratioH')
    expL = request.json.get('expL')
    expH = request.json.get('expH')
    movUndLinkStat = request.json.get('movUndLinkStat')
    UndLsLinkStat = request.json.get('UndLsLinkStat')

    df = read_dataframe_from_rds(1, "raw_options").reset_index(drop=True)
    if premStat == 0:
        pass
    else:
        premFilt = df['premium'] > 0 if premStat > 0 else df['premium'] < 0
        df = df[premFilt].copy()
    if undLsStat == 0:
        pass
    else:
        undLsFilt = df['UND_L/S'] > 0 if undLsStat > 0 else df['UND_L/S'] < 0
        df = df[undLsFilt].copy()
    if optLsStat == 0:
        pass
    else:
        optLsFilt = df['OPT_L/S'] > 0 if optLsStat > 0 else df['OPT_L/S'] < 0
        df = df[optLsFilt].copy()
    if undMvStat == 0:
        pass
    else:
        undMvFilt = df['MOV_UND'] > 0 if undMvStat > 0 else df['MOV_UND'] < 0
        df = df[undMvFilt].copy()
    if optMvStat == 0:
        pass
    else:
        optMvFilt = df['MOV_OPT'] > 0 if optMvStat > 0 else df['MOV_OPT'] < 0
        df = df[optMvFilt].copy()

    df['direc'] = df['contract'].str.split('-').str.get(1)
    if optType == 0:
        pass
    else:
        optTypFilt = df['direc'] == 'C' if optType > 0 else df['direc'] == 'P'
        df = df[optTypFilt].copy()

    if movUndLinkStat == 0:
        pass
    else:
        movUndLinkFilter = ((df['direc'] == 'C') & (df['MOV_UND'] > 0)) | ((df['direc'] == 'P') & (df['MOV_UND'] < 0))
        df = df[movUndLinkFilter].copy()

    if UndLsLinkStat == 0:
        pass
    else:
        UndLsLinkFilter = ((df['direc'] == 'C') & (df['UND_L/S'] > 0)) | ((df['direc'] == 'P') & (df['UND_L/S'] < 0))
        df = df[UndLsLinkFilter].copy()

    df.drop(columns=['direc'], inplace=True)

    if oiStat == 0:
        pass
    else:
        df = df.query("(oix.str.split('/').str[0].astype('float') <= 0.1) and (oix.str.split('/').str[1].astype('float') >= 0.9)", engine='python').copy()

    if mdStat == 0:
        pass
    elif mdStat > 0:
        df = df.query("(mdx.str.split('/').str[0].astype('float') < 0.5) and (mdx.str.split('/').str[1].astype('float') >= 0.5)", engine='python').copy()
    else:
        df = df.query("(mdx.str.split('/').str[0].astype('float') >= 0.5) and (mdx.str.split('/').str[1].astype('float') < 0.5)", engine='python').copy()

    df.rename(columns={'hpp/delta': 'hdxxx'}, inplace=True)
    df = df.query(f"(hdxxx.str.split('/').str[1].astype('float') < {pDelta})", engine='python').copy()
    df.rename(columns={'hdxxx': 'hpp/delta'}, inplace=True)

    df = df[df['Mkt.Value'] >= mktVal].copy()
    df = df[(df['RATIO'] >= ratioL) & (df['RATIO'] <= ratioH)].copy()
    df = df[(df['expiry'] >= expL) & (df['expiry'] <= expH)].copy()

    df = df.drop(columns=['symbol']).assign(
        strike=df['contract'].apply(lambda x: float(x.split('-')[-1])),
        direc=df['contract'].apply(lambda x: x.split('-')[1]),
    ).sort_values(
        by=['exchange', 'underlying', 'direc', 'strike'], ascending=[False, True, True, False]
    ).drop(columns=['strike', 'direc'])
    # cat rm
    df['contract'] = df['contract'] + '$' + df['rm1'].astype(str) + '#' + df['rm2'].astype(str) + '#' + df[
        'rmu1'].astype(str) + '#' + df['rmu2'].astype(str)
    df.drop(columns=['rm1', 'rm2', 'rmu1', 'rmu2'], inplace=True)
    return df.to_json(orient="records")


@app.route("/target-watch-list")
def init_watch_list_page():
    return render_template("watchList.html")


@app.route("/target-watch-list/subscribe_target", methods=['POST'])
def calc_watch_list_range():
    contract = request.json.get('contract')
    c_low = request.json.get('costRangeLow')
    c_high = request.json.get('costRangeHigh')
    aim = request.json.get('aim')
    vol = request.json.get('vol')
    exp = request.json.get('exp')
    rate = request.json.get('rate')

    if isinstance(contract, str) and '-' in contract:
        contract = contract.upper()
        direction = contract.split('-')[1]
        f = baw_price_call if direction == 'C' else baw_price_put
        strike = float(contract.split('-')[-1])
        opt_low = f(c_low, strike, exp, vol, rate, 0)
        opt_high = f(c_high, strike, exp, vol, rate, 0)
        aim_price = f(aim, strike, exp, vol, rate, 0)
        return jsonify(
            {
                'optLow': opt_low,
                'optHigh': opt_high,
                'optAim': aim_price
            }
        )
    else:
        return jsonify(None)


# @app.route("/extreme_price_cnt", methods=['GET'])
# def get_extreme_price_cnt():
#     res = md_assess.get_extreme_price_cnt_data()
#     data = res.to_json(orient='split')
#     return data
#
#
# @app.route("/extreme_position_cnt", methods=['GET'])
# def get_extreme_position_cnt():
#     res = md_assess.get_extreme_position_cnt_data()
#     data = res.to_json(orient='split')
#     return data


# def threading_get_oi_neg_chg():
#     with thread_lock:
#         r = oiVol.read_dataframe_from_rds('oi_neg_chg_rank')
#         max_month = (datetime.now() + timedelta(days=185)).strftime('%y%m')
#         min_month = datetime.now().strftime('%y%m')     # 因为进不了交割月，所以这里就简单粗暴处理一下
#         r = r[[i for i in r.columns if (((i <= max_month) & (i > min_month)) | (i == 'oi_chg_pctg_by_symbol'))]].copy()
#         return r


# @app.route("/oi_chg_pctg_rank", methods=['GET'])
# def get_oi_neg_chg():
#     r = threading_get_oi_neg_chg()
#     r = r.reset_index(drop=False, names=['symbol']).rename(
#         columns={'oi_chg_pctg_by_symbol': 'tot_oi_chg_pctg'}
#     )
#     r = r.loc[:, ['symbol', 'tot_oi_chg_pctg'] + sorted([col for col in r.columns if col not in ['symbol', 'tot_oi_chg_pctg']])]
#     return r.to_json(orient='records')


# def make_tuple(x):
#     return (x['t_delta'], x['volume_quantile'])


# def threading_get_unusual_volume():
#     with thread_lock:
#         r = oiVol.read_dataframe_from_rds('unusual_volume')
#         r['contract_month'] = r['contract'].str[-4:]
#         max_month = (datetime.now() + timedelta(days=185)).strftime('%y%m')
#         min_month = datetime.now().strftime('%y%m')  # 因为进不了交割月，所以这里就简单粗暴处理一下
#         r = r[(r['contract_month'] > min_month) & (r['contract_month'] <= max_month)].copy()
#         r['tuple_values'] = r.apply(make_tuple, axis=1)
#         res_df = pd.pivot_table(r, columns='contract_month', index='symbol', values='tuple_values', aggfunc='first')
#         return res_df


# @app.route("/unusual_volume", methods=['GET'])
# def get_unusual_vol():
#     r = threading_get_unusual_volume()
#     r = r.reset_index(drop=False, names=['symbol'])
#     return r.to_json(orient='records')


def threading_get_unusual_option_whales():
    with thread_lock:
        r = oiVol.read_dataframe_from_rds('unusual_option_whales')
        return r


@app.route("/unusual_option_whales", methods=['GET'])
def get_unusual_option_whales():
    r = threading_get_unusual_option_whales()
    r = r.sort_values(by=['oi_cap_chg'], ascending=False)
    return r.to_json(orient='records')


def start_rest_server():
    ip = get_local_ip()
    logger.info(f"Server Running @ {ip}...")
    server = WSGIServer(
        ("0.0.0.0", 15167), app
    )
    server.serve_forever()


if __name__ == '__main__':
    start_rest_server()
