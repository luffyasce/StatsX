from gevent import monkey
monkey.patch_all()
import json
import signal
import threading
from flask import Flask, request
from flask_cors import CORS
from gevent.pywsgi import WSGIServer
from infra.trade.api.ctp_trade import *
from infra.tool.rules import TradeRules
from utils.tool.encodes import JsonEncoder
from utils.tool.logger import log
from utils.tool.configer import Config


config = Config()
trade_conf = config.get_trade_conf

logger = log(__file__, "infra", warning_only=False)

app = Flask(__name__)
CORS(app)

trade_rules = TradeRules()


@app.route("/query_position", methods=['POST'])
def query_position():
    contract = request.form.get('contract')
    req_id = trader.query_position(contract)
    while not trader.trade_spi.req_position_record.get(req_id, False):
        sleep(0.01)
    return json.dumps(trader.trade_spi.req_position_detail, ensure_ascii=False, cls=JsonEncoder)


@app.route("/query_account", methods=['GET'])
def query_account():
    req_id = trader.query_account()
    while req_id != trader.trade_spi.account_detail.get("nRequestID", 0):
        sleep(0.01)
    return json.dumps(trader.trade_spi.account_detail, ensure_ascii=False, cls=JsonEncoder)


@app.route("/buy_open", methods=['POST'])
def buy_open():
    order_ref = trader.buy_open(**request.form.to_dict())
    return order_ref


@app.route("/sell_open", methods=['POST'])
def sell_open():
    order_ref = trader.sell_open(**request.form.to_dict())
    return order_ref


@app.route("/buy_close", methods=['POST'])
def buy_close():
    order_ref = trader.buy_close(**request.form.to_dict())
    return order_ref


@app.route("/sell_close", methods=['POST'])
def sell_close():
    order_ref = trader.sell_close(**request.form.to_dict())
    return order_ref


@app.route("/revoke_order", methods=['POST'])
def revoke_order():
    order_ref = trader.withdraw_order(**request.form.to_dict())
    return order_ref


@app.route("/get_order_detail", methods=['POST'])
def get_order_detail():
    order_ref = request.form.get('order_ref')
    detail = trader.trade_spi.order_rtn_detail.get(order_ref, {})
    return json.dumps(detail, ensure_ascii=False, cls=JsonEncoder)


class Server(threading.Thread):
    def __init__(self, broker: str):
        super().__init__()
        self.broker = broker

    def run(self):
        global trader, server
        trader = CtpTrade(
            front_addr=trade_conf.get(self.broker, "trade_front_addr"),
            broker_id=trade_conf.get(self.broker, "broker_id"),
            investor_id=trade_conf.get(self.broker, "investor_id"),
            pwd=trade_conf.get(self.broker, "pwd"),
            app_id=trade_conf.get(self.broker, "app_id"),
            auth_code=trade_conf.get(self.broker, "auth_code"),
            user_product_info=trade_conf.get(self.broker, "product_info"),
            mode=trade_conf.getint(self.broker, "login_mode"),
        )
        with trader:
            server = WSGIServer(
                (trade_conf.get(self.broker, "local_server_addr"), trade_conf.getint(self.broker, "local_server_port")),
                app
            )
            server.serve_forever()


def shutdown():
    server.stop()
    server.close()


def start_trade_server(broker: str):
    global trader, server
    server = None
    Server(broker).start()
    while server is None:
        sleep(5)
    else:
        shutdown()


if __name__ == "__main__":
    start_trade_server('SIM')
