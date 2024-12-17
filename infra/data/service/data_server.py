import json
from flask import Flask, request
from flask_cors import CORS
from gevent.pywsgi import WSGIServer
from utils.tool.encodes import JsonEncoder
from utils.tool.logger import log
from utils.tool.configer import Config
import infra.data.api.init_data_api as root_api

config = Config()
conf = config.get_conf

logger = log(__name__, "infra")

app = Flask(__name__)
CORS(app)


@app.route("/", methods=['POST'])
def query():
    form_ = json.loads(request.json, cls=json.JSONDecoder)
    api_name = form_.pop("api")
    func = form_.pop("function")
    try:
        args = form_.pop("misc")
    except KeyError:
        args = []
    else:
        pass
    with eval(f"root_api.{api_name}()") as api:
        res = eval(f"api.{func}")(*args, **form_)
    return json.dumps({"data": res}, ensure_ascii=False, cls=JsonEncoder)


def start_data_server():
    server = WSGIServer(
        (conf.get("DataServer", "address"), conf.getint("DataServer", "port")), app
    )
    server.serve_forever()

