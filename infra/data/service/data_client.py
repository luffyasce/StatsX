import json
import pandas as pd
import requests
from utils.tool.logger import log
from utils.tool.decorator import auto_retry

logger = log(__name__, "infra")


class DataClient:
    def __init__(self, host: str, port: str):
        self.url = f"{host}:{port}" if 'http' in host else f"http://{host}:{port}"

    @auto_retry()
    def query_data(self, data_api_class_name: str, func: str, *args, **kwargs):
        req_data = {
            "api": data_api_class_name,
            "function": func,
            "misc": args,
            **kwargs
        }
        resp_ = requests.post(
            f"{self.url}/",
            json=json.dumps(req_data, cls=json.JSONEncoder, ensure_ascii=False)
        )
        if resp_.status_code == 200:
            try:
                res = json.loads(json.loads(resp_.content, cls=json.JSONDecoder).get("data"), cls=json.JSONDecoder)
            except Exception:
                res = json.loads(resp_.content, cls=json.JSONDecoder).get("data")
            else:
                pass
            return res
        else:
            logger.error(
                f"query data err: {resp_.status_code} {data_api_class_name}.{func}"
            )

    @staticmethod
    def json_convert(data: json):
        return pd.DataFrame().from_dict(data)


if __name__ == "__main__":
    d = DataClient("localhost", "6543")
    r = d.query_data("RQ", "get_price", "CU2203", "2022-03-01")
    print(d.json_convert(r))