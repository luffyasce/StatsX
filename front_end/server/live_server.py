import asyncio
import websockets
import json
import pandas as pd
from datetime import datetime
from utils.tool.configer import Config
from utils.database.unified_db_control import UnifiedControl
from model.live.live_data_source.source import MDO
from utils.buffer.redis_handle import Redis

udc = UnifiedControl('base')
conf = Config()
live_source = MDO()
rds = Redis()


def main_contract_dict():
    res_df = pd.DataFrame()
    for e in conf.exchange_list:
        tdf = udc.read_dataframe(
            "processed_future_cn_roll_data",
            f"all_main_{e}",
            ascending=[('trading_date', False)],
            filter_row_limit=1
        )
        t_date = tdf.iloc[0]['trading_date']
        xl = udc.read_dataframe(
            "processed_future_cn_roll_data",
            f"all_main_{e}",
            filter_datetime={'trading_date': {'eq': t_date.strftime('%Y-%m-%d')}},
            filter_columns=['symbol', 'O_NM_N']
        )
        res_df = pd.concat([res_df, xl], axis=0)
    res = res_df.set_index('symbol')['O_NM_N'].to_dict()
    return res


main_contract_dict_buff = main_contract_dict()


def get_top_symbol_main_md_data():
        ss = rds.get_key(db=1, k='top_symbols', decode=True)
        symbol_list = ss.split('-')
        contract_list = [main_contract_dict_buff[s] for s in symbol_list]
        res_pack = {}
        for contract in contract_list:
            live_md = live_source.get_1min_md_data_by_contract(contract)
            if not live_md.empty:
                live_md['datetime_minute'] = live_md['datetime_minute'].dt.strftime('%m%d %H:%M')
            res_pack[contract] = live_md.to_json(orient='records')
        return res_pack


async def handler(websocket, path):
    try:
        async for msg in websocket:
            print(f"WS HEARTBEAT @ {datetime.now()}: {msg}")
            data = get_top_symbol_main_md_data()
            await websocket.send(json.dumps(data))
    except websockets.ConnectionClosedOK:
        print("WS CONNECTION CLOSED")


async def start_server():
    return await websockets.serve(handler, '0.0.0.0', 15169)


async def main():
    server = await start_server()
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
