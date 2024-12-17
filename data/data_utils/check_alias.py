from utils.database.unified_db_control import UnifiedControl

udc = UnifiedControl(db_type='base')


def check_symbol_alias(symbol: str):
    symbol_alias_dict = udc.read_dataframe('processed_future_cn_meta_data', 'pre_symbol_info_DIY')
    symbol_alias_dict = dict(zip(symbol_alias_dict['pre_symbol'], symbol_alias_dict['current_symbol']))
    flag = symbol
    while True:
        flag = symbol_alias_dict.get(flag, None)
        if flag is None:
            break
        else:
            symbol = flag
    return symbol


