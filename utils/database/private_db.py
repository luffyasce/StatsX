"""
Because UnifiedControl is a singleton class, and private db mode cannot be detected.
One can only access private db mode through here or define your own private functions with @private decorator,
which can ensure that there is only one 'base' singleton.
"""
from utils.database.unified_db_control import UnifiedControl
from utils.tool.decorator import private


_base = UnifiedControl(db_type='base')


@private(db_ctl=_base)
def read_private(*args, **kwargs):
    return _base.read_dataframe(*args, **kwargs)


@private(db_ctl=_base)
def write_private(*args, **kwargs):
    return _base.insert_dataframe(*args, **kwargs)


@private(db_ctl=_base)
def del_private_row(*args, **kwargs):
    return _base.del_row(*args, **kwargs)


@private(db_ctl=_base)
def get_table_names(*args, **kwargs):
    return _base.get_table_names(*args, **kwargs)
