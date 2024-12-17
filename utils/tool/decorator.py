import os
from datetime import datetime
import time
import random
from functools import wraps
import traceback
import sys
import tracemalloc
import pandas as pd
import dill
import math
import inspect
import cProfile
import pstats
from utils.tool.logger import log
from utils.tool.configer import Config
from utils.tool.hash import Hash
from utils.tool.beep import Beep
from utils.tool.datetime_wrangle import check_weekdays
from apscheduler.schedulers.background import BlockingScheduler
from utils.custom.exception.errors import DynamicCodingError
dill.settings['recurse'] = True

logger = log(__file__, "utils")

configer = Config()


def only_on_weekdays():
    def wrapper(func):
        @wraps(func)
        def f(*args, **kwargs):
            if check_weekdays(datetime.now()):
                ret = func(*args, **kwargs)
            else:
                ret = None
            return ret
        return f
    return wrapper


def trace_mem(logfile: str = __file__):
    """
    To track memory usage during decorated function.
    :param logfile:
    :return:
    """
    def wrapper(func):
        @wraps(func)
        def tracing(*args, **kwargs):
            logger = log(logfile, 'memory')
            tracemalloc.start()
            current_, peak_ = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            logger.info(f"Before function memory usage {current_/1e6}MB; Peak: {peak_/1e6}MB")
            tracemalloc.start()
            res = func(*args, **kwargs)
            current_, peak_ = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            logger.info(f"After function memory usage {current_ / 1e6}MB; Peak: {peak_ / 1e6}MB")
            return res
        return tracing
    return wrapper


# def divide_conquer(ahead: int = 1, pace: int = 100_000, core_num: int = 4, need_return: bool = True):
#     """
#     A multiprocessing function for large data handling.
#     If you need to use this decorator,
#     make sure that your function attributes comes first with the (only) source dataframe,
#     and the other params follow.
#     :param ahead: the amount of record that you want to fallback on your loc start
#     :param pace:
#     :param core_num:
#     :param need_return:
#     :return:
#     """
#     if ahead > pace:
#         raise ValueError(f"ahead: {ahead} must not be greater than pace: {pace}")
#
#     def divide_wrap(func):
#         @wraps(func)
#         def dc(*args):
#             if len([i for i in args if isinstance(i, pd.DataFrame)]) != 1 or not isinstance(args[0], pd.DataFrame):
#                 raise AttributeError("There should be only one source dataframe in this function "
#                                      "and the dataframe should always be the first positional argument")
#             other_args = args[1:]
#             df = list(args).pop(0)
#             mem_size = sys.getsizeof(df) / 1e6
#             if mem_size <= 1000:
#                 res = func(*(df, *other_args))
#                 return res
#             else:
#                 chunks = math.ceil(len(df) / pace)
#                 res = pd.Series(dtype=float)
#
#                 def new_func(*args_):
#                     # I only define this function to avoid pickle error on windows.
#                     # If you are using linux or unix-like systems with fork, you don't actually need to do this.
#                     r_ = func(*args_)
#                     r_ = r_.to_dict()
#                     return r_
#                 # initiating process pool
#                 pool = pathos.multiprocessing.ProcessPool(ncpus=core_num)
#                 dls = []
#                 # chunking dataframe and apply them to multiprocessing pool
#                 for i in range(chunks):
#                     start_ = i * pace - ahead if i != 0 else i * pace
#                     end_ = (i + 1) * pace
#                     if end_ <= len(df):
#                         df_i = df.loc[df.iloc[start_: end_].index]
#                     else:
#                         df_i = df.loc[df.iloc[start_:].index]
#                     dls.append(df_i)
#                 prms = [[i] * len(dls) for i in other_args]
#                 prms = [tuple(ii) for ii in prms]
#                 res_ = pool.amap(new_func, tuple(dls), *prms)
#                 while not res_.ready():
#                     time.sleep(0.1)
#                 res_ls = res_.get()
#                 # after getting all results, since the results are orderless,
#                 # if there are overlaps in between data chunks,
#                 # we need to sort them to make sure the final result is correct.
#                 if need_return:
#                     if ahead == 0:
#                         for res_i in res_ls:
#                             res_i = res_i.loc[res_i[~res_i.index.isin(res.index)].index]
#                             res = res.append(res_i)
#                     else:
#                         temp_df = pd.DataFrame(res_ls, index=[pd.Series(r).index.min() for r in res_ls])
#                         temp_df = temp_df.sort_index(ascending=True)
#                         # concatenate final result
#                         for _, v in temp_df.iterrows():
#                             v = v.sort_index(ascending=True).dropna()
#                             v = v.loc[v[~v.index.isin(res.index)].index]
#                             res = res.append(v)
#                     return res.sort_index(ascending=True)
#                 else:
#                     return
#         return dc
#     return divide_wrap


def sched_job(scheduler: BlockingScheduler, logger: log = None, **sched_kwargs):
    def func_wrapper(func):
        if logger is not None:
            logger.info(f"Register cron job for {func.__name__} with params: {sched_kwargs}")

        @wraps(func)
        def __function(*args, **kwargs):
            scheduler.add_job(func, **sched_kwargs, args=args, kwargs=kwargs)
        return __function
    return func_wrapper


def private(
        db_ctl
):
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):
            db_ctl._switch_client()
            res = func(*args, **kwargs)
            db_ctl._restore_client()
            return res
        return __function
    return func_wrapper


def try_catch(
        err_msg: str = "",
        suppress_traceback: bool = True,
        catch_args: bool = False,
        enable_alert: bool = False,
):
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
            except Exception as err:
                msg_ = f"{func.__name__} ERR: {err_msg} {err.__class__.__name__} {err.args[0] if len(err.args) != 0 else ''}"
                msg_ = msg_ if suppress_traceback else msg_ + f"\ndetails: \n{str(traceback.format_exc())}"
                if catch_args:
                    msg_ += f"\n{' / '.join([str(i) for i in args])} " \
                            f"{' / '.join([f'{k}:{str(v)}' for k, v in kwargs.items()])}"
                logger.error(msg_)
                if enable_alert:
                    Beep.warning()
                return None
            else:
                return res
        return __function
    return func_wrapper


def auto_retry(
        show_err: bool = False,
        throw_err: bool = True,
        max_retries: int = 1,
        random_wait: bool = True,
):
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):
            max_t = 0
            while True:
                try:
                    res = func(*args, **kwargs)
                except Exception as err:
                    max_t += 1
                    msg_ = f"\ndetails: \n{str(traceback.format_exc())}"
                    if max_t <= max_retries:
                        time.sleep(5)
                        logger.warning(f"{err=}")
                        if show_err:
                            logger.error(msg_)
                        if random_wait:
                            time.sleep(round(random.random()))
                        continue
                    else:
                        if throw_err:
                            raise Exception(msg_)
                        else:
                            break
                else:
                    return res
        return __function
    return func_wrapper


def goto(indent: int = 4):
    """
    Use this with caution, make sure that keyword provided will not replace elsewhere except from postfix and prefix
    :param indent:
    :return:
    """
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):

            def kw_replace(source: str):
                conf = configer.get_conf
                goto_db_prefix = conf.get("Databases", "app_db_prefix")
                goto_tb_postfix = conf.get("Databases", "app_tb_postfix")
                if not isinstance(source, str):
                    return source
                else:
                    replace_ls = ['processed', 'pretreated']
                    emmit_ls = [
                        'THS', 'RQ', 'BS', 'JQ', 'DIY', 'WIND', 'QHKC', 'SSE', 'SZSE', 'HKEX',
                        'INE', 'SHFE', 'DCE', 'CZCE', 'CFFEX',
                        'SINA', 'SNOWBALL', 'EASTMONEY'
                    ]
                    repl_dict = {i: goto_db_prefix for i in replace_ls} if goto_db_prefix is not None else {}
                    emt_dict = {i: goto_tb_postfix for i in emmit_ls} if goto_tb_postfix is not None else {}
                    repl_dict = {**repl_dict, **emt_dict}
                    if goto_db_prefix is None and goto_tb_postfix is None:
                        logger.info(
                            f"Decorator: goto will not replace any postfix / prefix since params are not given."
                        )
                    else:
                        logger.info(
                            f"Decorator: goto will replace {replace_ls} with {goto_db_prefix},"
                            f"and {emmit_ls} with {goto_tb_postfix}."
                        )
                    for k, v in repl_dict.items():
                        source = source.replace(k, v)
                    return source

            res = func(*args, **kwargs)
            source_ = inspect.getsource(func)
            source_ = '\n'.join(map(lambda x: x[indent:], source_.split('\n')[1:]))
            source_ = kw_replace(source_)
            source_ = '\n'.join([("def new_func(" + source_.split('\n')[0].split('(')[1]), *source_.split('\n')[1:]])
            try:
                exec(source_, globals())
                nargs = [kw_replace(i) for i in args]
                nkwargs = {k: kw_replace(v) for k, v in kwargs.items()}
                res_2 = new_func(*nargs, **nkwargs)
            except Exception as err:
                res_2 = None
                raise DynamicCodingError(f"{str(traceback.format_exc())}")
            else:
                pass
            finally:
                return res, res_2
        return __function
    return func_wrapper


def profiling(
        profile_label: str,
        read_existing_report: bool,
        print_row_nums: int = 10,
        sort_by_cum_t: bool = True,
):
    def func_wrapper(func):
        @wraps(func)
        def __function(*args, **kwargs):
            pth = configer.path
            ffp = os.path.join(pth, 'docs', f"profile_{profile_label}.prof")
            sorting = pstats.SortKey.CUMULATIVE if sort_by_cum_t else pstats.SortKey.TIME

            def examine():
                prof = cProfile.Profile()
                prof.enable()
                r = func(*args, **kwargs)
                prof.disable()
                p_ = pstats.Stats(prof).sort_stats(sorting)
                p_.print_stats(print_row_nums)
                prof.dump_stats(ffp)
                return r
            if not read_existing_report:
                res = examine()
            else:
                try:
                    pstats.Stats(ffp).sort_stats(sorting).print_stats(print_row_nums)
                except FileNotFoundError:
                    res = examine()
                else:
                    res = 0
            return res
        return __function
    return func_wrapper


def singleton(cls):
    _instance = {}

    def _wrapper(*args, **kwargs):
        name_hash = cls.__name__ + "|" + Hash.all_params_encode(*args, **kwargs)
        if name_hash not in _instance:
            _instance[name_hash] = cls(*args, **kwargs)
        return _instance[name_hash]
    return _wrapper
