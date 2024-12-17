import os
import ctypes
from ctypes import *
from utils.tool.configer import Config
from utils.tool.decorator import try_catch

__path__ = Config().path

__opt_calc_dll__ = CDLL(os.path.join(__path__, os.sep.join(["infra", "math", "lib", "OptCalc.dll"])))
__opt_calc_dll__.bsd.restype = ctypes.c_double
__opt_calc_dll__.kc.restype = ctypes.c_double
__opt_calc_dll__.kp.restype = ctypes.c_double
__opt_calc_dll__.bsPriceCall.restype = ctypes.c_double
__opt_calc_dll__.bsPricePut.restype = ctypes.c_double
__opt_calc_dll__.bawPriceCall.restype = ctypes.c_double
__opt_calc_dll__.bawPricePut.restype = ctypes.c_double
__opt_calc_dll__.bsIVCall.restype = ctypes.c_double
__opt_calc_dll__.bsIVPut.restype = ctypes.c_double
__opt_calc_dll__.bawIVCall.restype = ctypes.c_double
__opt_calc_dll__.bawIVPut.restype = ctypes.c_double
__opt_calc_dll__.bsDeltaCall.restype = ctypes.c_double
__opt_calc_dll__.bsDeltaPut.restype = ctypes.c_double
__opt_calc_dll__.bsGamma.restype = ctypes.c_double
__opt_calc_dll__.bsVega.restype = ctypes.c_double
__opt_calc_dll__.bsThetaCall.restype = ctypes.c_double
__opt_calc_dll__.bsThetaPut.restype = ctypes.c_double


@try_catch(catch_args=True)
def bs_price_call(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsPriceCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_price_put(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsPricePut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def baw_price_call(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bawPriceCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def baw_price_put(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bawPricePut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_iv_call(spot_price, strike_price, tte, risk_free_rate, option_price, dividend):
    return __opt_calc_dll__.bsIVCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, risk_free_rate, option_price, dividend]]
    )


@try_catch(catch_args=True)
def bs_iv_put(spot_price, strike_price, tte, risk_free_rate, option_price, dividend):
    return __opt_calc_dll__.bsIVPut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, risk_free_rate, option_price, dividend]]
    )


@try_catch(catch_args=True)
def baw_iv_call(spot_price, strike_price, tte, risk_free_rate, option_price, dividend):
    return __opt_calc_dll__.bawIVCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, risk_free_rate, option_price, dividend]]
    )


@try_catch(catch_args=True)
def baw_iv_put(spot_price, strike_price, tte, risk_free_rate, option_price, dividend):
    return __opt_calc_dll__.bawIVPut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, risk_free_rate, option_price, dividend]]
    )


@try_catch(catch_args=True)
def bs_delta_call(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsDeltaCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_delta_put(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsDeltaPut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_gamma(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsGamma(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_vega(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsVega(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_theta_call(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsThetaCall(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


@try_catch(catch_args=True)
def bs_theta_put(spot_price, strike_price, tte, vol, risk_free_rate, dividend):
    return __opt_calc_dll__.bsThetaPut(
        *[ctypes.c_double(x) for x in [spot_price, strike_price, tte, vol, risk_free_rate, dividend]]
    )


if __name__ == "__main__":
    # test
    res = baw_price_put(10000, 20, 20, 0.15, 0.02, 0)
    print(res)