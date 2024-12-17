import pandas as pd
import numpy as np
from typing import Union
from pyecharts.charts import Line, Bar, Kline, Grid, Scatter, Page, Scatter3D
from pyecharts.commons.utils import JsCode
from pyecharts import options as opts

area_color_js = (
    "new echarts.graphic.LinearGradient(0, 0, 0, 1, "
    "[{offset: 0, color: '#eb64fb'}, {offset: 1, color: '#3fbbff0d'}], false)"
)


def draw_double_yaxis_lines(x: list, y_1: list, y_2: list, fp: Union[None, str] = None):
    line = (
        Line()
        .add_xaxis(x)
        .extend_axis(yaxis=opts.AxisOpts(is_show=True, is_scale=True))
        .add_yaxis('y_1', y_1)
        .add_yaxis('y_2', y_2, yaxis_index=1)
        .set_global_opts(
            yaxis_opts=opts.AxisOpts(is_scale=True),
            xaxis_opts=opts.AxisOpts(is_scale=True),
            title_opts=opts.TitleOpts(title="Double Lines"),
            datazoom_opts=[
                opts.DataZoomOpts(range_start=95, range_end=100),
                opts.DataZoomOpts(is_show=False, type_='inside', range_start=95, range_end=100)
            ],
            toolbox_opts=opts.ToolboxOpts(),
            tooltip_opts=opts.TooltipOpts(
                is_show=True,
                trigger='axis',
                axis_pointer_type='cross',
            )
        )
        .set_series_opts(
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    line.width = "1800PX"
    line.height = "600PX"
    if fp is not None:
        line.render(fp)
    else:
        return line


def draw_kline_with_yield_and_signal(df: pd.DataFrame, fp: Union[None, str] = None):
    kline_data = df[['open', 'close', 'low', 'high']].values.tolist()
    closing_p = df['close'].tolist()
    try:
        daily_yield_strat = df['pnl'].tolist()
        daily_yield_bench = df['benchmark_pnl'].tolist()
        yield_strat = df['pnl_cumsum'].tolist()
        yield_bench = df['benchmark_pnl_cumsum'].tolist()
    except KeyError:
        daily_yield_strat = df['pnl_chg'].tolist()
        daily_yield_bench = df['benchmark_pnl_chg'].tolist()
        yield_strat = df['pnl_chg_cumsum'].tolist()
        yield_bench = df['benchmark_pnl_chg_cumsum'].tolist()
    sigs = df['signal'].tolist()
    x_axis_ = df.index.tolist()
    kline = (
        Kline()
        .add_xaxis(x_axis_)
        .extend_axis(yaxis=opts.AxisOpts(is_show=False, is_scale=True))
        .extend_axis(yaxis=opts.AxisOpts(is_show=False, is_scale=True))
        .extend_axis(yaxis=opts.AxisOpts(is_show=True, is_scale=True))
        .add_yaxis(
            "标的行情K线图",
            kline_data,
            itemstyle_opts=opts.ItemStyleOpts(color="#ec0000", color0="#00da3c"),
        )
        .set_global_opts(
            yaxis_opts=opts.AxisOpts(is_scale=True),
            xaxis_opts=opts.AxisOpts(is_scale=True),
            title_opts=opts.TitleOpts(
                pos_top="top",
                title="资金收益（率）曲线图",
                subtitle="*注："
                         "\n1. 小数位过长是由于浮点数计算自然存在的误差。"
                         "\n2. 基准默认以单手标的收益计算。"
                         "\n3. 日内调仓策略的收益曲线是由分钟级行情合成日频图线， 其仓位信号亦是当日加总信号。"
            ),
            datazoom_opts=[
                opts.DataZoomOpts(range_start=95, range_end=100),
                opts.DataZoomOpts(is_show=False, type_='inside', range_start=95, range_end=100)
            ],
            toolbox_opts=opts.ToolboxOpts(),
            tooltip_opts=opts.TooltipOpts(
                is_show=True,
                trigger='axis',
                axis_pointer_type='cross',
            )
        )
    )
    b_ret = (
        Bar()
        .add_xaxis(x_axis_)
        .add_yaxis(
            "策略单期收益",
            daily_yield_strat,
            yaxis_index=1,
        )
        .add_yaxis(
            "基准单期收益",
            daily_yield_bench,
            yaxis_index=1,
        )
        .set_series_opts(
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    close_line = (
        Line()
        .add_xaxis(x_axis_)
        .add_yaxis(
            "收盘价",
            closing_p,
        )
        .set_series_opts(
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    sig_line = (
        Line()
        .add_xaxis(xaxis_data=x_axis_)
        .add_yaxis(
            series_name="仓位信号",
            y_axis=sigs,
            yaxis_index=2,
            label_opts=opts.LabelOpts(is_show=True),
            linestyle_opts=opts.LineStyleOpts(color="#fff"),
            itemstyle_opts=opts.ItemStyleOpts(
                color="red", border_color="#fff", border_width=3
            ),
            tooltip_opts=opts.TooltipOpts(is_show=True),
            areastyle_opts=opts.AreaStyleOpts(color=JsCode(area_color_js), opacity=1),
        )
    )
    ret_cum_line = (
        Line()
        .add_xaxis(x_axis_)
        .add_yaxis(
            "策略累计收益",
            yield_strat,
            yaxis_index=3,
        )
        .add_yaxis(
            "基准累计收益",
            yield_bench,
            yaxis_index=3,
        )
        .set_series_opts(
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    b_ret.overlap(sig_line)
    kline.overlap(close_line)
    kline.overlap(b_ret)
    kline.overlap(ret_cum_line)

    kline.width = "1800PX"
    kline.height = "600PX"
    if fp is not None:
        kline.render(fp)
    else:
        return kline


def draw_factor_return_eval(df: pd.DataFrame):
    cols = df.columns.tolist()
    cols.remove('return')
    charts = []
    for i in range(len(cols)):
        dataset_ = [
            list(z) for z in zip(df[cols[i]].tolist(), df['return'].tolist())
        ]
        chart = (
            Scatter()
            .add_dataset(
                dimensions=[
                    "factor",
                    "pnl"
                ],
                source=dataset_,
            )
            .add_yaxis(
                series_name=cols[i],
                y_axis=[],
                symbol_size=2.5,
                xaxis_index=0,
                yaxis_index=0,
                encode={"x": "factor", "y": "pnl", "tooltip": [0, 1, 2, 3, 4]},
                label_opts=opts.LabelOpts(is_show=False),
            )
            .set_global_opts(
                xaxis_opts=opts.AxisOpts(
                    type_="value",
                    grid_index=0,
                    name="factor",
                    axislabel_opts=opts.LabelOpts(rotate=50, interval=0),
                    is_scale=True
                ),
                yaxis_opts=opts.AxisOpts(type_="value", grid_index=0, name="pnl", is_scale=True),
                title_opts=opts.TitleOpts(title="Factor-pnl Eval"),
                datazoom_opts=[
                    opts.DataZoomOpts(range_start=95, range_end=100),
                    opts.DataZoomOpts(is_show=False, type_='inside', range_start=95, range_end=100)
                ],
                toolbox_opts=opts.ToolboxOpts(),
                tooltip_opts=opts.TooltipOpts(
                    is_show=True,
                    trigger='axis',
                    axis_pointer_type='cross',
                )
            )
        )

        chart.width = "600PX"
        chart.height = "600PX"
        charts.append(chart)
    return charts


def draw_factor_eval(df: pd.DataFrame):
    cols = df.columns.tolist()
    cols.remove('return')
    charts = []
    for i in range(len(cols)):
        dataset_ = [
            list(z) for z in zip(df[cols[i]].tolist(), df[cols[i]].shift(-1).tolist())
        ]
        chart = (
            Scatter()
            .add_dataset(
                dimensions=[
                    "factor",
                    "next_factor"
                ],
                source=dataset_,
            )
            .add_yaxis(
                series_name=cols[i],
                y_axis=[],
                symbol_size=2.5,
                xaxis_index=0,
                yaxis_index=0,
                encode={"x": "factor", "y": "next_factor", "tooltip": [0, 1, 2, 3, 4]},
                label_opts=opts.LabelOpts(is_show=False),
            )
            .set_global_opts(
                xaxis_opts=opts.AxisOpts(
                    type_="value",
                    grid_index=0,
                    name="factor",
                    axislabel_opts=opts.LabelOpts(rotate=50, interval=0),
                    is_scale=True,
                ),
                yaxis_opts=opts.AxisOpts(type_="value", grid_index=0, name="next_factor", is_scale=True),
                title_opts=opts.TitleOpts(title="Factor Eval"),
                datazoom_opts=[
                    opts.DataZoomOpts(range_start=95, range_end=100),
                    opts.DataZoomOpts(is_show=False, type_='inside', range_start=95, range_end=100)
                ],
                toolbox_opts=opts.ToolboxOpts(),
                tooltip_opts=opts.TooltipOpts(
                    is_show=True,
                    trigger='axis',
                    axis_pointer_type='cross',
                )
            )
        )
        chart.width = "600PX"
        chart.height = "600PX"
        charts.append(chart)
    return charts


def form_page(elements: list, fp):
    page = Page("Rich Visual", layout=Page.SimplePageLayout)
    page.add(*elements)
    page.render(fp)