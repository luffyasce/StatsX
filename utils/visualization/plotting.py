import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from typing import Union


def draw_dist(
        data: pd.Series,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
        **kwargs,
):
    sns.set(rc={'figure.figsize': (width, height)})
    fig = plt.figure(figsize=(width, height))
    img = sns.histplot(data, **kwargs)
    if filename is None:
        return img
    else:
        plt.savefig(filename)
    plt.close()


def draw_dist_vec(
        data: np.array,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
        **kwargs,
):
    fig = plt.figure(figsize=(width, height))
    img = plt.hist(data, **kwargs)
    if filename is None:
        return img
    else:
        plt.savefig(filename)
    plt.close()


def draw_overlay_lines(
        data: pd.DataFrame,
        secondary_y: Union[str, list, bool] = False,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
        **kwargs,
):
    img = data.plot(
        figsize=(width, height),
        secondary_y=secondary_y,
        **kwargs,
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_overlay_lines_vec(
        x: np.array,
        *ys,
        x_label: str,
        y_labels: tuple,
        secondary_y: Union[str, bool] = False,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
):
    fig, ax1 = plt.subplots(figsize=(width, height))

    for i, y in enumerate(ys):
        if secondary_y and y_labels[i] == secondary_y:
            color = 'tab:red'
            ax1.set_xlabel(xlabel=x_label, color=color)
            ax1.set_ylabel(y_labels[i], color=color)
            ax1.plot(x, y, color=color)
            ax1.tick_params(axis='y', labelcolor=color)
        else:
            ax2 = ax1.twinx()
            ax2.set_xlabel(xlabel=x_label)
            ax2.set_ylabel(y_labels[i])
            ax2.plot(x, y)
            ax2.tick_params(axis='y')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)
    plt.close()


def draw_split_pnl(
        data: pd.DataFrame,
        drawdown_column_name: str,
        profit_column_name: str,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
        **kwargs,
):
    """
    containing pnl line and drawdown
    :param profit_column_name:
    :param drawdown_column_name:
    :param data: should be a dataframe of [strategy pnl, drawdown]
    :param width:
    :param height:
    :param filename:
    :param kwargs:
    :return:
    """

    img = data.plot(
        figsize=(width, height),
        secondary_y=[drawdown_column_name, profit_column_name],
        colormap='tab20',
        **kwargs,
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_single_axis_line(
        data: pd.DataFrame,
        width: int = 16,
        height: int = 9,
        title: str = None,
        filename: Union[str, None] = None
):
    title = "" if title is None else title
    img = data.plot(
        title=title,
        figsize=(width, height),
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_single_axis_line_vec(
        x: np.array,
        *ys,
        x_label: str,
        y_labels: tuple,
        width: int = 16,
        height: int = 9,
        title: str = None,
        filename: Union[str, None] = None
):
    fig = plt.figure(figsize=(width, height))
    for i, y in enumerate(ys):
        plt.plot(x, y, label=y_labels[i])
    plt.title(title)
    plt.xlabel(x_label)
    plt.legend()
    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)
    plt.close()


def draw_pie_chart(
        data: pd.Series,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None
):
    """
    draw single pie chart with option to save figure locally.
    :param data:
    :param width:
    :param height:
    :param filename:
    :return:
    """
    img = data.plot(
        kind='pie',
        figsize=(width, height),
    ).get_figure()
    if filename is None:
        return img
    else:
        img.savefig(filename)
    plt.close()


def draw_pie_charts(
        data: pd.DataFrame,
        width: int = 16,
        height: int = 9,
):
    """
    draw pie charts in subplots without saving figures.
    :param data:
    :param width:
    :param height:
    :return:
    """
    img = data.plot(
        kind='pie',
        figsize=(width, height),
        subplots=True
    )
    return img


def draw_scatter(
        data: pd.DataFrame,
        x: str,
        y: str,
        groupby: Union[str, None] = None,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None
):
    sns.set(rc={'figure.figsize': (width, height)})
    fig = plt.figure(figsize=(width, height))
    img = sns.scatterplot(data=data, x=x, y=y, hue=groupby, )
    if filename is None:
        return img
    else:
        plt.savefig(filename)
    plt.close()


def draw_scatter_vec(
        data_x: np.array,
        data_y: np.array,
        x_label: str,
        y_label: str,
        width: int = 16,
        height: int = 9,
        filename: Union[str, None] = None,
        **kwargs
):
    fig = plt.figure(figsize=(width, height))
    img = plt.scatter(data_x, data_y, **kwargs)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    if filename is None:
        return img
    else:
        plt.savefig(filename)
    plt.close()
