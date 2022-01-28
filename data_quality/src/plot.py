import os
import math
from typing import Union, List, Optional

import numpy as np
import pandas as pd
from bokeh.plotting import figure, save
from bokeh.models import (Label, Button, CustomJS, ColumnDataSource, Div)
from bokeh.models.widgets import DataTable, TableColumn, Panel, Tabs
from bokeh.layouts import column, row, Spacer
from bokeh.io import output_file, show
from valdec.decorators import validate

from data_quality.src.utils import _human_format, _human_format_perc
from data_quality.src.check import TAG_CHECK_DESCRIPTION


def _create_gauge_plot(percertage: Union[float, List[float]],
                       width=100, height=100,
                       font_size=12,
                       prefix: str = None,
                       suffix: str = None,
                       colors: list = None):

    if isinstance(percertage, float):
        percertage = [percertage]
    percertage = np.cumsum(percertage)
    if colors is None:
        colors = ["orange", "grey", "red", "blue"]

    p = figure(x_range=(-1, 1), y_range=(-1, 1),
               plot_width=width, plot_height=height,
               tools="")
    p.background_fill_color = "white"
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.axis.visible = False
    p.toolbar.logo = None
    p.outline_line_color = None
    p.toolbar_location = None
    p.min_border_left = 0
    p.min_border_right = 0
    p.min_border_top = 0
    p.min_border_bottom = 0

    start_angle = -math.pi / 6
    end_angle = math.pi * 7 / 6

    p.annular_wedge(x=0, y=0, inner_radius=0.6, outer_radius=0.8, start_angle=start_angle, end_angle=end_angle,
                    color=colors[0])
    current_start = start_angle
    for i in range(len(percertage)):
        perc = percertage[::-1][i]
        split = current_start + (1 - perc) * (end_angle - start_angle)
        p.annular_wedge(x=0, y=0, inner_radius=0.6, outer_radius=0.8, start_angle=split, end_angle=end_angle,
                        color=colors[i+1])

    if prefix is None:
        prefix = ""
    if suffix is None:
        suffix = ""
    y = 0 if len(percertage) == 1 else 0.2
    p.add_layout(
        Label(x=0, y=y, text=f"{prefix}{_human_format_perc(percertage[0])}{suffix}",
              text_font_style="bold",
              text_font_size=f"{font_size}pt",
              text_baseline="middle",
              text_align="center",
              text_color="black"))
    if len(percertage) > 1:
        p.add_layout(
            Label(x=0, y=0, text=f"    +{_human_format_perc(percertage[1]-percertage[0])}\n warnings",
                  text_font_style="bold",
                  text_font_size=f"{font_size*0.5}pt",
                  text_baseline="top",
                  text_align="center",
                  text_color="black"))
    return p


def create_allert_icon(warning=False, size=40):
    p = figure(x_range=(-1.05, 1.05), y_range=(-1.05, 1.05),
               plot_width=size, plot_height=size,
               tools="")
    p.background_fill_color = "white"
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.axis.visible = False
    p.toolbar.logo = None
    p.outline_line_color = None
    p.toolbar_location = None
    p.min_border_left = 0
    p.min_border_right = 0
    p.min_border_top = 0
    p.min_border_bottom = 0

    h = 2 * np.sqrt(3) / 2
    width = 1/9
    if warning:
        color1 = "yellow"
        color2 = "black"
        line_color1 = "black"
        line_color2 = "yellow"
    else:
        color1 = "red"
        color2 = "white"
        line_color1 = "black"
        line_color2 = "black"
    line_width = 2 / 120 * size
    p.patch(x=[-1, 0, 1], y=[-h/2, h/2, -h/2], fill_color=color1, line_width=line_width, line_color=line_color1)
    p.patch(x=[width, width, -width, -width], y=[-h/4, h/4, h/4, -h/4], fill_color=color2, line_width=line_width, line_color=line_color2)
    p.circle([0], [-h * 3 / 8], radius=width, color=color2, line_width=line_width, line_color=line_color2)
    return p


def plot_table_results(table,
                       title: str = None,
                       sort_by_n_ko: bool = True,
                       consider_warnings: bool = True,
                       filter_only_ko: bool = True,
                       save_in_path: str = None,
                       show_flag: bool = False):
    table.calculate_result_info()
    show_warning = table.any_warning(flag_only_fail=filter_only_ko) and consider_warnings

    plots = []
    WIDTH = 1200
    HEIGHT = 100

    gauge_width = 300
    p = figure(x_range=(0, 1), y_range=(0, 1),
               plot_width=WIDTH - gauge_width, plot_height=gauge_width,
               tools="")
    p.background_fill_color = "white"
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.axis.visible = False
    p.toolbar.logo = None
    p.outline_line_color = None
    p.toolbar_location = None
    p.min_border_left = 0
    p.min_border_right = 0
    p.min_border_top = 0
    p.min_border_bottom = 0

    if title is not None:
        pass
    elif table.output_name is not None:
        title = table.output_name
    else:
        title = table.db_name.split(".")[-1]

    p.add_layout(
        Label(x=0.5, y=0.85, text=title,
              text_font_style="bold",
              text_font_size="30pt",
              text_baseline="middle",
              text_align="center",
              text_color="black"))

    p.add_layout(
        Label(x=0.01, y=0.5, text=f"Total number of rows: {_human_format(table.n_rows)}",
              text_font_style="bold",
              text_font_size="20pt",
              text_baseline="middle",
              text_align="left",
              text_color="black"))

    p.add_layout(
        Label(x=0.01, y=0.25, text=f"# Setted checks: {_human_format(table.n_checks)}",
              text_font_style="bold",
              text_font_size="15pt",
              text_baseline="middle",
              text_align="left",
              text_color="black"))
    if show_warning:
        p.add_layout(
            Label(x=0.01, y=0.15,
                  text=f"# Warning checks: {_human_format(table.n_warning_checks)}",
                  text_font_style="bold",
                  text_font_size="15pt",
                  text_baseline="middle",
                  text_align="left",
                  text_color="black"))

    if not table.over_n_max_rows_output(consider_warnings=False):
        n_problems = table.number_unique_rows_ko
        text = f"# Rows with a problem: {_human_format(n_problems)}"
        prefix = ""
    else:
        max_n_problems = min(table.total_number_ko, table.n_rows)
        min_n_problems = table.max_number_ko
        n_problems = min_n_problems
        text = f"Total number of Problems : {_human_format(table.total_number_ko)}"
        if (max_n_problems - min_n_problems) / table.n_rows > 0.01:
            prefix = ">"
        else:
            prefix = "≈"
    p.add_layout(
        Label(x=0.5, y=0.5, text=text,
              text_font_style="bold",
              text_font_size="20pt",
              text_baseline="middle",
              text_align="left",
              text_color="black"))

    if show_warning:
        if not table.over_n_max_rows_output(consider_warnings=True):
            n_warning = table.number_unique_rows_warning
            text = f"# Rows with a warning: {_human_format(n_warning)}"
            #prefix = ""
        else:
            max_n_warnings = min(table.total_number_warnings, table.n_rows)
            min_n_warnings = table.max_number_warnings
            n_warning = min_n_warnings
            text = f"Total number of Warnings : {_human_format(n_warning)}"
            # if (max_n_warnings - min_n_warnings) / table.n_rows > 0.01:
            #     prefix = ">"
            # else:
            #     prefix = "≈"
        p.add_layout(
            Label(x=0.5, y=0.25, text=text,
                  text_font_style="bold",
                  text_font_size="15pt",
                  text_baseline="middle",
                  text_align="left",
                  text_color="black"))
        gauge_plot = _create_gauge_plot([n_problems / table.n_rows, n_warning / table.n_rows],
                                        prefix=prefix,
                                        font_size=36,
                                        colors=["green", "orange", "red"], width=gauge_width, height=gauge_width)
    else:
        gauge_plot = _create_gauge_plot(n_problems / table.n_rows,
                                        prefix=prefix,
                                        font_size=36,
                                        colors=["green", "red"], width=gauge_width, height=gauge_width)
    plots.append(row(p, gauge_plot, align=["center"] * 2))

    check_list = table.check_list
    if not consider_warnings:
        check_list = [a for a in check_list if not a.flag_warning]

    if sort_by_n_ko:
        check_list = sorted(check_list, key=lambda d: - d.n_ko)

    for check in check_list:
        if (not filter_only_ko) or (check.n_ko > 0):
            perc_ko = check.n_ko / table.n_rows
            warning_icon_size = 40 if show_warning else 0
            width_labels = WIDTH - warning_icon_size
            check_label = Div(text=check.check_description, width=int(width_labels * 2 / 3), style={'font-size': '20pt'})
            n_check_label = Div(text=_human_format(check.n_ko), width=int(width_labels / 6), style={'font-size': '30pt'})
            perc_label = Div(text=_human_format_perc(perc_ko), width=int(width_labels / 6), style={'font-size': '30pt'})
            warning_icon = create_allert_icon(check.flag_warning, size=warning_icon_size)

            p = row(warning_icon, check_label, n_check_label, perc_label)

            if (check.ko_rows is not None) and (check.ko_rows.shape[0] > 0):
                df_plot = check.ko_rows.drop([TAG_CHECK_DESCRIPTION], axis=1)
                for col in table.datetime_columns:
                    if np.issubdtype(df_plot[col].dtype, np.datetime64):
                        try:
                            df_plot[col] = pd.to_datetime(df_plot[col], errors="ignore")
                            df_plot[col] = df_plot[col].dt.strftime("%Y-%m-%d")
                        except:
                            pass
                columns = [TableColumn(field=c, title=c) for c in df_plot.columns]
                row_height = 30
                table_height = min(row_height * (df_plot.shape[0] + 1), 600)
                data_table = DataTable(columns=columns, source=ColumnDataSource(df_plot), width=WIDTH,
                                       height=table_height,
                                       index_position=None)
                data_table.visible = False
                if check.flag_over_max_rows:
                    show_label = f"Show a max of {check.n_max_rows_output} samples"
                    hide_label = "Hide samples"
                else:
                    show_label = "Show details"
                    hide_label = "Hide details"
                button_show = Button(label=show_label, width_policy="min")
                button_hide = Button(label=hide_label, width_policy="min")
                button_show.js_on_click(CustomJS(
                    args=dict(
                        table=data_table,
                        button_hide=button_hide,
                        button_show=button_show
                    ),
                    code="""
                            table.visible=true
                            button_hide.visible=true
                            button_show.visible=false
                            """
                ))

                button_hide.js_on_click(CustomJS(args=dict(
                    table=data_table,
                    button_hide=button_hide,
                    button_show=button_show
                ),
                    code="""
                            table.visible=false
                            button_hide.visible=false
                            button_show.visible=true
                            """
                ))
                button_hide.visible = False

                p = column([p, button_show, button_hide, data_table], align=["center"] * 2)

            plots.append(column([Spacer(height=2, background="gray"), p], margin=(10, 10, 10, 10)))

    plot = column(plots)

    if save_in_path is not None:
        output_file(save_in_path, mode='inline')
        save(plot)
    if show_flag:
        if save_in_path is None:
            show(plot)
        else:
            os.startfile(save_in_path)
    return plot

@validate
def plot_session_results(session,
                         title: Optional[str] = None,
                         sort_by_n_ko: bool = True,
                         consider_warnings: bool = True,
                         filter_only_ko: bool = True,
                         save_in_path: Optional[str] = None,
                         show_flag: bool = False):
    if len(session.tables) == 1:
        session.tables[0].create_html_output(
            title=title,
            sort_by_n_ko=sort_by_n_ko,
            consider_warnings=consider_warnings,
            filter_only_ko=filter_only_ko,
            save_in_path=save_in_path,
            show_flag=show_flag)
    elif len(session.tables) > 1:
        plots = []
        for table in session.tables:
            plot = table.create_html_output(sort_by_n_ko=sort_by_n_ko,
                                            consider_warnings=consider_warnings,
                                            filter_only_ko=filter_only_ko,
                                            save_in_path=None,
                                            show_flag=False)
            if table.output_name is not None:
                t = table.output_name
            else:
                t = table.db_name.split(".")[-1]
            tab1 = Panel(child=plot, title=t)
            plots.append(tab1)
        tabs = Tabs(tabs=plots)
        tabs = column(tabs)
        if title is not None:
            p = Div(text=title, width=1200, height=100, style={'font-size': '30pt',
                                                               'text-align': 'center'})
            tabs = column(p, tabs)
        if save_in_path is not None:
            output_file(save_in_path, mode='inline')
            save(tabs)
        if show_flag:
            if save_in_path is None:
                show(tabs)
            else:
                os.startfile(save_in_path)
