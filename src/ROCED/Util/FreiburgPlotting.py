#!/usr/bin/env python
# ===============================================================================
#
# Copyright (c) 2015, 2016 by Georg Fleig, Frank Fischer
#
# This file is part of ROCED.
#
# ROCED is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ROCED is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ROCED.  If not, see <http://www.gnu.org/licenses/>.
#
# ===============================================================================
"""
Plots useful information from HTCondorRequirementAdapter and FreiburgSiteAdapter JSON outputs.
"""
from __future__ import print_function, unicode_literals, division

import argparse
import json
import numpy as np
from collections import OrderedDict
from os import path

import matplotlib
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt

font = {"family": "sans", "size": 20}
matplotlib.rc("font", **font)
matplotlib.rcParams["figure.figsize"] = 24, 10

time_scales = {"s": ("seconds", 1.0),
               "m": ("minutes", 60.0),
               "h": ("hours", 60.0 * 60.0),
               "d": ("days", 60.0 * 60.0 * 24.0)}


def moving_average(a, n=3):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n


class Data(object):
    """Constant keys used in JSON file. "Machine type" & "site name" are dynamically determined by ROCED config.

    Example content:
    {
    'fr-default': {'jobs_running': 594, 'jobs_idle': 2460},
    'freiburg_cloud': {'machines_requested': 56, 'condor_nodes_draining': 0, 'condor_nodes': 149}
    }
    """
    condor_running = "jobs_running"
    condor_idle = "jobs_idle"
    vm_requested = "machines_requested"
    vm_running = "condor_nodes"
    vm_draining = "condor_nodes_draining"


def init_plots(style=None, split=None, max_=None, time_scale=None):
    """Split the plot into 2 subplots + 1 "legend" plot.

    Bottom plot has the main information, top plot shows rough numbers of idle total jobs.
    Legend plot is just an invisible placeholder to reserve place for the legend.
    """
    plots = []
    split_value = (split / 100 + 1) * 100
    if split_value > max_:
        # If we only have a few jobs, all that stuff is unnecessary.
        fig = plt.figure()
        bottom_plot = fig.add_subplot(111)
        plt.hold(True)
        plots.append(bottom_plot)
    else:
        l = 1
        n = 3
        m = 8
        ratio = n / m
        gs = gridspec.GridSpec(3, 1, height_ratios=[l, n, m])
        plt.figure()
        top_plot = plt.subplot(gs[1, :])
        plots.append(top_plot)
        bottom_plot = plt.subplot(gs[2, :], sharex=top_plot)
        plots.append(bottom_plot)
        legend = plt.subplot(gs[0, :])
        plots.append(legend)
        top_plot.spines["bottom"].set_linestyle("dotted")
        top_plot.locator_params(axis="y", tight=True, nbins=5)
        bottom_plot.spines["top"].set_linestyle("dotted")
        legend.set_frame_on(False)
        legend.get_xaxis().set_visible(False)
        legend.get_yaxis().set_visible(False)
        plt.subplots_adjust(hspace=0.1)

        top_plot.tick_params(axis="x", bottom="off", labelbottom="off")
        bottom_plot.tick_params(axis="x", top="off", labeltop="off")

        # zoom in specific areas of the plot
        top_plot.set_ylim(split_value, max_)
        bottom_plot.set_ylim(0, split_value)

        # Diagonal splitting lines to show plot separation/different y-scales.
        length = .01
        kwargs = dict(transform=top_plot.transAxes, color="k", clip_on=False)
        top_plot.plot((-length, length), (-length, length), **kwargs)  # bottom-left (0,0)
        top_plot.plot((1 - length, 1 + length), (-length, length), **kwargs)  # bottom-right (1,0)
        kwargs.update(transform=bottom_plot.transAxes)  # switch to the bottom plot
        bottom_plot.plot((-length, length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-left (0,1)
        bottom_plot.plot((1 - length, 1 + length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-right(1,1)

    for figure in plots:
        figure.tick_params(axis="x", pad=15)
        figure.tick_params(axis="y", pad=15)

    if style == "screen":
        kwargs = dict()
        kwargs2 = dict(y=0.7, labelpad=20.0)
    elif style == "slide":
        kwargs = dict(size=36.0)
        kwargs2 = dict(y=0.71, labelpad=37.0, size=33.0)
        for figure in plots:
            figure.tick_params(axis="x", labelsize=34, pad=10., length=10)
            figure.tick_params(axis="y", labelsize=34, length=10)
    else:
        raise ValueError("Plotting style unknown!")
    bottom_plot.set_xlabel(r"Time [%s]" % time_scale, ha="right", x=1, **kwargs)
    bottom_plot.set_ylabel(r"Number of Jobs/VMs", va="top", **kwargs2)

    return plots


def get_plot_dict(plot_style):
    """Define plot styles."""
    # set dictionary for labels and colors, depending on style setting
    if plot_style == "screen":
        plot_dict = {
            Data.condor_running: ("jobs running", "#b8c9ec"),  # light blue
            Data.condor_idle: ("jobs waiting", "#fdbe81"),  # light orange
            Data.vm_requested: ("Slots requested", "#fb8a1c"),  # orange
            Data.vm_running: ("Slots available", "#2c7bb6"),  # blue
            Data.vm_draining: ("Slots draining", "#7f69db"),  # light blue
        }
    elif plot_style == "slide":
        plot_dict = {
            Data.condor_running: ("Jobs running", "#b8c9ec"),  # light blue
            Data.condor_idle: ("Jobs waiting", "#fdbe81"),  # light orange
            Data.vm_requested: ("Slots requested", "#fb8a1c"),  # orange
            Data.vm_running: ("Slots available", "#2c7bb6"),  # blue
            Data.vm_draining: ("Slots draining", "#7f69db"),  # light blue
        }
        matplotlib.rcParams["svg.fonttype"] = "none"
        matplotlib.rcParams["path.simplify"] = True
        matplotlib.rcParams["path.simplify_threshold"] = 0.5
        matplotlib.rcParams["font.sans-serif"] = "Linux Biolinum O"
        matplotlib.rcParams["font.family"] = "sans-serif"
        matplotlib.rcParams["figure.dpi"] = 300
    else:
        raise ValueError("Plotting style unknown!")
    return plot_dict


def fill_empty_values(correction_period, correct_smooth, rel_times, content):
    # type: (int, bool, np.ndarray, list) -> (np.ndarray, list)
    """Fill long periods without log entries with additional entries to make the plot smoother."""

    # get indices of timestamps with time difference > correction_period
    rel_time_diffs = np.diff(rel_times)
    if correct_smooth is True:
        indices = np.nonzero(rel_time_diffs > correction_period)
        print("Ignoring %i periods with no log entries for over %s seconds:" % (len(indices[0]), correction_period))
    else:
        indices = []

    index_offset = 0
    try:
        for index in indices[0]:
            print("Begin: %ss, End: %ss, Diff: %ss"
                  % (rel_times[index + index_offset], rel_times[index + index_offset + 1],
                     rel_times[index + index_offset + 1] - rel_times[index + index_offset]))

            # add two entries to x  axis (time)
            rel_times = np.insert(rel_times, index + index_offset + 1, rel_times[index + index_offset] + 1)
            rel_times = np.insert(rel_times, index + index_offset + 2, rel_times[index + index_offset + 2] - 1)

            # add two entries to y axis, if one value == 0: 0, else: average
            prev = content[index + index_offset - 1]
            next_ = content[index + index_offset + 1]
            new = {}
            try:
                if correct_smooth is True:
                    for key in set.intersection(set(prev), set(next_)):
                        new[key] = {}
                        for value_key in set.intersection(set(prev[key]), set(next_[key])):
                            if prev[key][value_key] == 0 or next_[key][value_key] == 0:
                                new[key][value_key] = 0
                            else:
                                new[key][value_key] = int((prev[key][value_key] + next_[key][value_key]) / 2)
                else:
                    raise ValueError
            except (ValueError, KeyError):
                new = {None: {None}}
            print("%d & %d: %s" % (index + index_offset, index + index_offset + 1, new))
            content.insert(index + index_offset, new)
            content.insert(index + index_offset + 1, new)
            # print(index + index_offset, index + index_offset + 1)
            index_offset += 2
    except IndexError:
        pass
    return rel_times, content


def main(file_list, live, output_name, correction_period, correct_zero, time_scale, plot_style, x_limits,
         smooth, cores):
    plot_dict = get_plot_dict(plot_style)

    # get log files and sort entries, result is a tuple
    logs = {}
    for input_file in file_list:
        if ".json" in input_file:
            with open(input_file, "r") as json_file:
                logs.update(json.load(json_file))
    log = sorted(logs.items())

    # convert timestamps to relative times and store quantities in separate lists
    timestamps, content = zip(*log)
    rel_times = np.array([(int(timestamp) - int(timestamps[0])) for timestamp in timestamps])
    content = list(content)

    if correction_period > 0:
        rel_times, content = fill_empty_values(correction_period, correct_zero, rel_times, content)

    rel_times = rel_times / time_scales.get(time_scale, "m")[1]

    quantities = {}

    # add empty list for each quantity
    for quantity in plot_dict:
        quantities[quantity] = np.zeros(len(rel_times))

    # add content to quantity lists, use np.NaN if no value is available
    i_entry = 0
    for entry in content:
        for site in entry:
            for quantity in plot_dict:
                if quantity in entry[site]:
                    try:
                        quantities[quantity][i_entry] = entry[site][quantity]
                    except (KeyError, TypeError):
                        if quantity == Data.vm_draining:
                            quantities[quantity][i_entry] = 0
                        else:
                            quantities[quantity][i_entry] = np.NaN
                            # print("Missing information: %s %s" %
                            #       (timestamps[i_entry], datetime.fromtimestamp(float(timestamps[i_entry]))))
        i_entry += 1

    if smooth:
        average_order = len(rel_times) / 500
        rel_times = rel_times[:-(average_order - 1)]
        for data in quantities:
            quantities[data] = moving_average(quantities[data], n=average_order)

    # build up quantities (stack them)
    jobs_idle = np.add(quantities[Data.condor_idle], quantities[Data.condor_running])
    jobs_running = quantities[Data.condor_running]

    machines_requested = cores * np.add(quantities[Data.vm_requested],
                                        np.add(quantities[Data.vm_running], quantities[Data.vm_draining]))
    condor_nodes = cores * np.add(quantities[Data.vm_running], quantities[Data.vm_draining])
    condor_nodes_draining = cores * quantities[Data.vm_draining]

    # Setup plots
    plots = init_plots(style=plot_style, split=int(np.max(machines_requested)),
                       max_=int(np.max(jobs_idle)),
                       time_scale=time_scales.get(time_scale, "m")[0])
    plot_count = len(plots)
    if plot_count == 1:
        plot_count = 2

    for figure in plots[0:plot_count - 1]:
        figure.plot(rel_times, machines_requested, label=plot_dict[Data.vm_requested][0],
                    color=plot_dict[Data.vm_requested][1], linestyle="-", marker="", linewidth=2.0)
        figure.plot(rel_times, condor_nodes, label=plot_dict[Data.vm_running][0],
                    color=plot_dict[Data.vm_running][1], linestyle="-", marker="", linewidth=2.0)
        figure.plot(rel_times, condor_nodes_draining, label=plot_dict[Data.vm_draining][0],
                    color=plot_dict[Data.vm_draining][1], linestyle="-", marker="", linewidth=2.0)

        stack1 = figure.fill_between(rel_times, jobs_idle, facecolor=plot_dict[Data.condor_idle][1],
                                     color=None, edgecolor=None, linewidth=0.0, label=plot_dict[Data.condor_idle][0])
        stack2 = figure.fill_between(rel_times, jobs_running, facecolor=plot_dict[Data.condor_running][1],
                                     color=None, edgecolor=None, linewidth=0.0, label=plot_dict[Data.condor_running][0])
        for entry in stack1, stack2:
            figure.plot([], [], color=entry.get_facecolor()[0], linewidth=10, label=entry.get_label())

        if x_limits:
            figure.set_xlim(x_limits[0], x_limits[1])

    # We add multiple instances of line descriptions - get rid of them via OrderedDict
    handles, labels = plots[0].get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    if plots[0] is not plots[-1]:
        # If we have subplots, add legend to a separate subplot.
        kwargs = dict(bbox_to_anchor=(0, 0, 1, 1), mode="expand", loc="lower left")
    else:
        kwargs = dict(loc="best")
    if plot_style == "slide":
        kwargs["fontsize"] = 30

    plots[-1].legend(by_label.values(), by_label.keys(), ncol=2, **kwargs)

    if live:
        plt.show()
    else:
        if not output_name:
            output_name = path.splitext(file_list[0])[0]
        plt.savefig(output_name + ".png", bbox_inches="tight")
        plt.savefig(output_name + ".pdf", bbox_inches="tight")
        plt.savefig(output_name + ".svg", bbox_inches="tight")
        print("Output written to: %s" % output_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plotting tool for ROCED status logs.")
    parser.add_argument("file_list", type=str, nargs="+",
                        help="input file list")
    parser.add_argument("-l", "--live", action="store_true",
                        help="plot to screen (default: %(default)s)")
    parser.add_argument("-o", "--output_name", type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument("--correction-period", type=int, default=600,
                        help="time in seconds for periods without log entries to be ignored, "
                             "0 disables correction (default: %(default)s)")
    parser.add_argument("--correct-zero", action="store_false", default=True,
                        help="Add 0 values when correcting/smoothing longer periods. "
                             "Option disables it. (default: %(default)s)")
    parser.add_argument("--cores", type=int, default="4",
                        help="Cores per VM. (default: %(default)s)")
    parser.add_argument("-t", "--time-scale", type=str, default="m",
                        help="time scale of plot: s(econd), m(inute), h(our), d(ay) (default: %(default)s)")
    parser.add_argument("-s", "--plot_style", type=str, default="screen",
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument("-x", "--x-limits", type=float, default=None, nargs=2,
                        help="x-axis limit (lower, upper) (default: %(default)s)")
    parser.add_argument("--smooth", action="store_true", default=False,
                        help="Smooth the plot by creating a moving average (default: %(default)s)")
    main(**vars(parser.parse_args()))
