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
from os import path
# import datetime
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

font = {"family": "sans", "size": 20}
matplotlib.rc("font", **font)
matplotlib.rcParams["figure.figsize"] = 18, 8

time_scales = {"s": ("seconds", 1.0),
               "m": ("minutes", 60.0),
               "h": ("hours", 60.0 * 60.0),
               "d": ("days", 60.0 * 60.0 * 24.0)}


def init_plots(style=None, split=None, max=None, time_scale=None):
    """Split the plot into 2 subplots + 1 "legend" plot.

    Bottom plot has the main information, top plot shows rough numbers of idle total jobs.
    Legend plot is just an invisible placeholder to reserve place for the legend.
    """
    plots = []
    split_value = (split / 100 + 1) * 100
    if split_value > max:
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
        fig = plt.figure()
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
        top_plot.set_ylim(split_value, max)
        bottom_plot.set_ylim(0, split_value)

        # Diagonal splitting lines to show plot separation/different y-scales.
        length = .01
        kwargs = dict(transform=top_plot.transAxes, color="k", clip_on=False)
        top_plot.plot((-length, length), (-length, length), **kwargs)  # bottom-left (0,0)
        top_plot.plot((1 - length, 1 + length), (-length, length), **kwargs)  # bottom-right (1,0)
        kwargs.update(transform=bottom_plot.transAxes)  # switch to the bottom plot
        bottom_plot.plot((-length, length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-left (0,1)
        bottom_plot.plot((1 - length, 1 + length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-right (1,1)

    for figure in plots:
        figure.tick_params(axis="x", pad=15)
        figure.tick_params(axis="y", pad=15)

    if style == "fr-screen":
        bottom_plot.set_xlabel(r"Time [%s]" % time_scale, ha="right", x=1)
        bottom_plot.set_ylabel(r"Number of Jobs/VMs", va="top", y=.7, labelpad=20.0)
    elif style == "fr-slide":
        bottom_plot.set_xlabel(r"Time [%s]" % time_scale, ha="right", x=1, size=36.0)
        bottom_plot.set_ylabel(r"Number of Jobs/VMs", va="top", y=.71, labelpad=37.0, size=33.0)
        for figure in plots:
            figure.tick_params(axis="x", labelsize=34, pad=10., length=10)
            figure.tick_params(axis="y", labelsize=34, length=10)
    else:
        raise ValueError("Plotting style unknown!")

    return plots


def main():
    parser = argparse.ArgumentParser(description="Plotting tool for ROCED status logs.")
    parser.add_argument("input_files", type=str, nargs="+",
                        help="input files")
    parser.add_argument("-l", "--live", action="store_true",
                        help="plot to screen (default: %(default)s)")
    parser.add_argument("-o", "--output", type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument("--correction-period", type=int, default=600,
                        help="time in seconds for periods without log entries to be ignored, "
                             "0 disables correction (default: %(default)s)")
    parser.add_argument("-t", "--time-scale", type=str, default="m",
                        help="time scale of plot: s(econd), m(inute), h(our), d(ay) (default: %(default)s)")
    parser.add_argument("-s", "--style", type=str, default="fr-screen",
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument("-x", "--xlim", type=float, default=None, nargs=2,
                        help="x-axis limit (lower, upper) (default: %(default)s)")
    args = parser.parse_args()

    # set dictionary for labels and colors, depending on style setting
    if args.style == "fr-screen":
        plot_dict = {
            "jobs_running": ("jobs running", "#b8c9ec"),  # light blue
            "jobs_idle": ("jobs waiting", "#fdbe81"),  # light orange
            "machines_requested": ("Slots requested", "#fb8a1c"),  # orange
            "condor_nodes": ("Slots available", "#2c7bb6"),  # blue
            "condor_nodes_draining": ("Slots draining", "#7f69db"),  # light blue
        }
    elif args.style == "fr-slide":
        plot_dict = {
            "jobs_running": ("Jobs running", "#b8c9ec"),  # light blue
            "jobs_idle": ("Jobs waiting", "#fdbe81"),  # light orange
            "machines_requested": ("Slots requested", "#fb8a1c"),  # orange
            "condor_nodes": ("Slots available", "#2c7bb6"),  # blue
            "condor_nodes_draining": ("Slots draining", "#7f69db"),  # light blue
        }
        matplotlib.rcParams["svg.fonttype"] = "none"
        matplotlib.rcParams["path.simplify"] = True
        matplotlib.rcParams["path.simplify_threshold"] = 0.5
        matplotlib.rcParams["font.sans-serif"] = "Linux Biolinum O"
        matplotlib.rcParams["font.family"] = "sans-serif"
    else:
        raise ValueError("Plotting style unknown!")

    # get log files and sort entries, result is a tuple
    logs = {}
    for input_file in args.input_files:
        if ".json" in input_file:
            with open(input_file, "r") as json_file:
                logs.update(json.load(json_file))
    log = sorted(logs.items())

    # convert timestamps to relative times and store quantities in separate lists
    timestamps, content = zip(*log)
    rel_times = np.array([(int(timestamp) - int(timestamps[0])) for timestamp in timestamps])
    content = list(content)

    ###
    # find long periods between two log entries and create two new entries per period with 0 data to correct the plot
    ###
    if args.correction_period > 0:
        rel_time_diffs = np.diff(rel_times)
        # get all indices of timestamps with a time difference greater than requested
        indices = np.nonzero(rel_time_diffs > args.correction_period)
        print("Ignoring %i periods with no log entries for over %s seconds:"
              % (len(indices[0]), args.correction_period))
        index_offset = 0
        for index in indices[0]:
            print("Begin: %ss, End: %ss, Diff: %ss"
                  % (rel_times[index + index_offset], rel_times[index + 1 + index_offset],
                     rel_times[index + 1 + index_offset] - rel_times[index + index_offset]))
            # add two entries to time axis
            rel_times = np.insert(rel_times, index + index_offset + 1, rel_times[index + index_offset] + 1)
            rel_times = np.insert(rel_times, index + index_offset + 2, rel_times[index + index_offset + 2] - 1)
            # add two entries to y axis containing nothing
            content.insert(index + index_offset, {None: {None}})
            content.insert(index + index_offset + 1, {None: {None}})
            print(index + index_offset, index + index_offset + 1)
            index_offset += 2

    rel_times = rel_times / time_scales.get(args.time_scale, "m")[1]

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
                        if quantity == "condor_nodes_draining":
                            quantities[quantity][i_entry] = 0
                        else:
                            quantities[quantity][i_entry] = np.NaN
                            # print("Missing information: %s %s" %
                            #       (timestamps[i_entry], datetime.fromtimestamp(float(timestamps[i_entry]))))
        i_entry += 1

    # build up quantities (stack them)
    jobs_idle = np.add(quantities["jobs_idle"], quantities["jobs_running"])
    jobs_running = quantities["jobs_running"]
    machines_requested = 4 * np.add(quantities["machines_requested"],
                                    np.add(quantities["condor_nodes"], quantities["condor_nodes_draining"]))
    condor_nodes = 4 * np.add(quantities["condor_nodes"], quantities["condor_nodes_draining"])
    condor_nodes_draining = 4 * quantities["condor_nodes_draining"]

    # Setup plots
    plots = init_plots(style=args.style, split=int(np.max(machines_requested)),
                       max=int(np.max(jobs_idle)),
                       time_scale=time_scales.get(args.time_scale, "m")[0])
    counter = len(plots)
    if counter == 1:
        counter = 2

    for figure in plots[0:counter - 1]:
        figure.plot(rel_times, machines_requested, label=plot_dict["machines_requested"][0],
                    color=plot_dict["machines_requested"][1], linestyle="-", marker='', linewidth=2.0)
        figure.plot(rel_times, condor_nodes, label=plot_dict["condor_nodes"][0],
                    color=plot_dict["condor_nodes"][1], linestyle="-", marker='', linewidth=2.0)
        figure.plot(rel_times, condor_nodes_draining, label=plot_dict["condor_nodes_draining"][0],
                    color=plot_dict["condor_nodes_draining"][1], linestyle="-", marker='', linewidth=2.0)

        stack1 = figure.fill_between(rel_times, jobs_idle, facecolor=plot_dict["jobs_idle"][1],
                                     color=None, edgecolor=None, linewidth=0.0, label=plot_dict["jobs_idle"][0])
        stack2 = figure.fill_between(rel_times, jobs_running, facecolor=plot_dict["jobs_running"][1],
                                     color=None, edgecolor=None, linewidth=0.0, label=plot_dict["jobs_running"][0])
        for entry in stack1, stack2:
            figure.plot([], [], color=entry.get_facecolor()[0], linewidth=10, label=entry.get_label())

    handles, labels = plots[0].get_legend_handles_labels()
    kwargs = {}
    if plots[0] is not plots[-1]:
        # If we have subplots, add legend to a separate subplot.
        kwargs["bbox_to_anchor"] = (0, 0, 1, 1)
        kwargs["mode"] = "expand"
        kwargs["loc"] = "lower left"
    else:
        kwargs["loc"] = "best"
    if args.style == "fr-slide":
        kwargs["fontsize"] = 30

    plots[-1].legend(handles, labels, ncol=2, **kwargs)

    if args.xlim:
        plt.xlim(xmin=args.xlim[0], xmax=args.xlim[1])

    if args.live:
        plt.show()
    else:
        if not args.output:
            args.output = path.splitext(args.input_files[0])[0]
        plt.savefig(args.output + ".png", bbox_inches="tight")
        plt.savefig(args.output + ".pdf", bbox_inches="tight")
        plt.savefig(args.output + ".svg", bbox_inches="tight")
        print("Output written to: %s" % args.output)


if __name__ == "__main__":
    main()
