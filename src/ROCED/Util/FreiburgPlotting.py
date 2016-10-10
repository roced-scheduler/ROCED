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
from __future__ import print_function, division

import argparse
import json
import matplotlib
import matplotlib.dates as mdates
import matplotlib.gridspec as mgridspec
import matplotlib.pyplot as mplt
import matplotlib.ticker as mticker
from collections import OrderedDict
from datetime import datetime
from os import path

import numpy as np

from Rrd import Rrd


class Data(object):
    """Constant keys used in JSON file. "Machine type" & "site name" are dynamically determined by ROCED config.

    Example content:
    { 15465879: {
    'fr-default': {'jobs_running': 594, 'jobs_idle': 2460},
    'freiburg_cloud': {'machines_requested': 56, 'condor_nodes_draining': 0, 'condor_nodes': 149}
    } }
    """
    condor_running = "jobs_running"
    condor_idle = "jobs_idle"
    vm_requested = "machines_requested"
    vm_running = "nodes_running"
    vm_draining = "nodes_draining"


class FrPlotting(object):
    """
    Plots useful information from HTCondorRequirementAdapter and FreiburgSiteAdapter JSON outputs.
    """

    @staticmethod
    def _init_plots(style=None, split=None, max_=None):
        """Split the plot into 2 subplots + 1 "legend" plot.

        Bottom plot has the main information, top plot shows rough numbers of idle total jobs.
        Legend plot is just an invisible placeholder to reserve place for the legend.
        """
        split_value = (split / 100 + 1) * 100
        if split_value > max_:
            # If we only have a few jobs, all that stuff is unnecessary.
            fig = mplt.figure()
            bottom_plot = fig.add_subplot(111)
            mplt.hold(True)
            plots = [bottom_plot]
        else:
            l = 1
            n = 3
            m = 8
            ratio = n / m
            gs = mgridspec.GridSpec(3, 1, height_ratios=[l, n, m])
            mplt.figure()

            top_plot = mplt.subplot(gs[1, :])
            bottom_plot = mplt.subplot(gs[2, :], sharex=top_plot)
            legend = mplt.subplot(gs[0, :])
            plots = [top_plot, bottom_plot, legend]

            top_plot.spines["bottom"].set_linestyle("dotted")
            top_plot.locator_params(axis="y", tight=True, nbins=5)
            bottom_plot.spines["top"].set_linestyle("dotted")
            legend.set_frame_on(False)
            legend.get_xaxis().set_visible(False)
            legend.get_yaxis().set_visible(False)
            mplt.subplots_adjust(hspace=0.1)

            top_plot.tick_params(axis="x", which="both", bottom="off", labelbottom="off")
            bottom_plot.tick_params(axis="x", which="both", top="off", labeltop="off")
            mplt.setp(bottom_plot.axes.get_xticklabels(), rotation=60)

            # zoom in specific areas of the plot
            bottom_plot.set_ylim(0, split_value)
            top_plot.set_ylim(split_value, max_)

            # Diagonal splitting lines to show plot separation/different y-scales.
            length = .01
            kwargs = dict(transform=top_plot.transAxes, color="k", clip_on=False)
            top_plot.plot((-length, length), (-length, length), **kwargs)  # bottom-left (0,0)
            top_plot.plot((1 - length, 1 + length), (-length, length), **kwargs)  # bottom-right (1,0)
            kwargs.update(transform=bottom_plot.transAxes)  # switch to the bottom plot
            bottom_plot.plot((-length, length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-left (0,1)
            bottom_plot.plot((1 - length, 1 + length), (1 - length * ratio, 1 + length * ratio), **kwargs)  # top-right

        for figure in plots:
            figure.tick_params(axis="both", which="both", pad=15, width=1, length=4)

        if style == "screen":
            kwargs = dict()
            kwargs2 = dict(y=0.7, labelpad=20.0)
        elif style == "slide":
            kwargs = dict(size=36.0)
            kwargs2 = dict(y=0.71, labelpad=45.0, size=30.0)
            for figure in plots:
                figure.tick_params(axis="both", labelsize=34, length=10)
                figure.tick_params(axis="x", pad=10.)
        else:
            raise ValueError("Plotting style unknown!")

        bottom_plot.set_xlabel(r"Time", ha="right", x=1, **kwargs)
        bottom_plot.set_ylabel(r"Jobs|Slots", va="top", **kwargs2)

        return plots

    @staticmethod
    def __get_plot_dict(plot_style):
        """Define plot styles."""
        plot_dict = {
            Data.condor_running: ("Jobs running", "#b8c9ec"),  # light blue
            Data.condor_idle: ("Jobs available", "#fdbe81"),  # light orange
            Data.vm_requested: ("Slots requested", "#fb8a1c"),  # orange
            Data.vm_running: ("Slots available", "#2c7bb6"),  # blue
            Data.vm_draining: ("Slots draining", "#7f69db"),  # light blue
        }
        font = {"family": "sans", "size": 20}
        matplotlib.rc("font", **font)
        matplotlib.rcParams['pdf.fonttype'] = 42
        matplotlib.rcParams['ps.fonttype'] = 42
        if plot_style == "slide":
            matplotlib.rcParams["figure.figsize"] = 18, 8
            matplotlib.rcParams["svg.fonttype"] = "none"
            matplotlib.rcParams["path.simplify"] = True
            matplotlib.rcParams["path.simplify_threshold"] = 0.5
            matplotlib.rcParams["font.sans-serif"] = "Linux Biolinum O"
            matplotlib.rcParams["font.family"] = "sans-serif"
            matplotlib.rcParams["figure.dpi"] = 300
        elif plot_style == "screen":
            pass
        else:
            raise ValueError("Plotting style unknown!")
        return plot_dict

    @staticmethod
    def main(file_list, live, output_name, plot_style, x_limits, cores, interval):
        ###
        # Preparations
        ###
        plot_dict = FrPlotting.__get_plot_dict(plot_style)

        # Get a list of tuples, sorted by time. Format (timestamp, dict[required machines], dict[machines per site])
        logs = {}
        for input_file in file_list:
            if not path.exists(input_file):
                print("%s does not exist" % input_file)
                exit(1)
            elif ".json" not in input_file:
                print("Skipping %s (unknown format)" % input_file)
                continue

            with open(input_file, "r") as json_file:
                logs.update(json.load(json_file))
        ###
        # RRD Processing
        ###
        # Create RRD (overwrites existing file)
        # Identical timestamps are not allowed; substract 1 second from start time.
        rrd = Rrd.create(database_name="/tmp/freiburg_tmp", start=int(min(logs.keys())) - 1)
        rrd.update_from_dict(logs)
        # Grab processed data from rrd
        time_range, keys, data_list = rrd.fetch(commands=("-r", interval,
                                                          "-s", min(logs.keys()), "-e", max(logs.keys())),
                                                function="AVERAGE")
        ###
        # Data processing
        ###
        quantities = {}
        timestamps = np.asarray(time_range, dtype=datetime)
        data_array = np.asarray(data_list, dtype=np.float64)

        for counter, key in enumerate(keys):
            quantities[key] = np.array(data_array[:, counter], dtype=np.float64)
        ###
        # Plotting
        ###
        # stack quantities
        jobs_idle = np.add(quantities[Data.condor_idle], quantities[Data.condor_running])
        jobs_running = quantities[Data.condor_running]
        machines_requested = cores * np.add(quantities[Data.vm_requested],
                                            np.add(quantities[Data.vm_running], quantities[Data.vm_draining]))
        condor_nodes = cores * np.add(quantities[Data.vm_running], quantities[Data.vm_draining])
        condor_nodes_draining = cores * quantities[Data.vm_draining]

        plots = FrPlotting._init_plots(style=plot_style, split=int(np.nanmax(machines_requested)),
                                       max_=int(np.nanmax(jobs_idle)))
        plot_count = len(plots)
        if plot_count == 1:
            plot_count = 2

        for figure in plots[0:plot_count - 1]:
            figure.plot(timestamps, machines_requested, label=plot_dict[Data.vm_requested][0],
                        color=plot_dict[Data.vm_requested][1], linestyle="-", marker="", linewidth=2.0)
            figure.plot(timestamps, condor_nodes, label=plot_dict[Data.vm_running][0],
                        color=plot_dict[Data.vm_running][1], linestyle="-", marker="", linewidth=2.0)
            figure.plot(timestamps, condor_nodes_draining, label=plot_dict[Data.vm_draining][0],
                        color=plot_dict[Data.vm_draining][1], linestyle="-", marker="", linewidth=2.0)

            stack1 = figure.fill_between(timestamps, jobs_idle, facecolor=plot_dict[Data.condor_idle][1],
                                         linewidth=0.0, label=plot_dict[Data.condor_idle][0], interpolate=True)
            stack2 = figure.fill_between(timestamps, jobs_running, facecolor=plot_dict[Data.condor_running][1],
                                         linewidth=0.0, label=plot_dict[Data.condor_running][0], interpolate=True)
            for entry in stack1, stack2:
                figure.plot([], [], color=entry.get_facecolor()[0], linewidth=10, label=entry.get_label())

            if x_limits:
                figure.set_xlim(x_limits[0], x_limits[1])
            else:
                figure.set_xlim(timestamps.min(), timestamps.max())

        FrPlotting._adjust_plot_labels(plot_style, plots)

        ###
        # Output
        ###
        if live:
            mplt.show()
        else:
            if not output_name:
                output_name = path.splitext(file_list[0])[0]
            mplt.savefig(output_name + ".png", bbox_inches="tight")
            mplt.savefig(output_name + ".pdf", bbox_inches="tight")
            mplt.savefig(output_name + ".svg", bbox_inches="tight")
            print("Output written to: %s" % output_name)

    @staticmethod
    def _adjust_plot_labels(plot_style, plots):
        """Plot labels can only be changed after the data has been plotted."""
        # We add multiple instances of line descriptions - get rid of them via OrderedDict
        handles, labels = plots[0].get_legend_handles_labels()
        by_label = OrderedDict(zip(labels, handles))
        if plots[0] is not plots[-1]:
            # If we have subplots, add legend to a separate subplot.
            main_plot = plots[1]
            kwargs = dict(bbox_to_anchor=(0, 0, 1, 1), mode="expand", loc="lower left")
        else:
            main_plot = plots[0]
            kwargs = dict(loc="best")
        if plot_style == "slide":
            kwargs["fontsize"] = 30
        plots[-1].legend(by_label.values(), by_label.keys(), ncol=2, **kwargs)

        major_locator = mdates.AutoDateLocator(minticks=3, maxticks=6)
        formatter = mdates.AutoDateFormatter(major_locator)
        formatter.scaled = {mdates.DAYS_PER_YEAR: '%Y', mdates.DAYS_PER_MONTH: '%b', mdates.DAYS_PER_WEEK: 'CW %V',
                            1.0: '%m-%d', 1. / mdates.HOURS_PER_DAY: '%H:%M:%S',
                            1. / mdates.MINUTES_PER_DAY: '%H:%M:%S.%f'}

        main_plot.xaxis.set_major_locator(major_locator)
        main_plot.xaxis.set_minor_locator(mticker.AutoMinorLocator(n=mdates.DAYS_PER_WEEK))
        main_plot.xaxis.set_major_formatter(formatter)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plotting tool for ROCED status logs.")
    parser.add_argument("file_list", type=str, nargs="+",
                        help="input file list")
    parser.add_argument("-i", "--interval", type=str, default="4h",
                        help="Averaging interval (default: %(default)s)")
    parser.add_argument("-l", "--live", action="store_true",
                        help="plot to screen (default: %(default)s)")
    parser.add_argument("-o", "--output_name", type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument("--cores", type=int, default="4",
                        help="Cores per VM. (default: %(default)s)")
    parser.add_argument("-s", "--plot_style", type=str, default="screen",
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument("-x", "--x-limits", type=float, default=None, nargs=2,
                        help="x-axis limit (lower, upper) (default: %(default)s)")
    FrPlotting.main(**vars(parser.parse_args()))
