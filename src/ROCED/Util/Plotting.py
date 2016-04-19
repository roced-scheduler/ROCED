#!/usr/bin/env python
# ===============================================================================
#
# Copyright (c) 2015, 2016 by Guenther Erli
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
from __future__ import print_function, unicode_literals

"""
Plots useful information from HTCondorRequirementAdapter and OpenStackSiteAdapter JSON outputs.
"""

import argparse
import json
import numpy as np
import pandas as pd
import sys
from os import path

import matplotlib
import matplotlib.pyplot as plt

try:
    pass
    # import seaborn as sns
    # sns.set_style("ticks")
    # sns.despine()
    # sns.set_context("poster")
except ImportError:
    pass

# defining some machine specific settings
machine_settings = {
    "gridka": {
        "label": "GridKa",
        "vcpu": 4.0
    },
    "ekpcloud": {
        "label": "EKPCloud",
        "vcpu": 2.0
    },
    "oneandone": {
        "label": "OneAndOne",
        "vcpu": 4.0
    }
}

pair = "pair"
vcpus = "vcpu"
logs = {"GridKa": {pair: ["gridka", "gridka-default"], vcpus: 4},
        "EKPCloud": {pair: ["ekpcloud", "ekpcloud-default"], vcpus: 2},
        "OneAndOne": {pair: ["oneandone", "oneandone-default"], vcpus: 2}}


def load_style(style):
    # set dictionary for labels and colors, depending on style setting
    if style == "screen":
        plot_dict = pd.DataFrame({
            "jobs_running": ("HTCondor: jobs running", "#b8c9ec"),  # light blue
            "jobs_idle": ("HTCondor: jobs waiting", "#fdbe81"),  # light orange
            "machines_requested": ("ROCED: VMs requested x2", "#fb8a1c"),  # orange
            "condor_nodes": ("HTCondor: nodes available x2", "#2c7bb6"),  # blue
            "condor_nodes_draining": ("HTCondor: nodes draining x2", "#7f69db")  # light blue
        })
    elif style == "slide":
        plot_dict = pd.DataFrame({
            "jobs_running": ("Jobs running", "#b8c9ec"),  # light blue
            "jobs_idle": ("Jobs waiting", "#fdbe81"),  # light orange
            "machines_requested": ("VMs requested", "#fb8a1c"),  # orange
            "condor_nodes": ("VMs available", "#2c7bb6"),  # blue
            "condor_nodes_draining": ("VMs draining", "#7f69db")  # light blue
        })
        matplotlib.rcParams["svg.fonttype"] = "none"
        matplotlib.rcParams["path.simplify"] = True
        matplotlib.rcParams["path.simplify_threshold"] = 0.5
        matplotlib.rcParams["font.sans-serif"] = "Linux Biolinum O"
        matplotlib.rcParams["font.family"] = "sans-serif"
    else:
        print("Error: plotting style unknown!")
        return sys.exit(1)

    plot_dict = plot_dict.T
    plot_dict.columns = ["plot_name", "color"]
    return plot_dict


def load_log(input_files, correction_period, plot_dict):
    tmp = {}
    for input_file in input_files:
        if ".json" in input_file:
            with open(input_file, "r") as json_file:
                tmp.update(json.load(json_file))

    logs_ = {}
    for timestamp in tmp:
        for site in tmp[timestamp]:
            values = np.array([])
            columns = np.array([])
            values = np.append(values, int(timestamp))
            columns = np.append(columns, "timestamps")

            for key in plot_dict.index:
                if key not in tmp[timestamp][site]:
                    values = np.append(values, np.NaN)
                elif key == "jobs_idle":
                    values = np.append(values,
                                       int(tmp[timestamp][site][key]) + int(
                                           tmp[timestamp][site]["jobs_running"]))
                else:
                    values = np.append(values, int(tmp[timestamp][site][key]))
                columns = np.append(columns, key)

            if site in logs_:
                tmp_pd = pd.DataFrame([values], columns=columns)
                logs_[site] = logs_[site].append(tmp_pd, ignore_index=True)
            else:
                logs_[site] = pd.DataFrame([values], columns=columns)

    for site in logs_:
        logs_[site] = logs_[site].sort("timestamps")
        logs_[site].index = logs_[site].sort_index().index

    return logs_


def correct_data(logs_, correction):
    for site in logs_:
        runtimes = np.array(logs_[site]["timestamps"]) - logs_[site]["timestamps"][0]
        logs_[site]["runtimes"] = runtimes

        indices = np.nonzero(np.diff(runtimes) > correction)
        if len(indices[0]) > 0:
            print(
                "Ignoring " + str(len(indices[0])) + " periods with no log entries for over " + str(
                    correction) + " seconds:")
            for index in indices[0]:
                print("Begin: " + str(logs_[site]["runtimes"][index]) + "s, End: " + str(
                    logs_[site]["runtimes"][index + 1]) + "s, Diff: " + str(
                    logs_[site]["runtimes"][index + 1] - logs_[site]["runtimes"][index]) + "s")
                # add two entries to time axis
                tmp1 = pd.DataFrame([np.array([np.NaN for i in range(len(logs_[site].columns))])],
                                    columns=logs_[site].columns)
                tmp1["timestamps"] = np.array([logs_[site]["timestamps"][index] + 1])
                tmp1["runtimes"] = np.array([logs_[site]["runtimes"][index] + 1])
                tmp2 = pd.DataFrame([np.array([np.NaN for i in range(len(logs_[site].columns))])],
                                    columns=logs_[site].columns)
                tmp2["timestamps"] = np.array([logs_[site]["timestamps"][index + 1] - 1])
                tmp2["runtimes"] = np.array([logs_[site]["runtimes"][index + 1] - 1])
                logs_[site] = logs_[site].append(tmp1.append(tmp2, ignore_index=True),
                                                 ignore_index=True)

        logs_[site] = logs_[site].sort("timestamps")
        logs_[site].index = logs_[site].sort_index().index

    return logs_


def correct_quantities(logs_):
    # multiply by number of cores
    for site in logs_:
        logs_[site]["machines_requested"] = machine_settings[site]["vcpu"] * np.add(
            logs_[site]["machines_requested"],
            np.add(logs_[site]["condor_nodes"],
                   logs_[site][
                       "condor_nodes_draining"]))
        logs_[site]["condor_nodes"] = machine_settings[site]["vcpu"] * np.add(
            logs_[site]["condor_nodes"],
            logs_[site]["condor_nodes_draining"])
        logs_[site]["condor_nodes_draining"] = machine_settings[site]["vcpu"] * logs_[site][
            "condor_nodes_draining"]

    return logs_


def plot_to_screen(logs_, plot_dict, style, time_scale):
    # prepare plots
    fig = plt.figure()
    n_plots = len(logs_)
    y_pos = int(np.floor(np.sqrt(n_plots)))
    x_pos = int(np.ceil(n_plots / float(y_pos)))
    plots = {}
    for i, item in enumerate(range(1, n_plots + 1), 1):
        plots[i] = fig.add_subplot(x_pos, y_pos, i)

    time_scales = {"s": ("second", 1), "m": ("minute", 60), "h": ("hour", 60 * 60),
                   "d": ("day", 60 * 60 * 24)}

    i = 1
    if style == "screen":
        for site in logs_:
            plot = plots[i]
            plot.set_title("Resource allocation over time @ " + machine_settings[site][
                "label"])  # , x=0.5, y=0.88)

            logs_[site]["runtimes"] /= float(time_scales[time_scale][1])
            plot.set_xlabel(r"Time (" + time_scales[time_scale][0] + ")", ha="right", x=1)
            plot.set_ylabel(r"Number of Jobs & VM cores", va="top", y=.7, labelpad=20.0)

            stack1 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_idle"],
                                       facecolor=plot_dict["color"]["jobs_idle"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_idle"])
            stack2 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_running"],
                                       facecolor=plot_dict["color"]["jobs_running"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_running"])

            for entry in stack1, stack2:
                plot.plot([], [], color=entry.get_facecolor()[0], linewidth=10,
                          label=entry.get_label())
            plot.plot(logs_[site]["runtimes"], logs_[site]["machines_requested"],
                      label=plot_dict["plot_name"]["machines_requested"],
                      color=plot_dict["color"]["machines_requested"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes"],
                      label=plot_dict["plot_name"]["condor_nodes"],
                      color=plot_dict["color"]["condor_nodes"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes_draining"],
                      label=plot_dict["plot_name"]["condor_nodes_draining"],
                      color=plot_dict["color"]["condor_nodes_draining"], linestyle="-", marker="",
                      linewidth=2.0)

            # legend settings and plot output
            plot.legend(loc="upper right", numpoints=1, frameon=False)
            plot.set_ylim([0, 1.1 * np.amax(logs_[site]["jobs_idle"])])
            plot.set_xlim(
                [np.amin(logs_[site]["runtimes"]), 1.05 * np.amax(logs_[site]["runtimes"])])
            i += 1

    elif style == "slide":
        for site in logs_:
            plot = plots[i]
            plot.set_title("Resource allocation over time @ " + machine_settings[site]["label"],
                           size=24)

            logs_[site]["runtimes"] /= float(time_scales[time_scale][1])
            plot.set_xlabel(r"Time (" + time_scales[time_scale][0] + ")", ha="right", x=1, size=24)
            plot.set_ylabel(r"Number of Jobs & VM cores", va="top", y=.71, labelpad=37.0, size=24)
            plot.tick_params(axis="x", labelsize=20, pad=10., length=10)
            plot.tick_params(axis="y", labelsize=20, length=10)

            plot.plot(logs_[site]["runtimes"], logs_[site]["machines_requested"],
                      label=plot_dict["plot_name"]["machines_requested"],
                      color=plot_dict["color"]["machines_requested"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes"],
                      label=plot_dict["plot_name"]["condor_nodes"],
                      color=plot_dict["color"]["condor_nodes"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes_draining"],
                      label=plot_dict["plot_name"]["condor_nodes_draining"],
                      color=plot_dict["color"]["condor_nodes_draining"], linestyle="-", marker="",
                      linewidth=2.0)
            stack1 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_idle"],
                                       facecolor=plot_dict["color"]["jobs_idle"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_idle"])
            stack2 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_running"],
                                       facecolor=plot_dict["color"]["jobs_running"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_running"])
            for entry in stack1, stack2:
                plot.plot([], [], color=entry.get_facecolor()[0], linewidth=10,
                          label=entry.get_label())

            # legend settings and plot output
            plot.legend(loc="upper left", numpoints=1, frameon=False, fontsize=24, ncol=2)
            plot.set_xlim(
                [np.amin(logs_[site]["runtimes"]), 1.05 * np.amax(logs_[site]["runtimes"])])
            plot.set_ylim([0, 1.1 * np.amax(logs_[site]["jobs_idle"])])
            i += 1

    plt.show()
    return


def plot_to_file(logs_, plot_dict, style, time_scale, output, proportion, resolution):
    time_scales = {"s": ("second", 1), "m": ("minute", 60), "h": ("hour", 60 * 60),
                   "d": ("day", 60 * 60 * 24)}

    if style == "screen":
        for site in logs_:
            fig = plt.figure(num=None, figsize=(proportion[0], proportion[1]), dpi=360,
                             facecolor='w', edgecolor='k')
            plot = fig.add_subplot(1, 1, 1)
            # plot.hold(True)
            plot.set_title("Resource allocation over time @ " + machine_settings[site]["label"],
                           size=24)  # , x=0.5, y=0.88)

            logs_[site]["runtimes"] /= float(time_scales[time_scale][1])
            plot.set_xlabel(r"Time / " + time_scales[time_scale][0], ha="right", x=1, size=24)
            plot.set_ylabel(r"Number of Jobs/VMs", va="top", y=.7, labelpad=20.0, size=24)
            plot.tick_params(axis="x", labelsize=16, pad=10., length=10)
            plot.tick_params(axis="y", labelsize=16, pad=11., length=10)

            stack1 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_idle"],
                                       facecolor=plot_dict["color"]["jobs_idle"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_idle"])
            stack2 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_running"],
                                       facecolor=plot_dict["color"]["jobs_running"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_running"])

            for entry in stack1, stack2:
                plot.plot([], [], color=entry.get_facecolor()[0], linewidth=10,
                          label=entry.get_label())
            plot.plot(logs_[site]["runtimes"], logs_[site]["machines_requested"],
                      label=plot_dict["plot_name"]["machines_requested"],
                      color=plot_dict["color"]["machines_requested"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes"],
                      label=plot_dict["plot_name"]["condor_nodes"],
                      color=plot_dict["color"]["condor_nodes"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes_draining"],
                      label=plot_dict["plot_name"]["condor_nodes_draining"],
                      color=plot_dict["color"]["condor_nodes_draining"], linestyle="-", marker="",
                      linewidth=2.0)

            # legend settings and plot output
            plot.legend(loc="upper right", numpoints=1, frameon=False)
            plot.set_xlim(
                [np.amin(logs_[site]["runtimes"]), 1.05 * np.amax(logs_[site]["runtimes"])])
            plot.set_ylim([0, 1.1 * np.amax(logs_[site]["jobs_idle"])])

            plt.savefig(output + "_" + machine_settings[site]["label"] + ".png",
                        bbox_inches="tight")
            plt.savefig(output + "_" + machine_settings[site]["label"] + ".pdf",
                        bbox_inches="tight")
            plt.savefig(output + "_" + machine_settings[site]["label"] + ".svg",
                        bbox_inches="tight")
            print("Output written to: " + output + "_" + machine_settings[site]["label"])
            fig.delaxes(plot)

    elif style == "slide":
        for site in logs_:
            fig = plt.figure(num=None, figsize=(proportion[0], proportion[1]), dpi=360,
                             facecolor='w', edgecolor='k')
            plot = fig.add_subplot(1, 1, 1)
            plot.set_title("Resource allocation over time @ " + machine_settings[site]["label"],
                           size=24)

            logs_[site]["runtimes"] /= float(time_scales[time_scale][1])
            plot.set_xlabel(r"Time / " + time_scales[time_scale][0], ha="right", x=1, size=24)
            plot.set_ylabel(r"Number of Jobs/VMs", va="top", y=.71, labelpad=37.0, size=24)
            plot.tick_params(axis="x", labelsize=20, pad=10., length=10)
            plot.tick_params(axis="y", labelsize=20, pad=11., length=10)

            plot.plot(logs_[site]["runtimes"], logs_[site]["machines_requested"],
                      label=plot_dict["plot_name"]["machines_requested"],
                      color=plot_dict["color"]["machines_requested"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes"],
                      label=plot_dict["plot_name"]["condor_nodes"],
                      color=plot_dict["color"]["condor_nodes"], linestyle="-", marker="",
                      linewidth=2.0)
            plot.plot(logs_[site]["runtimes"], logs_[site]["condor_nodes_draining"],
                      label=plot_dict["plot_name"]["condor_nodes_draining"],
                      color=plot_dict["color"]["condor_nodes_draining"], linestyle="-", marker="",
                      linewidth=2.0)
            stack1 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_idle"],
                                       facecolor=plot_dict["color"]["jobs_idle"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_idle"])
            stack2 = plot.fill_between(logs_[site]["runtimes"], logs_[site]["jobs_running"],
                                       facecolor=plot_dict["color"]["jobs_running"], color=None,
                                       edgecolor=None,
                                       linewidth=0.0, label=plot_dict["plot_name"]["jobs_running"])
            for entry in stack1, stack2:
                plot.plot([], [], color=entry.get_facecolor()[0], linewidth=10,
                          label=entry.get_label())

            # legend settings and plot output
            plot.legend(loc="upper left", numpoints=1, frameon=False, fontsize=24, ncol=2)
            plot.set_xlim(
                [np.amin(logs_[site]["runtimes"]), 1.05 * np.amax(logs_[site]["runtimes"])])
            plot.set_ylim([0, 1.1 * np.amax(logs_[site]["jobs_idle"])])

            plt.savefig(output + "_" + machine_settings[site]["label"] + ".png",
                        bbox_inches="tight")
            plt.savefig(output + "_" + machine_settings[site]["label"] + ".pdf",
                        bbox_inches="tight")
            plt.savefig(output + "_" + machine_settings[site]["label"] + ".svg",
                        bbox_inches="tight")
            print("Output written to: " + output + "_" + machine_settings[site]["label"])

    return


def main():
    parser = argparse.ArgumentParser(description="Plotting tool for ROCED status logs.")
    parser.add_argument("input_files", type=str, nargs="+",
                        help="input files")
    parser.add_argument("-l", "--live", action="store_true",
                        help="plot to screen (default: %(default)s)")
    parser.add_argument("-o", "--output", type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument("--correction-period", type=int, default=600,
                        help="time in seconds for periods without log entries to be ignored, 0 disables correction (default: %(default)s)")
    parser.add_argument("-t", "--time-scale", type=str, default="m",
                        help="time scale of plot: s(econd), m(inute), h(our), d(ay) (default: %(default)s)")
    parser.add_argument("-s", "--style", type=str, default="screen",
                        help="output style (screen or print(for presentations/poster) (default: %(default)s)")
    parser.add_argument("-x", "--xlim", type=float, default=None, nargs=2,
                        help="x-axis limit (lower, upper) (default: %(default)s)")
    parser.add_argument("-p", "--proportion", type=int, default=[21, 9], nargs=2,
                        help="define proportion of output files (default: %(default)s)")
    parser.add_argument("-r", "--resolution", type=int, default=100,
                        help="resolution of output files (default: %(default)s)")
    args = parser.parse_args()

    plot_dict = load_style(args.style)
    logs_ = load_log(args.input_files, args.correction_period, plot_dict)

    if args.correction_period > 0:
        logs_ = correct_data(logs_, args.correction_period)

    logs_ = correct_quantities(logs_)

    if args.xlim:
        plt.xlim(xmin=args.xlim[0], xmax=args.xlim[1])

    if args.live:
        plot_to_screen(logs_, plot_dict, args.style, args.time_scale)
    else:
        if not args.output:
            args.output = path.splitext(args.input_files[0])[0]
        plot_to_file(logs_, plot_dict, args.style, args.time_scale, args.output, args.proportion,
                     args.resolution)

    return


if __name__ == "__main__":
    main()
