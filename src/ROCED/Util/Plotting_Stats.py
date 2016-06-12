#!/usr/bin/env python
# ===============================================================================
#
# Copyright (c) 2010, 2011, 2016 by Guenther Erli
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

import argparse
import csv
import datetime
import numpy as np
import os

import matplotlib.pyplot as plt

# keywords
old_status = "old_status"
new_status = "new_status"
timestamp = "timestamp"
timediff = "timediff"
site = "site"
mid = "mid"
to = "->"
title = "title"
machines = "machines"

# machine states
statusBooting = "booting"
statusUp = "up"
statusIntegrating = "integrating"
statusWorking = "working"
statusPendingDisintegration = "pending_disintegration"
statusDisintegrating = "disintegrating"
statusDisintegrated = "disintegrated"
statusDown = "down"

# dictionaries to plot
stats_dict = [
    {title: str(statusBooting + to + statusWorking), old_status: statusBooting, new_status: statusWorking},
    {title: str(statusBooting + to + statusDown), old_status: statusBooting, new_status: statusDown}
]
total_stats_dict = [
    {title: str(statusBooting + to + statusDown), old_status: statusBooting, new_status: statusDown},
    {title: str(statusBooting + to + statusWorking), old_status: statusBooting, new_status: statusWorking}
]

time_scales = {"s": ("second", 1), "m": ("minute", 60), "h": ("hour", 60 * 60),
               "d": ("day", 60 * 60 * 24)}


class Stats(object):
    # list to store parser arguments
    args = []

    # dictionary to store sites
    sites = {}

    # dictionaries to store statistics
    stats = {}
    total_stats = {}

    def __init__(self, args):
        self.args = args
        self.load_logs()

    def load_logs(self):
        """Read all csv information into a dictionary."""
        for input_file in self.args.input_files:
            if ".csv" in input_file:
                with open(input_file, "r") as csv_file:
                    reader = csv.DictReader(csv_file)
                    for row in reader:
                        tmp_site = row[site]
                        tmp_mid = row[mid]
                        tmp_old_status = row[old_status]
                        tmp_new_status = row[new_status]
                        tmp_timestamp = row[timestamp]
                        # Don't import empty entries - they are added at ROCED startup.
                        if tmp_old_status == "" or tmp_new_status == "":
                            continue
                        try:
                            self.sites[tmp_site][tmp_mid].append(
                                {old_status: tmp_old_status, new_status: tmp_new_status,
                                 timestamp: datetime.datetime.strptime(tmp_timestamp, "%Y-%m-%d %H:%M:%S.%f")})
                        except KeyError:
                            if tmp_site not in self.sites:
                                self.sites[tmp_site] = {}
                            if tmp_mid not in self.sites[tmp_site]:
                                self.sites[tmp_site][tmp_mid] = []
                            self.sites[tmp_site][tmp_mid].append(
                                {old_status: tmp_old_status, new_status: tmp_new_status,
                                 timestamp: datetime.datetime.strptime(tmp_timestamp, "%Y-%m-%d %H:%M:%S.%f")})

    def calc_stats(self, stat):
        """stat is a certain status transition from stats_dict."""
        for site_ in self.sites:
            for mid_ in self.sites[site_]:
                for status_change in self.sites[site_][mid_]:
                    if status_change[new_status] == stat[old_status]:
                        old_status_timestamp = status_change[timestamp]
                    if status_change[new_status] == stat[new_status]:
                        new_status_timestamp = status_change[timestamp]
                try:
                    if old_status_timestamp and new_status_timestamp:
                        time_diff = (new_status_timestamp - old_status_timestamp).total_seconds()
                    else:
                        time_diff = np.nan
                except UnboundLocalError:
                    # We found a situation that's not defined in stats_dict.
                    continue

                try:
                    self.stats[site_][stat[title]] = np.append(self.stats[site_][stat[title]], [time_diff])
                except KeyError:
                    if site_ not in self.stats:
                        self.stats[site_] = {}
                    if stat[title] not in self.stats[site_]:
                        self.stats[site_][stat[title]] = np.array([time_diff])
                        #        print self.stats

    def calc_total_stats(self, stat):
        for site_ in self.sites:
            min_, max_ = None, None
            for mid_ in self.sites[site_]:
                for status_change in self.sites[site_][mid_]:
                    if status_change[new_status] == stat[old_status]:
                        if min_ is None or status_change[timestamp] < min_:
                            min_ = status_change[timestamp]
                    if status_change[new_status] == stat[new_status]:
                        if max_ is None or status_change[timestamp] > max_:
                            max_ = status_change[timestamp]
            try:
                self.total_stats[site_][stat[title]] = {machines: len(self.sites[site_]),
                                                        timediff: (max_ - min_).total_seconds()}
            except KeyError:
                self.total_stats[site_] = {}
                self.total_stats[site_][stat[title]] = {machines: len(self.sites[site_]),
                                                        timediff: (max_ - min_).total_seconds()}
            except TypeError:
                # max_ or min_ = None
                continue

            fieldnames = [machines, timediff]
            filename = str(site_ + stat[title] + "_total_stats.csv")
            if os.path.isfile(filename):
                stats_file = open(filename, "a")
                writer = csv.DictWriter(stats_file, fieldnames=fieldnames)
            else:
                stats_file = open(filename, "wb")
                writer = csv.DictWriter(stats_file, fieldnames=fieldnames)
                writer.writeheader()
            writer.writerow({machines: self.total_stats[site_][stat[title]][machines],
                             timediff: self.total_stats[site_][stat[title]][timediff]})

    def plot_stats_to_screen(self):
        # prepare plots
        fig = plt.figure()
        n_plots = 0
        for site_ in self.stats:
            n_plots += len(self.stats[site_])
        if n_plots == 0:
            raise ValueError("Nothing to plot")
        y_pos = int(np.floor(np.sqrt(n_plots)))
        x_pos = int(np.ceil(n_plots / float(y_pos)))
        plots = {}
        for i, item in enumerate(range(1, n_plots + 1), 1):
            plots[i] = fig.add_subplot(x_pos, y_pos, i)

        i = 1
        for site_ in self.stats:
            for stat in self.stats[site_]:
                if len(self.stats[site_][stat]) <= 1:
                    continue
                plot = plots[i]
                plot.set_title(stat)

                self.stats[site_][stat] = np.divide(self.stats[site_][stat],
                                                    float(time_scales[self.args.time_scale][1]))
                plot.set_xlabel(r"Time (" + time_scales[self.args.time_scale][0] + ")", ha="right",
                                x=1)
                plot.set_ylabel(r"Number of VMs", va="top", y=.7, labelpad=20.0)

                plot.hist(self.stats[site_][stat])

                i += 1

        plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plotting tool for ROCED status logs.")
    parser.add_argument("input_files", type=str, nargs="+",
                        help="input files")
    parser.add_argument("-l", "--live", action="store_true",
                        help="plot to screen (default: %(default)s)")
    parser.add_argument("-o", "--output", type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument("-t", "--time-scale", type=str, default="s",
                        help="time scale of plot: s(econd), m(inute), h(our), d(ay) (default: %(default)s)")
    parser.add_argument("-s", "--style", type=str, default="screen",
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument("--total-output", type=str,
                        help="output file for total statistics")

    stats = Stats(parser.parse_args())
    for stat in stats_dict:
        stats.calc_stats(stat)
    for stat in total_stats_dict:
        stats.calc_total_stats(stat)
    stats.plot_stats_to_screen()


if __name__ == '__main__':
    try:
        main()
        exit(0)
    except ValueError as err:
        print(err)
        exit(1)
