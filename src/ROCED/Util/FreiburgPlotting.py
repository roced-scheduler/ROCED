#!/usr/bin/env python
# ===============================================================================
#
# Copyright (c) 2015 by Georg Fleig
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
from __future__ import print_function

import json
import argparse
from os import path
import sys
import numpy as np

import matplotlib
import matplotlib.pyplot as plt

font = {'family': 'sans',
        'size': 20}
matplotlib.rc('font', **font)
matplotlib.rcParams['figure.figsize'] = 18, 8


def main():
    parser = argparse.ArgumentParser(description='Plotting tool for ROCED status logs.')
    parser.add_argument('input_files', type=str, nargs='+',
                        help="input files")
    parser.add_argument('-l', '--live', action='store_true',
                        help="plot to screen (default: %(default)s)")
    parser.add_argument('-o', '--output', type=str,
                        help="output file (extension will be added automatically) (default: same name as input file)")
    parser.add_argument('--correction-period', type=int, default=600,
                        help="time in seconds for periods without log entries to be ignored, 0 disables correction (default: %(default)s)")
    parser.add_argument('-t', '--time-scale', type=str, default='m',
                        help="time scale of plot: s(econd), m(inute), h(our), d(ay) (default: %(default)s)")
    parser.add_argument('-s', '--style', type=str, default='fr-screen',
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument('-x', '--xlim', type=float, default=None, nargs=2,
                        help="x-axis limit (lower, upper) (default: %(default)s)")
    args = parser.parse_args()

    # set dictionary for labels and colors, depending on style setting
    if args.style == 'fr-screen':
        plot_dict = {
            'jobs_running': ('HTCondor: jobs running', '#b8c9ec'),  # light blue
            'jobs_idle': ('HTCondor: jobs waiting', '#fdbe81'),  # light orange
            'machines_requested': ('ROCED: VMs requested x4', '#fb8a1c'),  # orange
            'condor_nodes': ('HTCondor: nodes available x4', '#2c7bb6'),  # blue
            'condor_nodes_draining': ('HTCondor: nodes draining x4', '#7f69db'),  # light blue
        }
    elif args.style == 'fr-slide':
        plot_dict = {
            'jobs_running': ('Jobs running', '#b8c9ec'),  # light blue
            'jobs_idle': ('Jobs waiting', '#fdbe81'),  # light orange
            'machines_requested': ('VMs requested', '#fb8a1c'),  # orange
            'condor_nodes': ('VMs available', '#2c7bb6'),  # blue
            'condor_nodes_draining': ('VMs draining', '#7f69db'),  # light blue
        }
        matplotlib.rcParams['svg.fonttype'] = 'none'
        matplotlib.rcParams['path.simplify'] = True
        matplotlib.rcParams['path.simplify_threshold'] = 0.5
        matplotlib.rcParams['font.sans-serif'] = 'Linux Biolinum O'
        matplotlib.rcParams['font.family'] = 'sans-serif'
    else:
        print('Error: plotting style unknown!')
        sys.exit(1)

    # get log files and sort entries, result is a tuple
    logs = {}
    for input_file in args.input_files:
        if '.json' in input_file:
            with open(input_file, 'r') as json_file:
                logs.update(json.load(json_file))
    log = sorted(logs.items())

    # convert timestamps to relative times and store quantities in separate lists
    timestamps, content = zip(*log)
    rel_times = np.array([(int(timestamp) - int(timestamps[0])) for timestamp in timestamps])
    content = list(content)

    # find long periods between two log entries and create two new entries per period with 0 data to correct the plot
    if args.correction_period > 0:
        rel_time_diffs = np.diff(rel_times)
        # get all indices of timestamps with a time difference greater than requested
        indices = np.nonzero(rel_time_diffs > args.correction_period)
        print('Ignoring ' + str(len(indices[0])) + ' periods with no log entries for over ' + str(
            args.correction_period) + ' seconds:')
        index_offset = 0
        for index in indices[0]:
            print('Begin: ' + str(rel_times[index + index_offset]) + 's, End: ' + str(
                  rel_times[index + 1 + index_offset]) + 's, Diff: ' +
                  str(rel_times[index + 1 + index_offset] - rel_times[index + index_offset]) + 's')
            # add two entries to time axis
            rel_times = np.insert(rel_times, index + index_offset + 1, rel_times[index + index_offset] + 1)
            rel_times = np.insert(rel_times, index + index_offset + 2, rel_times[index + index_offset + 2] - 1)
            # add two entries to y axis containing nothing
            content.insert(index + index_offset, None)
            content.insert(index + index_offset + 1, None)
            index_offset += 2

    time_scales = {'s': ('second', 1), 'm': ('minute', 60), 'h': ('hour', 60 * 60), 'd': ('day', 60 * 60 * 24)}
    rel_times /= float(time_scales.get(args.time_scale, 'm')[1])

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
                        if quantity == 'condor_nodes_draining':
                            quantities[quantity][i_entry] = 0
                        else:
                            quantities[quantity][i_entry] = np.NaN
                            # print "Missing information: " + str(timestamps[i_entry]) + " " + str(datetime.fromtimestamp(float(timestamps[i_entry])))
        i_entry += 1

    # prepare plot
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.hold(True)

    # build up quantities (stack them, ..)
    jobs_idle = np.add(quantities['jobs_idle'], quantities['jobs_running'])
    jobs_running = quantities['jobs_running']
    machines_requested = 4 * np.add(quantities['machines_requested'],
                                    np.add(quantities['condor_nodes'], quantities['condor_nodes_draining']))
    condor_nodes = 4 * np.add(quantities['condor_nodes'], quantities['condor_nodes_draining'])
    condor_nodes_draining = 4 * quantities['condor_nodes_draining']

    if args.style == 'fr-screen':
        ax.set_xlabel(r'Time / ' + time_scales.get(args.time_scale, 'm')[0], ha='right', x=1)
        ax.set_ylabel(r'Number of Jobs/VMs', va='top', y=.7, labelpad=20.0)

        stack1 = plt.fill_between(rel_times, jobs_idle, facecolor=plot_dict['jobs_idle'][1], color=None, edgecolor=None,
                                  linewidth=0.0, label=plot_dict['jobs_idle'][0])
        stack2 = plt.fill_between(rel_times, jobs_running, facecolor=plot_dict['jobs_running'][1], color=None,
                                  edgecolor=None, linewidth=0.0, label=plot_dict['jobs_running'][0])
        for entry in stack1, stack2:
            plt.plot([], [], color=entry.get_facecolor()[0], linewidth=10, label=entry.get_label())
        plt.plot(rel_times, machines_requested, label=plot_dict['machines_requested'][0],
                 color=plot_dict['machines_requested'][1], linestyle='-', marker='', linewidth=2.0)
        plt.plot(rel_times, condor_nodes, label=plot_dict['condor_nodes'][0], color=plot_dict['condor_nodes'][1],
                 linestyle='-', marker='', linewidth=2.0)
        plt.plot(rel_times, condor_nodes_draining, label=plot_dict['condor_nodes_draining'][0],
                 color=plot_dict['condor_nodes_draining'][1], linestyle='-', marker='', linewidth=2.0)

        # legend settings and plot output
        plt.legend(loc='upper right', numpoints=1, frameon=False)

    elif args.style == 'fr-slide':
        ax.set_xlabel(r'Time / ' + time_scales.get(args.time_scale, 'm')[0], ha='right', x=1, size=36.0)
        ax.set_ylabel(r'Number of Jobs/VMs', va='top', y=.71, labelpad=37.0, size=33.0)
        ax.tick_params(axis='x', labelsize=34, pad=10., length=10)
        ax.tick_params(axis='y', labelsize=34, length=10)

        plt.plot(rel_times, machines_requested, label=plot_dict['machines_requested'][0],
                 color=plot_dict['machines_requested'][1], linestyle='-', marker='', linewidth=2.0)
        plt.plot(rel_times, condor_nodes, label=plot_dict['condor_nodes'][0], color=plot_dict['condor_nodes'][1],
                 linestyle='-', marker='', linewidth=2.0)
        plt.plot(rel_times, condor_nodes_draining, label=plot_dict['condor_nodes_draining'][0],
                 color=plot_dict['condor_nodes_draining'][1], linestyle='-', marker='', linewidth=2.0)
        stack1 = plt.fill_between(rel_times, jobs_idle, facecolor=plot_dict['jobs_idle'][1], color=None, edgecolor=None,
                                  linewidth=0.0, label=plot_dict['jobs_idle'][0])
        stack2 = plt.fill_between(rel_times, jobs_running, facecolor=plot_dict['jobs_running'][1], color=None,
                                  edgecolor=None, linewidth=0.0, label=plot_dict['jobs_running'][0])
        for entry in stack1, stack2:
            plt.plot([], [], color=entry.get_facecolor()[0], linewidth=10, label=entry.get_label())

        # legend settings and plot output
        plt.legend(loc='upper left', numpoints=1, frameon=False, fontsize=30, ncol=2)

    if args.xlim:
        plt.xlim(xmin=args.xlim[0], xmax=args.xlim[1])
    # plt.ylim(ymin=0, ymax=1250)
    if args.live:
        plt.show()
    else:
        if not args.output:
            args.output = path.splitext(args.input_files[0])[0]
        plt.savefig(args.output + '.png', bbox_inches='tight')
        plt.savefig(args.output + '.pdf', bbox_inches='tight')
        plt.savefig(args.output + '.svg', bbox_inches='tight')
        print('Output written to: ' + args.output)


if __name__ == '__main__':
    main()
