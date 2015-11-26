#!/usr/bin/python

# ===============================================================================
#
# Copyright (c) 2010, 2011 by Georg Fleig
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

import json
import argparse

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
    parser.add_argument('-s', '--style', type=str, default='screen',
                        help="output style (screen or print for presentations/poster) (default: %(default)s)")
    parser.add_argument('-x', '--xlim', type=float, default=None, nargs=2,
                        help="x-axis limit (default: %(default)s)")
    args = parser.parse_args()

    if args.style == "screen":
        plot_dict = {

        }

    logs = {}
    for input_file in args.input_files:
        if '.json' in input_file:
            with open(input_file, 'r') as json_file:
                logs.update(json.load(json_file))
    log = sorted(logs.items())

    timestamps, content = zip(*log)
    quantities = []

    for i in xrange(len(content)):
        quantities.append(int(content[i]["Diff."]))

    counts = {}
    for i in xrange(max(quantities)):
        if quantities.count(i) == 0 or i >= 300:
            continue
        counts[str(i)] = quantities.count(i)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.hold(True)

    ax.set_xlabel(r'Time / sec', ha='right', x=1)
    ax.set_ylabel(r'Number of Machines', va='top', y=.7, labelpad=20.0)

    for i in counts:
        plt.plot(str(i), str(counts[i]), 'ro')

    if args.live:
        plt.show()


if __name__ == '__main__':
    main()
