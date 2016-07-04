# ===============================================================================
#
# Copyright (c) 2010, 2011 by Thomas Hauth and Stephan Riedel
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
from __future__ import absolute_import, print_function, unicode_literals

import os
import subprocess
import sys

hostsFileName = "/etc/hosts"


def filterColValueMkFunc(colNum, val):
    def filterColValue(line):
        sp = line.split()
        if colNum < len(sp):
            return not sp[colNum] == val
        else:
            return True

    return filterColValue


def removeNode(lines_, node):
    return [x for x in lines_ if filterColValueMkFunc(1, node) is True]


# start the first job in the q to get the scheduler running with the new nodes
def pokeQueue():
    pipe = subprocess.Popen("sleep 5s && qrun $(qstat | grep 'Q batch' | awk '{print $1}' "
                            "| head -n1 | cut -d '.' -f 1)", bufsize=0, executable=None,
                            stdin=None, stdout=subprocess.PIPE, shell=True)
    pipe.wait()
    print(pipe.stdout.read())


def removeIp(lines_, ip_):
    return [x for x in lines_ if filterColValueMkFunc(1, ip_) is True]


def addEntry(lines_, nodename_, ip_):
    lines_ += "%s %s\n" % (ip_, nodename_)
    return lines_


def callQmgr(cmd):
    ret = subprocess.call(["qmgr", "-c", cmd])

    if not ret == 0:
        print("error while adding to torque")
        exit(ret)


def storeHosts(lines_):
    # Assume that change_ip is a function that takes a string and returns a new one
    # with the ip changed): example below
    new_file = open(hostsFileName, "w")
    new_file.writelines(lines_)
    new_file.flush()
    os.fsync(new_file.fileno())
    new_file.close()


def ensureIp(ip_):
    pipe = subprocess.Popen(["host", ip_], bufsize=0, executable=None, stdin=None,
                            stdout=subprocess.PIPE)
    pipe.wait()
    hostres = pipe.stdout.read()
    l = hostres.split()
    if not len(l) == 4:
        return ip_
    else:
        return l[3]


if len(sys.argv) < 3:
    print("usage:  torqconf add_node <nodename> <nodeip>")
    print("        torqconf del_node <nodename>")
    print("        torqconf get_node_name <nodeip>")
    exit(0)

f = open(hostsFileName, "r")
lines = f.readlines()
f.close()

if sys.argv[1] == "add_node":
    print("Adding Node...")
    ip = sys.argv[3]
    ip = ensureIp(ip)

    nodename = sys.argv[2]

    lines = removeIp(lines, ip)
    lines = addEntry(lines, nodename, ip)
    storeHosts(lines)

    callQmgr("create node %s" % nodename)
    pokeQueue()

    print("done")

if sys.argv[1] == "get_node_name":
    nodeip = ensureIp(sys.argv[2])
    p = subprocess.Popen("cat /etc/hosts | grep %s | awk '{print $2}'" % nodeip, bufsize=0,
                         executable=None, stdin=None, stdout=subprocess.PIPE, shell=True)
    p.wait()
    print(p.stdout.read())

if sys.argv[1] == "del_node":
    print("Deleting Node...")
    nodename = sys.argv[2]
    callQmgr("delete node %s" % nodename)
    lines = removeNode(lines, nodename)
    storeHosts(lines)

    print("done")

exit(0)
