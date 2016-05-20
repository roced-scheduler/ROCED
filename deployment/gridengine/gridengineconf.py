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
from __future__ import (absolute_import, print_function, unicode_literals)

"""
Simple script to (un)register nodes at the master host and to install/configure
the grid engine software on worker nodes.
Always use the the commands in the following order:
    1. After new machine booted up:
        1. Register node on master host
        2. Add node on worker node
    2. After job is done and before shutting down the vm:
        1. Del node on worker node
        2. Unregister node on master host
"""

import os
import subprocess
import sys
import time

hostsFileName = "/etc/hosts"
gridEngineDir = "/opt/sge6.2u5"
instFile = "sge_inst_template.conf"


def filterColValueMkFunc(colNum, val):
    def filterColValue(line):
        sp = line.split()
        if colNum < len(sp):
            return not sp[colNum] == val
        else:
            return True

    return filterColValue


def removeNode(lines_, node):
    return list(filter(filterColValueMkFunc(1, node), lines_))


def removeIp(lines_, ip_):
    return list(filter(filterColValueMkFunc(0, ip_), lines_))


def addEntry(lines_, nodename_, ip_):
    lines_ += ip_ + "\t" + nodename_ + "\n"
    return lines_


def callQconf(opt, arg):
    ret = subprocess.call(["/opt/sge6.2u5/bin/lx24-amd64/qconf", opt, arg],
                          env={"SGE_ROOT": "/opt/sge6.2u5", "TERM": "xterm-color"})

    if ret == "adminhost %s already exists" % arg:
        print(ret)
    if not ret == 0:
        print("error while adding to gridengine")
        sys.exit(ret)


def callQmod(opt, arg):
    ret = subprocess.call(["/opt/sge6.2u5/bin/lx24-amd64/qmod", opt, "cloud.q@%s" % arg],
                          env={"SGE_ROOT": "/opt/sge6.2u5"})

    if not ret == 0:
        print("error while draining node")
        sys.exit(ret)


def makeDir(dir_):
    ret = subprocess.call(["mkdir", "-p", dir_])

    if not ret == 0:
        print("error while making dir %s" % dir_)
        sys.exit(ret)


def mountNfs(remote_ip, remote_dir, local_dir):
    ps = subprocess.Popen("mount %s:%s %s" % (remote_ip, remote_dir, local_dir), bufsize=0,
                          executable=None, stdin=None, stdout=subprocess.PIPE, shell=True)
    ret = ps.wait()

    if not ret == 0:
        print("error while mounting nfs")
        sys.exit(ret)


def adjustInstFile(in_path, out_path, search, replace):
    inf = open(in_path, "r")
    file_ = inf.read()
    inf.close()

    file_ = file_.replace(search, replace)

    outf = open(out_path, "w")
    outf.write(file_)
    outf.close()


def callInstaller(opt, file_):
    ps = subprocess.Popen("./inst_sge %s -auto %s" % (opt, file_), cwd="/opt/sge6.2u5", bufsize=0,
                          executable=None, stdin=None, stdout=subprocess.PIPE, shell=True)
    ret = ps.wait()

    if not ret == 0:
        print("error while installing execd")
        sys.exit(ret)


def storeHosts(lines_):
    # Assume that change_ip is a function that takes a string and returns a new one with the ip changed): example below
    new_file = open(hostsFileName, "w")
    new_file.writelines(lines_)
    new_file.flush()
    os.fsync(new_file.fileno())
    new_file.close()


def ensureIp(ip_):
    p = subprocess.Popen(["host", ip_], bufsize=0, executable=None, stdin=None,
                         stdout=subprocess.PIPE)
    p.wait()
    hostres = p.stdout.read()
    l = hostres.split()
    if not len(l) == 4:
        return ip_
    else:
        return l[3]


def printUsage():
    print("usage:  gridengineconf register_node <nodename> <nodeip>")
    print("        gridengineconf unregister_node <nodename> <nodeip>")
    print("        gridengineconf add_node <gehostname> <gehostip> <nodename> <nodeip>")
    print("        gridengineconf del_node")
    print("        gridengineconf drain_node <nodename>")
    sys.exit(0)


arguments = ["register_node", "unregister_node", "add_node", "del_node", "get_node_name",
             "drain_node"]

if not (sys.argv[1] in arguments):
    printUsage()
    sys.exit(0)

f = open(hostsFileName, "r")
lines = f.readlines()
f.close()

if sys.argv[1] == "register_node":
    """adding node to /etc/hosts and configuring node as administrative host"""

    if len(sys.argv) != 4:
        printUsage()
        sys.exit(0)

    print("Registering Node on Grid Engine Master Host...")

    ip = sys.argv[3]
    # ip = ensureIp( ip )

    nodename = sys.argv[2]

    lines = removeIp(lines, ip)
    lines = addEntry(lines, nodename, ip)
    storeHosts(lines)

    time.sleep(1)

    callQconf("-ah", nodename)

    print("done")

if sys.argv[1] == "unregister_node":
    """removing node from /etc/hosts. Removing node from administrative hosts list is
    done by the uninstall method in del_node"""

    if len(sys.argv) != 4:
        printUsage()
        sys.exit(0)

    print("Unregistering Node on Grid Engine Master Host...")

    ip = sys.argv[3]
    # ip = ensureIp( ip )

    nodename = sys.argv[2]

    lines = removeIp(lines, ip)
    storeHosts(lines)

    print("done")

if sys.argv[1] == "add_node":
    """Adding master host and node to /etc/hosts and configures/installs execution
    daemon via the auto install. Configs are stored in "sge_girdengine_conf.template"
    and must be adjusted first"""

    if len(sys.argv) != 6:
        printUsage()
        sys.exit(0)

    print("Adding Execution Node...")

    host_ip = sys.argv[3]
    # host_ip = ensureIp( host_ip )

    hostname = sys.argv[2]

    node_ip = sys.argv[5]
    node_ip = ensureIp(node_ip)

    nodename = sys.argv[4]

    lines = removeIp(lines, host_ip)
    lines = addEntry(lines, hostname, host_ip)

    lines = removeIp(lines, node_ip)
    lines = addEntry(lines, nodename, node_ip)

    storeHosts(lines)

    makeDir(gridEngineDir)
    mountNfs(hostname, gridEngineDir, gridEngineDir)

    adjustInstFile("/opt/sge6.2u5/%s" % instFile, "/tmp/%s" % instFile, "NODE_HOSTNAME", nodename)

    callInstaller("-x", "/tmp/%s" % instFile)

    print("done")

if sys.argv[1] == "del_node":
    """Calls the automatic uninstall function of the grid engine installer. Uninstall
    function automatically removes node from administrative hosts list."""

    if len(sys.argv) != 2:
        printUsage()
        sys.exit(0)

    print("Deleting Execution Node...")

    callInstaller("-ux", "/tmp/%s" % instFile)

    print("done")

if sys.argv[1] == "drain_node":

    if len(sys.argv) != 3:
        printUsage()
        sys.exit(0)

    print("Draining Execution Node...")
    callQmod("-d", sys.argv[2])

    print("done")

sys.exit(0)
