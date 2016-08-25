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
from __future__ import unicode_literals, absolute_import

import logging
from xml.dom import minidom

from Core import MachineRegistry, Config
from IntegrationAdapter.Integration import IntegrationAdapterBase
from Util import ScaleTools


class GridEngineIntegrationAdapter(IntegrationAdapterBase):
    reg_gridengine_node_name = "gridengine_node_name"
    reg_gridengine_node_ip = "gridengine_node_ip"

    ConfigGridEngineIp = "ge_ip"
    ConfigGridEngineHostname = "ge_host"
    ConfigGridEngineKey = "ge_key"

    def __init__(self):
        super(GridEngineIntegrationAdapter, self).__init__()

        self.addCompulsoryConfigKeys(self.ConfigGridEngineIp, Config.ConfigTypeString)
        self.addCompulsoryConfigKeys(self.ConfigGridEngineHostname, Config.ConfigTypeString)
        self.addCompulsoryConfigKeys(self.ConfigGridEngineKey, Config.ConfigTypeString)

    def init(self):
        super(GridEngineIntegrationAdapter, self).init()
        self.mr.registerListener(self)

    def manage(self, cleanup=False):
        """called every manage cycle"""

        # only nodes which are set to drain/delete are in this list
        disint = self.mr.getMachines(status=self.mr.statusDisintegrating)

        if len(disint) != 0:
            for mid, machine in disint:

                machine_name = machine[self.reg_gridengine_node_name]
                command = ("/opt/sge6.2u5/bin/lx24-x86/qstat -f -q cloud.q@%s -u \"*\" -xml"
                           % machine_name)
                environment = {"SGE_ROOT": "/opt/sge6.2u5"}

                (res_xml, stdout, stderr) = ScaleTools.Shell.executeCommand(command, environment)

                if res_xml == 0:  # Shell command successful
                    xmldoc = minidom.parseString(stdout)
                    slots_used = int(xmldoc.getElementsByTagName("slots_used")[0].firstChild.data)
                    slots_reserved = int(
                        xmldoc.getElementsByTagName("slots_resv")[0].firstChild.data)

                    if (slots_used == 0) and (slots_reserved == 0):  # node has all jobs finished
                        self.disintegrateNode(mid)
                        self.disintegrateWithGridEngine(mid)

                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

    def drainNode(self, machine_id):

        paramList = self.mr.machines[machine_id][self.reg_gridengine_node_name]

        return self.runCommandOnGridEngineServer("python gridengineconf.py drain_node %s" % paramList)

    def integrateNode(self, machine_id):

        ssh = ScaleTools.Ssh.getSshOnMachine(self.mr.machines[machine_id])

        paramList = (" ".join([self.getConfig(self.ConfigGridEngineHostname),
                               self.getConfig(self.ConfigGridEngineIp),
                               self.mr.machines[machine_id][self.reg_gridengine_node_name],
                               self.mr.machines[machine_id][self.mr.regVpnIp]]))

        ssh.copyToRemote("gridengineconf.py")

        return ssh.handleSshCall("python gridengineconf.py add_node %s" % paramList)

    def disintegrateNode(self, machine_id):

        ssh = ScaleTools.Ssh.getSshOnMachine(self.mr.machines[machine_id])

        return ssh.handleSshCall("python gridengineconf.py del_node")

    def integrateWithGridEngine(self, machine_id):

        paramList = ("%s %s" % (self.mr.machines[machine_id][self.reg_gridengine_node_name],
                                self.mr.machines[machine_id][self.mr.regVpnIp]))

        return self.runCommandOnGridEngineServer("python gridengineconf.py register_node %s" % paramList)

    def disintegrateWithGridEngine(self, machine_id):

        paramList = ("%s %s" % (self.mr.machines[machine_id][self.reg_gridengine_node_name],
                                self.mr.machines[machine_id][self.mr.regVpnIp]))

        return self.runCommandOnGridEngineServer("python gridengineconf.py unregister_node %s" % paramList)

    def runCommandOnGridEngineServer(self, cmd):

        ssh = ScaleTools.Ssh(self.getConfig(self.ConfigGridEngineIp), "root",
                             self.getConfig(self.ConfigGridEngineKey), None, 1)

        ssh.copyToRemote("gridengineconf.py")

        return ssh.handleSshCall(cmd)

    def onEvent(self, evt):

        if isinstance(evt, MachineRegistry.StatusChangedEvent):

            if evt.newStatus == self.mr.statusUp:
                logging.info("Integrating machine with ip %s"
                             % self.mr.machines[evt.id].get(self.mr.regHostname))

                self.mr.updateMachineStatus(evt.id, self.mr.statusIntegrating)

                res_ge = self.integrateWithGridEngine(evt.id)
                res_node = self.integrateNode(evt.id)

                if res_ge[0] == 0 and res_node[0] == 0:  # check if ssh commands were successful
                    self.mr.updateMachineStatus(evt.id, self.mr.statusWorking)

            if evt.newStatus == self.mr.statusPendingDisintegration:
                logging.info("Disintegrating machine with ip %s"
                             % self.mr.machines[evt.id].get(self.mr.regHostname))

                res = self.drainNode(
                    evt.id)  # delete/drain node so that no new jobs are executed on the node

                if res[0] == 0:  # check if ssh command was successful
                    self.mr.updateMachineStatus(evt.id, self.mr.statusDisintegrating)

                logging.info("Draining node %s"
                             % self.mr.machines[evt.id][self.reg_gridengine_node_name])

    @property
    def description(self):
        return "GridEngineIntegrationAdapter"
