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

import datetime
import logging
import random

from Core import MachineRegistry
from Site import SiteAdapterBase


class FakeSiteAdapter(SiteAdapterBase):
    def __init__(self):
        super(FakeSiteAdapter, self).__init__()

        self.setConfig(self.ConfigSiteDescription,
                       "A Fake Site with no backend to test the scale core.")
        self.setConfig(self.ConfigMachines, {"euca-default": {}})

        self.privateConfig += [self.ConfigSiteDescription]

        self.runningMachines = dict()
        self.mr = MachineRegistry.MachineRegistry()

        self.bootTimeMu = 60
        self.bootTimeSigma = 5

    def init(self):
        self.mr.registerListener(self)

    def onEvent(self, evt):
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            # check correct site etc...
            if evt.newStatus == self.mr.statusDisintegrated:
                # ha, machine to kill
                self.mr.updateMachineStatus(evt.id, self.mr.statusDown)
                # self.mr.removeMachine(evt.id)

    def runningMachines():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._runningMachines

        def fset(self, value):
            self._runningMachines = value

        def fdel(self):
            del self._runningMachines

        return locals()

    runningMachines = property(**runningMachines())

    def manage(self):
        for k in self.runningMachines:
            if (datetime.datetime.now() - self.mr.machines[k][
                self.mr.regStatusLastUpdate]).seconds > random.gauss(
                self.bootTimeMu, self.bootTimeSigma) and \
                            self.mr.machines[k][self.mr.regStatus] == self.mr.statusBooting:
                logging.info("Machine " + str(k) + " is done booting")
                self.mr.updateMachineStatus(k, self.mr.statusUp)

    def spawnMachines(self, machineType, count):
        for i in range(0, count):
            self.mr = MachineRegistry.MachineRegistry()
            id = self.mr.newMachine()

            self.runningMachines[id] = None

            self.mr.machines[id][self.mr.regSite] = self.getSiteName()
            self.mr.machines[id][self.mr.regMachineType] = machineType
            self.mr.updateMachineStatus(id, self.mr.statusBooting)

        return count

    def terminateMachines(self, machineType, count):
        reg = MachineRegistry.MachineRegistry()

        # a tuple is returned here
        toRemove = filter(lambda (k, v): v[reg.regStatus] == reg.statusWorking and \
                                         v[reg.regMachineType] == machineType, \
                          self.mr.machines.iteritems())

        # only pick the needed amount
        toRemove = toRemove[0:count]
        map(lambda (k, v): reg.updateMachineStatus(k, reg.statusPendingDisintegration), toRemove)

        return len(toRemove)

    # def getRunningMachines(self):
    #     res = dict()
    #
    #     res["machine1"] = []
    #
    #     for k in self.runningMachines:
    #         if self.mr.machines[k].get(self.mr.reg_status) == self.mr.StatusBooting or \
    #                         self.mr.machines[k].get(self.mr.reg_status) == self.mr.StatusUp or \
    #                         self.mr.machines[k].get(
    #                             self.mr.reg_status) == self.mr.StatusIntegrating or \
    #                         self.mr.machines[k].get(self.mr.reg_status) == self.mr.StatusWorking:
    #             res["machine1"].append(k)
    #     return res
