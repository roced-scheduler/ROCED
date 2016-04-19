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

import logging
import random

from Core import MachineRegistry
from .Site import SiteAdapterBase


class FakeSiteAdapter(SiteAdapterBase):
    def __init__(self):
        super(FakeSiteAdapter, self).__init__()

        self.setConfig(self.ConfigSiteDescription,
                       "A Fake Site with no backend to test the scale core.")
        self.setConfig(self.ConfigMachines, {"euca-default": {}})

        self.privateConfig += [self.ConfigSiteDescription]

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

    def manage(self):
        for machineType in self.runningMachines:
            for mid in self.runningMachines[machineType]:
                if self.mr.calcLastStateChange(mid) > random.gauss(
                   self.bootTimeMu, self.bootTimeSigma) and self.mr.machines[mid][
                        self.mr.regStatus] == self.mr.statusBooting:
                    logging.info("Machine " + str(mid) + " is done booting")
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)

    def spawnMachines(self, machineType, count):
        for i in range(0, count):
            self.mr = MachineRegistry.MachineRegistry()
            mid = self.mr.newMachine()

            self.runningMachines[mid] = {}

            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        return count

    def terminateMachines(self, machineType, count):
        toRemove = list(self.mr.getMachines(status=self.mr.statusWorking, machineType=machineType))

        # only pick the needed amount
        toRemove = toRemove[0:count]

        [self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration) for mid in toRemove]

        return len(toRemove)
