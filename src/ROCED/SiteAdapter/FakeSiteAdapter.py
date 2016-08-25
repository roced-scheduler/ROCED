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

import random

from Core import MachineRegistry
from .Site import SiteAdapterBase


class FakeSiteAdapter(SiteAdapterBase):
    def __init__(self):
        """A "hypothetical" site adapter, simulating regular behaviour.

         Simulated behaviour from a site:
         - Boot machines via "spawnMachines" - this usually calls a site's API
         - Handle status changes via manage (e.g. an ordered machine started to boot)
         - Terminate running machines
         - Remove shutdown machines"""
        super(FakeSiteAdapter, self).__init__()

        self.setConfig(self.ConfigSiteDescription, "A Fake Site with no backend to test the scale core.")
        self.privateConfig += [self.ConfigSiteDescription]

        self.bootTimeMu = 4
        self.bootTimeSigma = 2

    def init(self):
        self.mr.registerListener(self)

    def onEvent(self, evt):
        if (isinstance(evt, MachineRegistry.StatusChangedEvent) and
                    self.mr.machines[evt.id].get(self.mr.regSite) == self.siteName):
            # check correct site etc...
            if evt.newStatus == self.mr.statusDisintegrated:
                # ha, machine to kill
                self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def manage(self, cleanup=False):
        for machineType in self.runningMachines:
            [self.mr.updateMachineStatus(mid, self.mr.statusUp) for mid in self.runningMachines[machineType]
             if self.mr.machines[mid][self.mr.regStatus] == self.mr.statusBooting and
             self.mr.calcLastStateChange(mid) > random.gauss(self.bootTimeMu,
                                                             self.bootTimeSigma)]
        for mid in self.getSiteMachines(status=self.mr.statusDown):
            self.mr.removeMachine(mid)

    def spawnMachines(self, machineType, count):
        for i in range(count):
            mid = self.mr.newMachine()

            self.runningMachines[mid] = {}

            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        return count

    def terminateMachines(self, machineType, count):
        workingMachines = self.getSiteMachines(status=self.mr.statusWorking, machineType=machineType)

        toRemove = []

        # Pick machines with machine load 0!
        for mid in workingMachines:
            if workingMachines[mid].get(self.mr.regMachineLoad, 0) == 0:
                toRemove.append(mid)

        number = len(toRemove)
        if number >= count:
            number = count
        elif number == 0:
            self.logger.warning("No idle machine(s) found when trying to terminate %d machines." % count)
        else:
            self.logger.warning("Can only shutdown %d machines, rest is still working." % number)

        # only pick the needed amount
        toRemove = toRemove[0:number]

        [self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration) for mid in toRemove]

        return len(toRemove)
