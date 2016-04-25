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
from IntegrationAdapter.Integration import IntegrationAdapterBase


class FakeIntegrationAdapter(IntegrationAdapterBase):
    def init(self):
        super(FakeIntegrationAdapter, self).init()
        self.mr = MachineRegistry.MachineRegistry()
        self.mr.registerListener(self)

    def manage(self):
        [self.mr.updateMachineStatus(mid, self.mr.statusWorking) for mid
         in self.mr.getMachines(status=self.mr.statusIntegrating)]

        [self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated) for mid
         in self.mr.getMachines(status=self.mr.statusDisintegrating)
         if self.mr.calcLastStateChange(mid) > random.randint(10, 50)]

    def onEvent(self, evt):
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            if evt.newStatus == self.mr.statusUp:
                logging.info("Integrating machine with ip " + str(
                    self.mr.machines[evt.id].get(self.mr.regHostname)))
                # ha, new machine to integrate
                self.mr.updateMachineStatus(evt.id, self.mr.statusIntegrating)
            elif evt.newStatus == self.mr.statusPendingDisintegration:
                # ha, machine to disintegrate
                self.mr.updateMachineStatus(evt.id, self.mr.statusDisintegrating)

    @property
    def description(self):
        return "FakeIntegrationAdapter"
