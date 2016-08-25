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
import random

from Core import MachineRegistry, Config
from IntegrationAdapter.Integration import IntegrationAdapterBase


class FakeIntegrationAdapter(IntegrationAdapterBase):
    configSiteLogger = "logger_name"
    configSiteName = "site_name"

    def __init__(self):
        """A "hypothetical" integration  adapter, simulating regular behaviour.

         Simulated behaviour from a batch system:
         - Handle status changes via manage (e.g. an ordered machine started to boot)
            - Track "integrating" machines until they appear in the batch system.
            - Track "working" machines workload.
            - Disintegrate idle machines."""
        super(FakeIntegrationAdapter, self).__init__()
        self.addCompulsoryConfigKeys(self.configSiteName, Config.ConfigTypeString,
                                     description="Site name")
        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of Site Adapter", default="FakeInt")
        self.mr.registerListener(self)

    def init(self):
        super(FakeIntegrationAdapter, self).init()
        self.logger = logging.getLogger(self.getConfig("logger_name"))
        self.siteName = self.getConfig(self.configSiteName)

    def manage(self, cleanup=False):
        [self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated) for mid
         in self.mr.getMachines(site=self.siteName, status=self.mr.statusDisintegrating)
         if self.mr.calcLastStateChange(mid) > random.randint(2, 6)]

        # In our test cases, this is done by site adapter & requirement adapter
        # [self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration) for mid
        #  in self.mr.getMachines(status=self.mr.statusWorking)
        #  if self.mr.calcLastStateChange(mid) > random.randint(2, 6)]
        [self.mr.updateMachineStatus(mid, self.mr.statusWorking) for mid
         in self.mr.getMachines(site=self.siteName, status=self.mr.statusIntegrating)]

    def onEvent(self, evt):
        if (isinstance(evt, MachineRegistry.StatusChangedEvent) and
                    self.mr.machines[evt.id].get(self.mr.regSite) == self.siteName):
            if evt.newStatus == self.mr.statusUp:
                self.logger.info("Integrating machine with ip %s" % self.mr.machines[evt.id].get(self.mr.regHostname))
                # ha, new machine to integrate
                self.mr.updateMachineStatus(evt.id, self.mr.statusIntegrating)
            elif evt.newStatus == self.mr.statusWorking:
                self.mr.machines[evt.id][self.mr.regMachineLoad] = 0
            elif evt.newStatus == self.mr.statusPendingDisintegration:
                # ha, machine to disintegrate
                self.mr.updateMachineStatus(evt.id, self.mr.statusDisintegrating)

    @property
    def description(self):
        return "FakeIntegrationAdapter"
