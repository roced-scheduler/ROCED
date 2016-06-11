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
from __future__ import unicode_literals

from time import time
import logging
from Core import MachineRegistry
from RequirementAdapter.Requirement import RequirementAdapterBase


class FakeRequirementAdapter(RequirementAdapterBase):
    def __init__(self):
        """A "hypothetical" requirement adapter, simulating regular behaviour.

         Simulated behaviour from a batch system:
         - Processing starts with 5 idle jobs, which results in 5 machines.
         - Once a "working" machine is found, a job is assigned and processed for 4 seconds.
         - When a job is running, the machine load is 1
         - When a job finishes, the machine load is 0

         The requirement adapter usually just has to track the number of running and idle jobs.
         Integration adapter should handle machine load, etc."""
        super(FakeRequirementAdapter, self).__init__(machineType="vm-default")
        self._curRequirement = 5
        self.completeJobs = True
        self.machinesRunningJobs = {}
        self._jobcount = self._curRequirement
        self._jobDuration = 4  # in seconds
        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        super(FakeRequirementAdapter, self).init()

    @property
    def description(self):
        return "FakeRequirementAdapter"

    @property
    def requirement(self):
        if self.completeJobs is False:
            return self._curRequirement

        ###
        # Simulated batch system:
        ###

        # free done jobs
        for mid in list(self.machinesRunningJobs):
            if time() - self.machinesRunningJobs[mid] > self._jobDuration:
                logging.debug("Job on machine %s finished." % mid)
                self.mr.machines[mid][self.mr.regMachineLoad] = 0
                self.machinesRunningJobs.pop(mid)
                if self._curRequirement > 0:
                    self._curRequirement -= 1

        # find "free" machines & assign jobs
        for mid in self.mr.getMachines(status=self.mr.statusWorking):
            if self._curRequirement > 0 and self._jobcount > 0 and mid not in self.machinesRunningJobs:
                self.machinesRunningJobs[mid] = time()
                self.mr.machines[mid][self.mr.regMachineLoad] = 1
                self._jobcount -= 1
                logging.debug("Job on machine %s started." % mid)

        return self._curRequirement
