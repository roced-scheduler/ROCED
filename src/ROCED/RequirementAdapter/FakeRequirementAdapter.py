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


from datetime import datetime

from Core import MachineRegistry
from RequirementAdapter.Requirement import RequirementAdapterBase


class FakeRequirementAdapter(RequirementAdapterBase):
    def __init__(self):
        super(FakeRequirementAdapter, self).__init__()
        self._curRequirement = 5
        self.completeJobs = False
        self.machinesRunningJobs = {}
        self.jobDuration = 10  # seconds
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

        # free done jobs
        [self.machinesRunningJobs.pop(key) for (key, value) in self.machinesRunningJobs.items()
         if (datetime.now() - value).seconds > self.jobDuration]

        # find 'free' machines
        for (mid, machine) in self.mr.getMachines().items():
            if self._curRequirement > 0:
                if mid not in self.machinesRunningJobs:
                    if machine.get(self.mr.regStatus) == self.mr.statusWorking:
                        self.machinesRunningJobs[mid] = datetime.now()
                        self._curRequirement -= 1
                    if machine.get(self.mr.regStatus) == self.mr.statusBooting:
                        self.machinesRunningJobs[mid] = datetime.now()
                        self._curRequirement -= 1
                    if machine.get(self.mr.regStatus) == self.mr.statusUp:
                        self.machinesRunningJobs[mid] = datetime.now()
                        self._curRequirement -= 1
                    if machine.get(self.mr.regStatus) == self.mr.statusIntegrating:
                        self.machinesRunningJobs[mid] = datetime.now()
                        self._curRequirement -= 1

        return self._curRequirement

    def getNeededMachineType(self):
        # return "euca-default"
        return "vm-default"
