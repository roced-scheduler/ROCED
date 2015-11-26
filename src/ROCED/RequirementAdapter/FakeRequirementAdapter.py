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
        RequirementAdapterBase.__init__(self)
        self.curReq = 5
        self.completeJobs = False
        self.machinesRunningJobs = {}
        self.jobDuration = 10  # seconds
        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        self.exportMethod(self.setCurrentRequirement, "FakeRequirementAdapter_setCurrentRequirement")

    def getCurrentRequirement(self):

        if self.completeJobs == False:
            return self.curReq

        # free done jobs
        done = filter(lambda (k, v): (datetime.now() - v).seconds > self.jobDuration,
                      self.machinesRunningJobs.iteritems())

        map(lambda (k, v): self.machinesRunningJobs.pop(k), done)

        # find 'free' machines
        for (k, v) in self.mr.machines.iteritems():
            if self.curReq > 0:
                if not k in self.machinesRunningJobs:
                    if v.get(self.mr.regStatus) == self.mr.statusWorking:
                        self.machinesRunningJobs[k] = datetime.now()
                        self.curReq = self.curReq - 1
                    if v.get(self.mr.regStatus) == self.mr.statusBooting:
                        self.machinesRunningJobs[k] = datetime.now()
                        self.curReq = self.curReq - 1
                    if v.get(self.mr.regStatus) == self.mr.statusUp:
                        self.machinesRunningJobs[k] = datetime.now()
                        self.curReq = self.curReq - 1
                    if v.get(self.mr.regStatus) == self.mr.statusIntegrating:
                        self.machinesRunningJobs[k] = datetime.now()
                        self.curReq = self.curReq - 1

        return self.curReq

    def setCurrentRequirement(self, c):
        self.curReq = c
        # to avoid the None problem with XML RPC
        return 23

    def getNeededMachineType(self):
        # return "euca-default"
        return "vm-default"

    def getDescription(self):
        return "FakeRequirementAdapter"
