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

"""
The RequirementAdapters check the state of the queues and tell the broker the current requirements.
The broker then decides how many machines have to be started.
"""

import abc

from Core.Adapter import AdapterBase, AdapterBoxBase


class RequirementAdapterBase(AdapterBase):
    __metaclass__ = abc.ABCMeta

    ConfigReqName = "reqName"

    def __init__(self, machineType="default"):
        super(RequirementAdapterBase, self).__init__()
        self._curRequirement = 0
        self._machineType = machineType
        self.setConfig(self.ConfigReqName, "DefaultReq")

    def init(self):
        super(RequirementAdapterBase, self).init()
        self.exportMethod(lambda requirement_: self.__setattr__(name="requirement",
                                                                value=requirement_),
                          type(self).__name__ + "_setRequirement")

    @property
    def name(self):
        return self.getConfig(self.ConfigReqName)

    @property
    @abc.abstractmethod
    def description(self):
        return "RequirementAdapterBase"

    @property
    @abc.abstractmethod
    def requirement(self):
        """Return numbers of machine(s) required (integer) or None (Bool) if error."""
        return self._curRequirement

    @requirement.setter
    def requirement(self, requirement_):
        """External "Set requirement". Primarily intended for RPC API.

        Number of required machines should never be lower than 0, otherwise it's some sort
        of connection/request/calculation problem."""
        if requirement_ < 0:
            requirement_ = None
        self._curRequirement = requirement_

    def getNeededMachineType(self):
        return self._machineType


class RequirementBox(AdapterBoxBase):
    def __init__(self):
        super(RequirementBox, self).__init__()
        self.reqCache = {}

    def getMachineTypeRequirement(self, fromCache=False):
        """Calculate list of required machines per machine type

        Format: {machine type: integer; machine_type: integer}"""
        if fromCache is True:
            return self.reqCache

        needDict = dict()

        for adapter in self._adapterList:
            if adapter.getNeededMachineType() not in needDict:
                needDict[adapter.getNeededMachineType()] = 0

            curReq = adapter.requirement
            if curReq is not None:
                needDict[adapter.getNeededMachineType()] += int(curReq)
            else:
                needDict[adapter.getNeededMachineType()] = None

        self.reqCache = needDict

        return needDict

    def manage(self, cleanup=False):
        pass
