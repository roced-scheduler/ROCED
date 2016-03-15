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


"""
The RequirementAdapters check the state of the queues and tell the broker the current requirements.
The broker then decides how many machines have to be started.

class Requirement(object):

    _need = 0

    def __init__(self, need = 0):
        self._hasNeed = need

    def hasNeed(self):
        return self._need > 0

    def getNeed(self):
        return self._need

"""

import abc

from Core.Adapter import AdapterBase, AdapterBoxBase


class RequirementAdapterBase(AdapterBase):
    __metaclass__ = abc.ABCMeta
    _machineType = None
    _curRequirement = 0

    ConfigReqName = "reqName"

    def __init__(self, machineType="default"):
        super(RequirementAdapterBase, self).__init__()

        self._machineType = machineType
        self.setConfig(self.ConfigReqName, "DefaultReq")

    def init(self):
        super(RequirementAdapterBase, self).init()
        self.exportMethod(self.setRequirement, "RequirementAdapterBase_setRequirement")

    def getCurrentRequirement(self):
        return self._curRequirement

    def setRequirement(self, req):
        self._curRequirement = req

    def getNeededMachineType(self):
        return self._machineType

    def getName(self):
        return self.getConfig(self.ConfigReqName)

    def getDescription(self):
        return "RequirementAdapterBase"


class RequirementBox(AdapterBoxBase):
    def __init__(self):
        super(RequirementBox, self).__init__()
        self.reqCache = {}

    def getMachineTypeRequirement(self, fromCache=False):

        if fromCache == True:
            return self.reqCache

        needDict = dict()

        for a in self.adapterList:
            if not needDict.has_key(a.getNeededMachineType()):
                needDict[a.getNeededMachineType()] = 0

            curReq = a.getCurrentRequirement()
            if not curReq == None:
                needDict[a.getNeededMachineType()] += int(curReq)
            else:
                needDict[a.getNeededMachineType()] = None

        self.reqCache = needDict

        return needDict
