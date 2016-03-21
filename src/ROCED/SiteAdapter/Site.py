# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015 by Georg Fleig, Thomas Hauth and Stephan Riedel
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


import abc
import copy
import logging

from Core import Config
from Core import MachineRegistry
from Core.Adapter import AdapterBase, AdapterBoxBase


class SiteInformation(object):
    def __init__(self):
        self.siteName = None
        self.baselineMachines = 0
        self.maxMachines = 0
        self.supportedMachineTypes = []
        self.cost = 0
        self.isAvailable = True

    def siteName():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._siteName

        def fset(self, value):
            self._siteName = value

        def fdel(self):
            del self._siteName

        return locals()

    siteName = property(**siteName())

    def baselineMachines():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._baselineMachines

        def fset(self, value):
            self._baselineMachines = value

        def fdel(self):
            del self._baselineMachines

        return locals()

    baselineMachines = property(**baselineMachines())

    def maxMachines():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._maxMachines

        def fset(self, value):
            self._maxMachines = value

        def fdel(self):
            del self._maxMachines

        return locals()

    maxMachines = property(**maxMachines())

    def supportedMachineTypes():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._supportedMachineTypes

        def fset(self, value):
            self._supportedMachineTypes = value

        def fdel(self):
            del self._supportedMachineTypes

        return locals()

    supportedMachineTypes = property(**supportedMachineTypes())

    def cost():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._cost

        def fset(self, value):
            self._cost = value

        def fdel(self):
            del self._cost

        return locals()

    cost = property(**cost())

    # if true, this site is available to run new nodes.
    # If a site is not available, already running nodes keep running
    def isAvailable():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._isAvailable

        def fset(self, value):
            self._isAvailable = value

        def fdel(self):
            del self._isAvailable

        return locals()

    isAvailable = property(**isAvailable())


class SiteAdapterBase(AdapterBase):
    """
    Abstract base class for specific cloud site information.
    """
    __metaclass__ = abc.ABCMeta

    ConfigSiteName = "site_name"
    ConfigSiteType = "site_type"
    ConfigSiteDescription = "site_description"
    ConfigMachines = "machines"
    ConfigIsAvailable = "is_available"
    ConfigCost = "cost"
    ConfigMaxMachines = "max_machines"
    ConfigMachineBootTimeout = "machine_boot_timeout"
    ConfigBaselineMachines = "baseline_machines"

    # Override the following for your custom cloud implementation
    @abc.abstractmethod
    def __init__(self):
        super(SiteAdapterBase, self).__init__()

        self.setConfig(self.ConfigSiteName, "default-site")
        self.setConfig(self.ConfigSiteDescription, "DefaultDescription")
        self.setConfig(self.ConfigIsAvailable, True)
        self.setConfig(self.ConfigCost, 0)
        self.setConfig(self.ConfigMaxMachines, None)  # None means: no limit

        self.setConfig(self.ConfigBaselineMachines, 0)
        self.setConfig(self.ConfigMachineBootTimeout, 300)

        self.addCompulsoryConfigKeys(self.ConfigSiteName, Config.ConfigTypeString)

        self.addOptionalConfigKeys(self.ConfigMachines, Config.ConfigTypeDictionary, default={})
        self.addOptionalConfigKeys(self.ConfigIsAvailable, Config.ConfigTypeBoolean, default=True)
        self.addOptionalConfigKeys(self.ConfigMachineBootTimeout, Config.ConfigTypeInt, default=30)
        self.addOptionalConfigKeys(self.ConfigMaxMachines, Config.ConfigTypeInt, default=10)

        self.logger = logging.getLogger('Site')

    @abc.abstractmethod
    def spawnMachines(self, machineType, count):
        """
        Spawn machines on the corresponding cloud site.

        :param machineType:
        :param count:
        :return:
        """
        pass

    def terminateMachines(self, machineType, count):
        """
        Terminate machine(s) on the corresponding site.

        This *should* be redefined but doesn't /have to/. There are other ways to handle
        termination (e.g.: automatic timeout/shutdown).

        :param machineType:
        :param count:
        """
        pass

    def getSiteMachines(self, status=None, machineType=None):
        """
        Return dictionary with machines running on this site.

        :param status: optional filter on status
        :param machineType: optional filter on machine type
        :return {}:
        """
        mr = MachineRegistry.MachineRegistry()
        return mr.getMachines(self.getSiteName(), status, machineType)

    def getRunningMachinesCount(self):
        running_machines = self.getRunningMachines()
        running_machines_count = dict()
        for machine_type in running_machines:
            running_machines_count[machine_type] = len(running_machines[machine_type])
        return running_machines_count

    ###
    # All overrides done, the following functions will work and don't need to be overwritten """
    ###

    def applyMachineDecision(self, decision):

        decision = copy.deepcopy(decision)
        running_machines_count = self.getRunningMachinesCount()
        max_machines = self.getConfig(self.ConfigMaxMachines)

        for (machine_type, n_machines) in decision.iteritems():
            # calc relative value when there are already machines running
            n_running_machines = 0
            if machine_type in running_machines_count:
                n_running_machines = running_machines_count[machine_type]
                decision[machine_type] -= n_running_machines

            # spawn
            if decision[machine_type] > 0:
                # respect site limit for max machines for spawning but don't remove machines when above limit
                # this limit is currently implemented per machine type, not per site!
                if max_machines and (decision[machine_type] + n_running_machines) > max_machines:
                    self.logger.info(
                        "Request exceeds maximum number of allowed machines on this site (" +
                        str(decision[machine_type] + n_running_machines) + ">" + str(max_machines) +
                        ")! ")
                    self.logger.info("Will spawn " + str(
                            max(0, max_machines - n_running_machines)) + " machines.")
                    decision[machine_type] = max_machines - n_running_machines
                    # is the new decision valid?
                    if decision[machine_type] > 0:
                        self.spawnMachines(machine_type, decision[machine_type])
                else:
                    self.spawnMachines(machine_type, decision[machine_type])

            # terminate
            elif decision[machine_type] < 0:
                self.terminateMachines(machine_type, abs(decision[machine_type]))

    # returns all machine which occupy computing resources on the cloud
    def getSiteMachinesAsDict(self):
        myMachines = self.getSiteMachines()
        machineList = dict()

        for i in self.getConfig(self.ConfigMachines):
            machineList[i] = []

        for (k, v) in myMachines.iteritems():
            machineList[v[self.mr.regMachineType]].append(k)

        return machineList

    # returns all machine which occupy computing resources on the cloud
    def getCloudOccupyingMachines(self):
        myMachines = self.getSiteMachines()
        machineList = dict()

        for i in self.getConfig(self.ConfigMachines):
            machineList[i] = []

        for (k, v) in myMachines.iteritems():
            if not v.get(self.mr.regStatus) == self.mr.statusDown:
                # will later hold specific information, like id, ip etc
                try:
                    machineList[v[self.mr.regMachineType]].append(k)
                except KeyError:
                    pass

        return machineList

    def getCloudOccupyingMachinesCount(self):
        return reduce(lambda v, (k, inp): v + len(inp),
                      self.getCloudOccupyingMachines().iteritems(), 0)

    def isMachineTypeSupported(self, machineType):
        return machineType in self.getConfig(self.ConfigMachines)

    '''
    static information about the site
    '''

    def getSiteInformation(self):

        sinfo = SiteInformation()
        sinfo.siteName = self.getSiteName()
        sinfo.maxMachines = self.getConfig(self.ConfigMaxMachines)
        sinfo.baselineMachines = self.getConfig(self.ConfigBaselineMachines)
        sinfo.supportedMachineTypes = self.getConfig(self.ConfigMachines).keys()
        sinfo.cost = self.getConfig(self.ConfigCost)
        sinfo.isAvailable = self.getConfig(self.ConfigIsAvailable)

        return sinfo

    def getRunningMachines(self):
        """ Return machines running at a specific site

        :return dictionary {machine: status} :
        """

        myMachines = self.getSiteMachines()
        machineList = dict()

        for i in self.getConfig(self.ConfigMachines):
            machineList[i] = []

        for (k, v) in myMachines.iteritems():
            if (v.get(self.mr.regStatus) == self.mr.statusBooting) or \
                    (v.get(self.mr.regStatus) == self.mr.statusUp) or \
                    (v.get(self.mr.regStatus) == self.mr.statusWorking):
                # will later hold specific information, like id, ip etc
                machineList[v[self.mr.regMachineType]].append(k)

        return machineList

    def getSiteName(self):
        return self.getConfig(self.ConfigSiteName)

    def getSiteType(self):
        return self.getConfig(self.ConfigSiteType)

    def getDescription(self):
        return self.getConfig(self.ConfigSiteDescription)


class SiteBox(AdapterBoxBase):
    def getRunningMachines(self):
        all_ = dict()

        for s in self.adapterList:
            all_[s.getSiteName()] = s.getRunningMachines()

        return all_

    def getRunningMachinesCount(self):
        all_ = dict()

        for s in self.adapterList:
            all_[s.getSiteName()] = s.getRunningMachinesCount()

        return all_

    def applyMachineDecision(self, decision):
        map(lambda x: x.applyMachineDecision(decision.get(x.getSiteName(), dict())),
            self.adapterList)

    def getSiteConfigAsDict(self):
        all_ = dict()

        for s in self.adapterList:
            all_[s.getSiteName()] = s.getConfigAsDict()

        return all_

    def getSite(self, siteName):
        res = filter(lambda x: x.getSiteName() == siteName, self.adapterList)
        if len(res) == 1:
            return res[0]
        else:
            return None

    def getSiteInformation(self):
        all_ = dict()

        for s in self.adapterList:
            all_[s.getSiteName()] = s.getSiteInformation()

        return all_

# def getMachineCount(self, machineType):
#     return reduce(lambda x, y: x + y.getMachineCount("machineType"),
#                   self.adapterList, 0)
#
#
# def spawnMachines(self, machineType, count):
#     supported = filter(lambda x: x.isMachineTypeSupported(machineType), self.adapterList)
#
#     if len(supported) == 0:
#         logging.error("MachineType " + machineType + " not supported by any Spawn adapter")
#         return 0
#
#     # take cost and free slots into consideration
#     return supported[0].spawnMachines(machineType, count)
