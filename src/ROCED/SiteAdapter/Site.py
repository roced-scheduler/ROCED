# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015, 2016 by Georg Fleig, Frank Fischer,
# Thomas Hauth and Stephan Riedel
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

import abc
import copy
import logging

from Core import MachineRegistry, Config
from Core.Adapter import AdapterBase, AdapterBoxBase


class SiteInformation(object):
    def __init__(self):
        self.siteName = None
        self.baselineMachines = 0
        self.maxMachines = 0
        self.supportedMachineTypes = []
        self.cost = 0
        self.isAvailable = True


class SiteAdapterBase(AdapterBase):
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

    mr = MachineRegistry.MachineRegistry()

    # Override the following for your custom cloud implementation
    @abc.abstractmethod
    def __init__(self):
        """Abstract base class for specific cloud site information."""
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

        self.logger = logging.getLogger("Site")

    @abc.abstractmethod
    def manage(self):
        """Periodically called manage function responsible for initiating site specific status changes.

        This method has to handle the following (machine registry) machine status transitions:
            Machine spawned -> Machine added to registry (Booting)
            Booting         -> Up
            Disintegrated   -> Down
            Down            -> Machine removed from registry
        The method "OnEvent" can also handle some of these. If the class is registered with the machine registry.

        1. Connect with the site to retrieve a list of all machines including their status.
        2. Iterate the Machine Registry (Method getSiteMachines) and change machine status accordingly.

        :return:
        """
        pass

    @abc.abstractmethod
    def spawnMachines(self, machineType, count):
        # type: (str, int) -> None
        """Spawn machines on the corresponding cloud site.

        :param machineType:
        :param count:
        :return:
        """
        self.logger.debug("Spawning %d machines of type %s" % (count, machineType))

    def terminateMachines(self, machineType, count):
        """Terminate machine(s) on the corresponding site.

        This *should* be redefined but doesn't /have to/. There are other ways to handle
        termination (e.g.: automatic timeout/shutdown).

        :param machineType:
        :param count:
        """
        pass

    def modServiceMachineDecision(self, decision):
        # type: (dict) -> dict
        """Modify machine request decision to accommodate service machine requirements.

        Some sites have specific requirements for service machines, for example squid server, DNS server, local
        Condor schedds, ...
        This method is called by the core, after requirement adapter and broker decided on the number of machines
        to boot on each site. Depending on the site's setup, this method should add the required service machine(s),
        replace single machines with service machine(s) or even replace the whole site order.

        :param decision:    Dictionary with (relative) machine requirements {machine_type: count, machine_type: count}
        :return:
        """
        return decision

    ###
    # The following functions shouldn't be required to be overwritten
    ###
    def getSiteMachines(self, status=None, machineType=None):
        # type: (str, str) -> dict
        """Return dictionary with machines running on this site (Machine registry).

        :param status: (optional) filter on machine status
        :param machineType: (optional) filter on machine type
        :return {machine_id: {a:b,c:d,e:f}, machine_id: {a:b,c:d,e:f}, ...}:
        """
        return self.mr.getMachines(self.siteName, status, machineType)

    def applyMachineDecision(self, decision):

        decision = copy.deepcopy(decision)
        running_machines_count = self.runningMachinesCount
        max_machines = self.getConfig(self.ConfigMaxMachines)

        for (machine_type, n_machines) in decision.items():
            # calc relative value when there are already machines running
            n_running_machines = 0
            if machine_type in running_machines_count:
                n_running_machines = running_machines_count[machine_type]
                decision[machine_type] -= n_running_machines

            # spawn
            if decision[machine_type] > 0:
                # TODO: Implement max_machines per site, not per machine type!!!
                # respect site limit for max machines for spawning but don't remove machines when
                # above limit this limit is currently implemented per machine type, not per site!
                if max_machines and (decision[machine_type] + n_running_machines) > max_machines:
                    self.logger.info("Request exceeds maximum number of allowed machines on this site (%d>%d)!"
                                     % (decision[machine_type] + n_running_machines, max_machines))
                    self.logger.info("Will spawn %s machines." % max(0, max_machines - n_running_machines))
                    decision[machine_type] = max_machines - n_running_machines
                    # is the new decision valid?
                    if decision[machine_type] > 0:
                        self.spawnMachines(machine_type, decision[machine_type])
                else:
                    self.spawnMachines(machine_type, decision[machine_type])

            # terminate
            elif decision[machine_type] < 0:
                self.terminateMachines(machine_type, abs(decision[machine_type]))

    def getSiteMachinesAsDict(self, statusFilter=None):
        # type: (list) -> dict
        """Retrieve machines running at a site. Optionally can filter on a status list.

        :return dictionary {machine_type: [machine ID, machine ID, ...], ...} :
        """
        if statusFilter is None:
            statusFilter = []

        myMachines = self.getSiteMachines()
        machineList = dict()

        for i in self.getConfig(self.ConfigMachines):
            machineList[i] = []

        for mid, machine in myMachines.items():
            if statusFilter:  # empty list returns false in this statement
                if machine.get(self.mr.regStatus) in statusFilter:
                    machineList[machine[self.mr.regMachineType]].append(mid)
            else:
                machineList[machine[self.mr.regMachineType]].append(mid)

        return machineList

    @property
    def runningMachines(self):
        """Dictionary of machines running at a site.

        :return dictionary {machine_type: [machine ID, machine ID, ...], ...} :
        """
        statusFilter = [self.mr.statusBooting,
                        self.mr.statusUp,
                        self.mr.statusIntegrating,
                        self.mr.statusWorking,
                        self.mr.statusPendingDisintegration]
        return self.getSiteMachinesAsDict(statusFilter)

    @property
    def runningMachinesCount(self):
        """Dictionary of number of machine types running at a site.

        :return {machine_type: integer, ...}:
        """
        running_machines = self.runningMachines
        running_machines_count = dict()
        for (machine_type, midList) in running_machines.items():
            running_machines_count[machine_type] = len(midList)
        return running_machines_count

    @property
    def cloudOccupyingMachines(self):
        # type: () -> dict
        """All machines which occupy computing resources on the cloud.

        Same behaviour as getRunningMachines, just with other status filtering.

        :return dictionary {machine_type: [machine ID, machine ID, ...], ...} :
        """
        statusFilter = [self.mr.statusBooting,
                        self.mr.statusUp,
                        self.mr.statusIntegrating,
                        self.mr.statusWorking,
                        self.mr.statusPendingDisintegration,
                        self.mr.statusDisintegrating,
                        self.mr.statusDisintegrated]
        return self.getSiteMachinesAsDict(statusFilter)

    @property
    def cloudOccupyingMachinesCount(self):
        """Total number of machines occupying computing resources on a site."""
        sum_ = 0
        for (machineType, midList) in self.cloudOccupyingMachines.items():
            sum_ += len(midList)
        return sum_

    def isMachineTypeSupported(self, machineType):
        return machineType in self.getConfig(self.ConfigMachines)

    """
    static information about a site
    """

    @property
    def siteInformation(self):
        # type: () -> dict

        sinfo = SiteInformation()
        sinfo.siteName = self.siteName
        sinfo.maxMachines = self.getConfig(self.ConfigMaxMachines)
        sinfo.baselineMachines = self.getConfig(self.ConfigBaselineMachines)
        sinfo.supportedMachineTypes = self.getConfig(self.ConfigMachines)
        sinfo.cost = self.getConfig(self.ConfigCost)
        sinfo.isAvailable = self.getConfig(self.ConfigIsAvailable)

        return sinfo

    @property
    def siteName(self):
        return self.getConfig(self.ConfigSiteName)

    @property
    def siteType(self):
        return self.getConfig(self.ConfigSiteType)

    @property
    def description(self):
        return self.getConfig(self.ConfigSiteDescription)


class SiteBox(AdapterBoxBase):
    @property
    def runningMachines(self):
        # type: () -> dict
        """Dictionary with running machines per site.

        :return {siteName: {machine_type: [machine ID, machine ID, ...], ...}}:
        """
        return {site.siteName: site.runningMachines for site in self._adapterList}

    @property
    def runningMachinesCount(self):
        # type: () -> dict
        """Dictionary with number of running machines per site.

        :return {siteName: {machine_type: integer, ...}}:
        """
        return {site.siteName: site.runningMachinesCount for site in self._adapterList}

    @property
    def siteConfigAsDict(self):
        # type: () -> dict
        """Dictionary with configuration per site.

        :return {siteName: {machine_type: integer, ...}}:
        """
        return {site.siteName: site.getConfigAsDict() for site in self._adapterList}

    @property
    def siteInformation(self):
        # type: () -> dict
        return {site.siteName: site.siteInformation for site in self._adapterList}

    def getSite(self, siteName):
        res = [site for site in self._adapterList if site.siteName == siteName]
        if len(res) == 1:
            return res[0]
        else:
            return None

    def applyMachineDecision(self, decision):
        [x.applyMachineDecision(decision.get(x.siteName, dict())) for x in self._adapterList]

    def modServiceMachineDecision(self, decision):
        # type: (dict) -> dict
        """Modify "decision to order" (add or replace) to boot service machines (e.g. SQUIDs)."""
        for site in self._adapterList:
            if site.siteName in decision:
                try:
                    temp_decision = site.modServiceMachineDecision(decision[site.siteName])
                    decision[site.siteName] = temp_decision
                except AttributeError:
                    # method not being defined in the site adapter is no problem. Just ignore it...
                    pass

        return decision
