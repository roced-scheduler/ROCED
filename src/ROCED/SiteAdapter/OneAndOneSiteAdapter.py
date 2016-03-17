# ==============================================================================
#
# Copyright (c) 2016 by Guenther Erli
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
# ==============================================================================

import logging
import re

from Core import Config, MachineRegistry
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog
from oneandone.client import OneAndOneService, Server, Hdd

# sh** happens with oneandone....
import time


class OneAndOneSiteAdapter(SiteAdapterBase):
    """
    site adapter for 1and1 cloud

    responsible for booting up and shutting down machines
    """

    # name for this adapter to be shown in ROCED output
    configSiteLogger = "logger_name"

    # login credentials for OneAndOne Cloud
    configApiToken = "api_token"

    # machine specific settings
    configMachines = "machines"
    configAppliance = "appliance"
    configFirewallPolicy = "firewall_policy"
    configMonitoringPolicy = "monitoring_policy"
    configHddSize = "hdd_size"
    configVcores = "vcores"
    configCoresPerProcessor = "cores_per_processor"
    configRam = "ram"
    configPassword = "password"
    configSquid = "squid"

    # site settings
    configMaxMachines = "max_machines"

    # keywords for machine registry
    reg_site_server_id = "reg_site_server_id"
    reg_site_server_status = "reg_site_server_status"
    reg_site_server_name = "reg_site_server_name"
    reg_site_server_condor_name = "reg_site_server_condor_name"

    # keywords for 1and1 responses
    oao = "oneandone"
    oao_id = "id"
    oao_ip = "ip"
    oao_ips = "ips"
    oao_status = "status"
    oao_state = "state"
    oao_name = "name"

    oao_state_deploying = "DEPLOYING"
    oao_state_powered_on = "POWERED_ON"
    oao_state_powered_off = "POWERED_OFF"
    oao_state_power_on = "POWER_ON"
    oao_state_power_off = "POWER_OFF"
    oao_delete = "DELETE"
    oao_state_powering_on = "POWERING_ON"
    oao_state_powering_off = "POWERING_OFF"
    oao_method_software = "SOFTWARE"
    oao_method_hardware = "HARDWARE"

    def __init__(self):
        """Init function

        load config keys from config files

        :return:
        """
        SiteAdapterBase.__init__(self)

        # load SiteAdapter name
        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of SiteAdapter",
                                   default="OaO_Site")

        # load API Token
        self.addCompulsoryConfigKeys(self.configApiToken, Config.ConfigTypeString,
                                     description="API Token for loggin in to OneAndOne Cloud")

        # load machine specific settings
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary,
                                     description="Machine type")
        self.addCompulsoryConfigKeys(self.configAppliance, Config.ConfigTypeString,
                                     description="Appliance to boot VM")
        self.addOptionalConfigKeys(self.configFirewallPolicy, Config.ConfigTypeString,
                                   description="Firewall policy for VM",
                                   default="")
        self.addOptionalConfigKeys(self.configMonitoringPolicy, Config.ConfigTypeString,
                                   description="Monitoring policy for VM",
                                   default="None")
        self.addOptionalConfigKeys(self.configCoresPerProcessor, Config.ConfigTypeInt,
                                   description="Number of cores per processor",
                                   default=4)
        self.addOptionalConfigKeys(self.configHddSize, Config.ConfigTypeInt,
                                   description="Size of Hdd",
                                   default=80)
        self.addOptionalConfigKeys(self.configVcores, Config.ConfigTypeInt,
                                   description="Number of virtual cores",
                                   default=4)
        self.addOptionalConfigKeys(self.configRam, Config.ConfigTypeInt,
                                   description="Ram size",
                                   default=8)
        self.addOptionalConfigKeys(self.configPassword, Config.ConfigTypeString,
                                   description="Password for virtual machines",
                                   default=None)
        self.addOptionalConfigKeys(self.configSquid, Config.ConfigTypeString,
                                   description="Squid server name",
                                   default=None)

        # site settings
        self.addOptionalConfigKeys(self.configMaxMachines, Config.ConfigTypeInt,
                                   description="limit amount of machines",
                                   default=None)

        # set name of Site Adapter for ROCED output
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))

        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        # disable urllib3 logging
        urllib3_logger = logging.getLogger("requests")
        urllib3_logger.setLevel(logging.CRITICAL)

        self.mr.registerListener(self)

    def getOneAndOneClient(self):
        """
        initialize 1and1 client
        :return: OneAndOneService()
        """

        try:
            client = OneAndOneService(self.getConfig(self.configApiToken))
        except Exception as e:
            self.logger.warning("Could not establish connection to 1&1 Cloud Site")
            self.logger.warning(str(e))
            return

        return client

    def getOneAndOneMachines(self):
        """
        return a list of all running servers at 1and1
        :return: client.list_servers()
        """

        client = self.getOneAndOneClient()
        try:
            tmp = client.list_servers()
        except Exception as e:
            self.logger.warning("Could not establish connection to 1&1. ERROR:")
            self.logger.warning(str(e))
            return

        servers = {}
        for server in tmp:
            servers[server[self.oao_id]] = {}
            for key in server.keys():
                if key is not self.oao_id:
                    servers[server[self.oao_id]][key] = server[key]

        return servers

    def getIndex(self, servers, requested):
        """
        return a list of unused indices
        :param servers: all running machines
        :param requested: amount of requested machines
        :return: index
        """

        used_indices = []
        for server in servers:
            # find all machines matching the requirement "roced-" + index
            number_found = re.findall(r"roced-([0-9]+)$", servers[server][self.oao_name])
            if len(number_found) > 0:
                used_indices.append(int(number_found[0]))
        if len(used_indices) > 0:
            i = 0
            new_indices = []
            while requested > 0:
                if i in used_indices:
                    i += 1
                else:
                    new_indices.append(i)
                    used_indices.append(i)
                    requested -= 1

        else:
            new_indices = range(requested)

        return new_indices

    def getRunningMachines(self):
        """Returns a dictionary containing all running machines

        The number of running machines needs to be recalculated when using status integrating and pending
        disintegration. Machines pending disintegration are still running an can accept new jobs. Machines integrating
        are counted as running machines by default.

        :return: machineList
        """

        myMachines = self.getSiteMachines()
        machineList = dict()

        machineList[self.getConfig(self.configMachines).keys()[0]] = []
        # for i in self.getConfig(self.configMachines).keys():
        #    machineList[i] = []

        # filter for machines in status booting, up, integrating, working or pending disintegration
        for (k, v) in myMachines.iteritems():
            if (v.get(self.mr.regStatus) == self.mr.statusBooting) or \
                    (v.get(self.mr.regStatus) == self.mr.statusUp) or \
                    (v.get(self.mr.regStatus) == self.mr.statusIntegrating) or \
                    (v.get(self.mr.regStatus) == self.mr.statusWorking) or \
                    (v.get(self.mr.regStatus) == self.mr.statusPendingDisintegration):
                # will later hold specific information, like id, ip etc
                machineList[v[self.mr.regMachineType]].append(k)

        return machineList

    def spawnMachines(self, machineType, requested):
        """
        spawn VMs for 1and1 cloud service
        :param machineType:
        :param requested:
        :return: count
        """

        if not machineType == self.getConfig(self.configMachines).keys()[0]:
            return 0

        client = self.getOneAndOneClient()
        servers = self.getOneAndOneMachines()

        index = self.getIndex(servers, requested)
        for count in xrange(requested):
            vm_name = "roced-" + "{0:0>3}".format(index.pop(0))  # str(index.pop(0))
            server = Server(name=vm_name,
                            appliance_id=self.getConfig(self.configAppliance),
                            vcore=self.getConfig(self.configVcores),
                            cores_per_processor=self.getConfig(self.configCoresPerProcessor),
                            ram=self.getConfig(self.configRam),
                            firewall_policy_id=self.getConfig(self.configFirewallPolicy),
                            monitoring_policy_id=self.getConfig(self.configMonitoringPolicy))

            hdd = Hdd(size=self.getConfig(self.configHddSize), is_main=True)
            hdds = [hdd]

            try:
                vm = client.create_server(server=server, hdds=hdds)
            except Exception as e:
                self.logger.warning("Could not start server on OneAndOne Cloud Service")
                self.logger.warning(str(e))
                continue

            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.getSiteName()
            self.mr.machines[mid][self.mr.regSiteType] = self.getSiteType()
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.machines[mid][self.reg_site_server_name] = vm[self.oao_name]
            self.mr.machines[mid][self.reg_site_server_id] = vm[self.oao_id]
            self.mr.machines[mid][self.reg_site_server_status] = vm[self.oao_status][self.oao_state]

            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # all machines booted
        # return count
        return

    def modifyMachineStatus(self, mid, action, method=oao_method_software):

        client = self.getOneAndOneClient()

        if action in [self.oao_state_power_on, self.oao_state_power_off]:
            client.modify_server_status(server_id=self.mr.machines[mid][self.reg_site_server_id], action=action,
                                        method=method)
            print "shut me down!!!"
        if action == self.oao_delete:
            try:
                client.delete_server(server_id=self.mr.machines[mid][self.reg_site_server_id])
            except Exception as e:
                self.logger.info(
                    "Machine " + str(self.mr.machines[mid][self.reg_site_server_name]) + " already deleted")
                # self.logger.warning("Could not establish connection to 1&1 Cloud Site")
                self.logger.warning(str(e))

        # sh** happens with oneandone...
        time.sleep(2)

        return

    def terminateMachines(self, machineType, count):
        return

        # a tuple is returned here
        # toRemove = filter(lambda (k, v): (  # v[self.mr.regStatus] == self.mr.statusDisintegrating
        # or v[self.mr.regStatus] == self.mr.statusDisintegrated
        # or
        #                                     v[self.mr.regStatus] == self.mr.statusPendingDisintegration)
        #                                 and v[self.mr.regSite] == self.getSiteName()
        #                                 and v[self.mr.regMachineType] == machineType,
        #                  self.mr.machines.iteritems())
        # booting machines first, less overhead
        # toRemove = sorted(toRemove, lambda (k1, v1), (k2, v2): (v1[self.mr.regStatus] == self.mr.statusWorking) * 2 - 1)

        # only pick the needed amount
        # toRemove = toRemove[0:count]
        # dont shutdown machines yet, only trigger the deregister process
        # map(lambda (k, v): self.mr.updateMachineStatus(k, self.mr.statusDisintegrating), toRemove)
        # return len(toRemove)

        # client = self.getOneAndOneClient()

        # for mid in xrange(len(toRemove)):
        #    print toRemove[mid][1][self.reg_site_server_id]
        #    self.modifyMachineStatus(mid=toRemove[mid][0], action=self.oao_state_power_off,
        #                             method=self.oao_method_software)
        # client.modify_server_status(server_id=toRemove[i][1][self.reg_site_server_id],
        #                            action=self.oao_state_power_off, method=self.oao_method_software)

    def manage(self):
        """
        managing machine states that change dependant of the state changes on 1and1 cloud site run once per cycle

        :return:
        """

        # get machines from 1and1 cloud site
        oao_machines = self.getOneAndOneMachines()

        # if something fails while receiving response from 1and1 a type none will be returned
        if (oao_machines == None):  # or (len(oao_machines) == 0):
            return

        for mid in self.mr.getMachines(self.getSiteName()):
            # check if machine is already deleted on site
            if (self.mr.machines[mid][self.reg_site_server_id] not in oao_machines):
                if self.mr.machines[mid][self.mr.regStatus] is not self.mr.statusDown:
                    self.mr.removeMachine(mid)
                    continue

            # down -> removed from machine registry
            if self.mr.machines[mid][self.mr.regStatus] == self.mr.statusDown:
                if self.mr.machines[mid][self.reg_site_server_id] not in oao_machines:
                    self.mr.removeMachine(mid)
                    continue
                elif self.mr.machines[mid][self.reg_site_server_id] in oao_machines:
                    self.modifyMachineStatus(mid, self.oao_delete)
                    del oao_machines[self.mr.machines[mid][self.reg_site_server_id]]

            # check if condor name is set
            if self.reg_site_server_condor_name not in self.mr.machines[mid]:
                # if not check if ip is available
                if (oao_machines[self.mr.machines[mid][self.reg_site_server_id]]["ips"] is not None) and (
                            oao_machines[self.mr.machines[mid][self.reg_site_server_id]]["ips"][0]["ip"] is not None):
                    # if so, set ip as condor name, remove "." from ip
                    self.mr.machines[mid][self.reg_site_server_condor_name] = \
                        oao_machines[self.mr.machines[mid][self.reg_site_server_id]]["ips"][0]["ip"].replace(".", "")

            # check for status which is handled by integration adapter
            if self.mr.machines[mid][self.mr.regStatus] in [self.mr.statusIntegrating, self.mr.statusWorking,
                                                            self.mr.statusPendingDisintegration]:
                del oao_machines[self.mr.machines[mid][self.reg_site_server_id]]

            # booting -> up
            if self.mr.machines[mid][self.mr.regStatus] == self.mr.statusBooting:
                if oao_machines[self.mr.machines[mid][self.reg_site_server_id]][self.oao_status][
                    self.oao_state] == self.oao_state_powered_on:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    self.mr.machines[mid][self.reg_site_server_status] = \
                        oao_machines[self.mr.machines[mid][self.reg_site_server_id]][self.oao_status]
                # remove from 1and1 machine list
                del oao_machines[self.mr.machines[mid][self.reg_site_server_id]]

            if self.mr.machines[mid][self.mr.regStatus] == self.mr.statusDisintegrating:
                self.modifyMachineStatus(mid, self.oao_state_power_off)
                del oao_machines[self.mr.machines[mid][self.reg_site_server_id]]

            # disintegrated -> down
            if self.mr.machines[mid][self.mr.regStatus] == self.mr.statusDisintegrated:
                if oao_machines[self.mr.machines[mid][self.reg_site_server_id]][self.oao_status][
                    self.oao_state] == self.oao_state_powered_off:
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                del oao_machines[self.mr.machines[mid][self.reg_site_server_id]]

        # add all machines remaining in machine list from 1&1
        for vm in oao_machines:
            # check if machine is quid server
            if (self.getConfig(self.configSquid) is not None) and (
                oao_machines[vm][self.oao_name] == self.getConfig(self.configSquid)):
                continue

            # check if machine is already in machine registry
            if vm in self.mr.getMachines(self.getSiteName()):
                continue
            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.getSiteName()
            self.mr.machines[mid][self.mr.regSiteType] = self.getSiteType()
            self.mr.machines[mid][self.mr.regMachineType] = self.oao  # machineType
            self.mr.machines[mid][self.reg_site_server_name] = oao_machines[vm][self.oao_name]
            self.mr.machines[mid][self.reg_site_server_id] = oao_machines[vm][self.oao_id]
            self.mr.machines[mid][self.reg_site_server_status] = oao_machines[vm][self.oao_status][self.oao_state]

            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # add current amounts of machines to Json log file
        self.logger.info("Current machines running at " + str(self.getSiteName()) + " : " + str(
            self.getRunningMachinesCount()[self.getConfig(self.configMachines).keys()[0]]))  # ["vm-default"]))
        json_log = JsonLog()
        json_log.addItem(self.getSiteName(), 'machines_requested',
                         int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                             len(self.getSiteMachines(status=self.mr.statusUp)) +
                             len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem(self.getSiteName(), 'condor_nodes', len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem(self.getSiteName(), 'condor_nodes_draining',
                         len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))

        del oao_machines

    def onEvent(self, mid):
        """
        event handler, called when a machine state changes
        :param mid:
        :return:
        """

        pass

        # check correct site etc...
        # if isinstance(mid, MachineRegistry.StatusChangedEvent):
        #    if self.mr.machines[mid.id].get(self.mr.regSite) == self.getSiteName():
        #        if mid.newStatus == self.mr.statusDisintegrating:
        #            self.modifyMachineStatus(mid.id, self.oao_state_power_off)
        #        # if new status is down, delete machine
        #        if mid.newStatus == self.mr.statusDown:
        #            self.modifyMachineStatus(mid.id, self.oao_delete)
