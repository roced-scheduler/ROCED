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

from __future__ import unicode_literals
import logging
import re
import time
import sys

from oneandone.client import OneAndOneService, Server, Hdd

from Core import Config, MachineRegistry
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog

PY3 = sys.version_info > (3,)


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
    configApplianceID = "appliance_id"
    configFirewallPolicyID = "firewall_policy_id"
    configMonitoringPolicyID = "monitoring_policy_id"
    configHddSize = "hdd_size"
    configVcores = "vcores"
    configCoresPerProcessor = "cores_per_processor"
    configRam = "ram"
    configPassword = "password"
    configPrivateNetworkID = "private_network_id"
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

    # keywords for server status in 1and1 API
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
        super(OneAndOneSiteAdapter, self).__init__()

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
        self.addCompulsoryConfigKeys(self.configApplianceID, Config.ConfigTypeString,
                                     description="Appliance to boot VM")
        self.addOptionalConfigKeys(self.configFirewallPolicyID, Config.ConfigTypeString,
                                   description="Firewall policy for VM",
                                   default="")
        self.addOptionalConfigKeys(self.configMonitoringPolicyID, Config.ConfigTypeString,
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
        self.addOptionalConfigKeys(self.configPrivateNetworkID, Config.ConfigTypeString,
                                   description="Private network",
                                   default=None)
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
        super(OneAndOneSiteAdapter, self).init()

        # disable urllib3 logging
        if PY3:
            urllib3_logger = logging.getLogger("urllib3.connectionpool")
        else:
            urllib3_logger = logging.getLogger("requests.packages.urllib3.connectionpool")
        urllib3_logger.setLevel(logging.CRITICAL)

        self.mr.registerListener(self)

    def getOneAndOneClient(self):
        """
        initialize 1and1 client
        :return: OneAndOneService()
        """

        # Try initializing the 1and1 client ant return it
        try:
            client = OneAndOneService(self.getConfig(self.configApiToken))
        # If initializing failed return nothing
        except Exception as e:
            self.logger.warning("Could not establish connection to 1&1 Cloud Site. ERROR:")
            self.logger.warning(str(e))
            return

        return client

    def getOneAndOneMachines(self):
        """
        return a list of all running servers at 1and1
        :return: client.list_servers()
        """

        # Try getting a list of all machines running on 1and1
        try:
            client = self.getOneAndOneClient()
            tmp = client.list_servers()
        # if it fails raise exception and return nothing
        except Exception as e:
            self.logger.warning("Could not establish connection to 1&1 Cloud Site. ERROR:")
            self.logger.warning(str(e))
            return

        # build a dictionary containing the machines
        servers = {}
        for server in tmp:
            # set ID as keyword
            servers[server[self.oao_id]] = {}
            for key in server:
                # add all information to dict if it is not the ID
                if key != self.oao_id:
                    servers[server[self.oao_id]][key] = server[key]

        # return the dictionary
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
            number_found = re.findall("roced-([0-9]+)$", servers[server][self.oao_name])
            if len(number_found) > 0:
                used_indices.append(int(number_found[0]))

        # generate a list of unesed indices if there are used ones
        if len(used_indices) > 0:
            i = 0
            new_indices = []
            while requested > 0:
                # if index is already used move on with the next index
                if i in used_indices:
                    i += 1
                # else add index to new indices and move on with the next index
                else:
                    new_indices.append(i)
                    used_indices.append(i)
                    requested -= 1
                    i += 1
        # otherwise generate a list [0..(requested-1)]
        else:
            new_indices = list(range(requested))

        # return the unused indices
        return new_indices

    def generateCondorName(self, ip):
        """
        This function generates the machine name that is used to sign in to HTCondor

        :param ip: ip address as 128.14.123.182
        :return: ip_addr - string as 128014123182
        """
        ip_string = ""
        for ip_part in re.split(r'\.', ip):
            ip_string += "{0:0>3}".format(ip_part)

        return ip_string

    def getRunningMachines(self):
        """
        Returns a dictionary containing all running machines

        The number of running machines needs to be recalculated when using status integrating and pending
        disintegration. Machines pending disintegration are still running an can accept new jobs. Machines integrating
        are counted as running machines by default.

        :return: machineList
        """

        # get all machines running on site
        myMachines = self.getSiteMachines()
        machineList = dict()

        # generate empty list for machines running on 1and1
        machineList[list(self.getConfig(self.configMachines).keys())[0]] = []

        # filter for machines in status booting, up, integrating, working or pending disintegration
        for (k, v) in myMachines.items():
            if v.get(self.mr.regStatus) in [self.mr.statusBooting, self.mr.statusUp,
                                            self.mr.statusIntegrating, self.mr.statusWorking,
                                            self.mr.statusPendingDisintegration]:
                # add machine to previously defined list
                machineList[v[self.mr.regMachineType]].append(k)

        return machineList

    def spawnMachines(self, machineType, requested):
        """
        spawn VMs for 1and1 cloud service
        :param machineType:
        :param requested:
        :return: count
        """

        # check if machine type is requested machine type
        if not machineType == list(self.getConfig(self.configMachines).keys())[0]:
            return 0

        # get 1and1 client and machine list
        client = self.getOneAndOneClient()
        servers = self.getOneAndOneMachines()

        # find all unused indices to spawn machines
        index = self.getIndex(servers, requested)
        # loop over all requested machines
        for count in range(requested):
            # set machine name
            vm_name = "roced-" + "{0:0>3}".format(index.pop(0))  # str(index.pop(0))
            # create machine with all required information
            server = Server(name=vm_name,
                            appliance_id=self.getConfig(self.configApplianceID),
                            vcore=self.getConfig(self.configVcores),
                            cores_per_processor=self.getConfig(self.configCoresPerProcessor),
                            ram=self.getConfig(self.configRam),
                            firewall_policy_id=self.getConfig(self.configFirewallPolicyID),
                            monitoring_policy_id=self.getConfig(self.configMonitoringPolicyID),
                            power_on=(not self.getConfig(self.configPrivateNetworkID))
                            )

            # create HDD with requested size
            hdd = Hdd(size=self.getConfig(self.configHddSize), is_main=True)
            hdds = [hdd]

            # try booting up the machine
            try:
                vm = client.create_server(server=server, hdds=hdds)
            # if it failes raise exception and continue with next machine
            except Exception as e:
                self.logger.warning("Could not start server on OneAndOne Cloud Service")
                self.logger.warning(str(e))
                continue

            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.machines[mid][self.reg_site_server_name] = vm[self.oao_name]
            self.mr.machines[mid][self.reg_site_server_id] = vm[self.oao_id]
            self.mr.machines[mid][self.reg_site_server_status] = vm[self.oao_status][self.oao_state]

            # update machine status
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # all machines booted
        # return
        return

    def modifyMachineStatus(self, mid, action, method=oao_method_software):
        """
        This function modifies the machine status on 1and1 Cloud site.

        :param mid:
        :param action: shut down or delete
        :param method: hardware method or software method
        :return:
        """

        # get 1and1 client
        client = self.getOneAndOneClient()

        # check if machine status is on or off, if so shut down the machine
        if action in [self.oao_state_power_on, self.oao_state_power_off]:
            try:
                client.modify_server_status(
                    server_id=self.mr.machines[mid][self.reg_site_server_id],
                    action=action,
                    method=method)
            except Exception as e:
                self.logger.info("Machine %s already shutting down."
                                 % self.mr.machines[mid][self.reg_site_server_name])
                self.logger.warning(e.message)
        # check if machine should be deleted
        if action == self.oao_delete:
            # if so, try to delete the machine on 1and1 Cloud Site
            try:
                client.delete_server(server_id=self.mr.machines[mid][self.reg_site_server_id])
            # else raise exception
            # this could happen, if machine is arleady deleted
            except Exception as e:
                self.logger.info("Machine %s already deleted."
                                 % self.mr.machines[mid][self.reg_site_server_name])
                # self.logger.warning("Could not establish connection to 1&1 Cloud Site")
                self.logger.warning(e.message)

        # wait 1 second with the next action due to 1and1's firewall policies (1 request/second)
        time.sleep(1)
        return

    def assignPrivateNetwork(self, mid, netw_id):
        """ Assign VM to a private network

        The VMs have to be assigned to a private network manually due to the missing possibility to assign it while
        requesting the VM.

        :param mid:
        :param netw_id:
        :return:
        """

        # get 1and1 client
        client = self.getOneAndOneClient()

        client.assign_private_network(server_id=self.mr.machines[mid][self.reg_site_server_id],
                                      private_network_id=netw_id)
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
        if oao_machines is None:  # or (len(oao_machines) == 0):
            return

        # loop over all machines on 1and1 Cloud Site and already in machine registry
        for mid in self.mr.getMachines(self.siteName):
            machine_ = self.mr.machines[mid]
            # TODO: is this needed?
            # check if machine is already deleted on site
            if not machine_[self.reg_site_server_id] in oao_machines:
                if machine_[self.mr.regStatus] != self.mr.statusDown:
                    # if so remove machine from machine registry
                    self.mr.removeMachine(mid)
                    continue

            # down -> removed from machine registry
            # if machine status in machine registry is down
            if machine_[self.mr.regStatus] == self.mr.statusDown:
                # if machine is not in 1and1 Cloud Site list
                if not machine_[self.reg_site_server_id] in oao_machines:
                    # remove machine from machine registry
                    self.mr.removeMachine(mid)
                    continue
                # else check if machine is still in 1and1 Cloud Site list
                # TODO: could be handled by else condition?
                elif machine_[self.reg_site_server_id] in oao_machines:
                    # delete machine on 1and1 Cloud Site
                    self.modifyMachineStatus(mid, self.oao_delete)
                    del oao_machines[machine_[self.reg_site_server_id]]

            # check if condor name is set
            if not self.reg_site_server_condor_name in machine_:
                # if not check if ip is already available
                if (oao_machines[machine_[self.reg_site_server_id]]["ips"] is not None and
                            oao_machines[machine_[self.reg_site_server_id]]["ips"][0][
                                "ip"] is not None):
                    # if so, set ip as condor name, remove "." from ip
                    self.mr.machines[mid][self.reg_site_server_condor_name] = (
                        self.generateCondorName(oao_machines[machine_[self.reg_site_server_id]]
                                                ["ips"][0]["ip"]))
                    # oao_machines[self.mr.machines[mid][self.reg_site_server_id]]["ips"][0]["ip"].replace(".", "")

            # check for status which is handled by integration adapter
            if machine_[self.mr.regStatus] in [self.mr.statusIntegrating,
                                               self.mr.statusWorking,
                                               self.mr.statusPendingDisintegration]:
                del oao_machines[machine_[self.reg_site_server_id]]

            # booting -> up
            # check if machine status is booting
            try:
                oao_state = oao_machines[machine_[self.reg_site_server_id]][self.oao_status][
                    self.oao_state]
            except KeyError:
                pass
            if machine_[self.mr.regStatus] == self.mr.statusBooting:
                # machines that are powered off have to be assigned to a private network
                if oao_state == self.oao_state_powered_off:
                    # assign the network
                    self.assignPrivateNetwork(mid, self.getConfig(self.configPrivateNetworkID))
                    # boot up the machine afterwards
                    self.modifyMachineStatus(mid, self.oao_state_power_on)
                # check if machine status on 1and1 Cloud Site is already powered on
                elif oao_state == self.oao_state_powered_on:
                    # if so, update machine registry status to up
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    # and write 1and1 Cloud Site status to machine registry
                    # TODO: is this needed by any other function?
                    self.mr.machines[mid][self.reg_site_server_status] = (
                        oao_machines[machine_[self.reg_site_server_id]][self.oao_status])
                # remove from 1and1 machine list
                del oao_machines[machine_[mid][self.reg_site_server_id]]

            # disintegrating
            # check if machine is in status disintegrating
            if machine_[self.mr.regStatus] == self.mr.statusDisintegrating:
                # if so, power off machine
                # machine gets moved to disintegrated when it disappears from condor list
                # -> handled by IntegrationAdapter
                self.modifyMachineStatus(mid, self.oao_state_power_off)
                del oao_machines[machine_[self.reg_site_server_id]]

            # disintegrated -> down
            # check if machine is disintegrated
            if machine_[self.mr.regStatus] == self.mr.statusDisintegrated:
                # if so, check if machine is already powered off on 1and1 Cloud Site
                if oao_state == self.oao_state_powered_off:
                    # if so, update machine status to down
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                del oao_machines[machine_[self.reg_site_server_id]]

        # add all machines remaining in machine list from 1&1
        for vm in oao_machines:
            # check if machine is quid server
            if (self.getConfig(self.configSquid) is not None and
                        oao_machines[vm][self.oao_name] in self.getConfig(self.configSquid)):
                continue

            # check if machine is already in machine registry
            if vm in self.mr.getMachines(self.siteName):
                continue
            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = self.oao  # machineType
            self.mr.machines[mid][self.reg_site_server_name] = oao_machines[vm][self.oao_name]
            self.mr.machines[mid][self.reg_site_server_id] = oao_machines[vm][self.oao_id]
            self.mr.machines[mid][self.reg_site_server_status] = (
                oao_machines[vm][self.oao_status][self.oao_state])

            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # add current amounts of machines to Json log file
        self.logger.info("Current machines running at %s: %d"
                         % (self.siteName, self.runningMachinesCount[
            self.getConfig(self.configMachines).keys()[0]]))  # ["vm-default"]))
        json_log = JsonLog()
        json_log.addItem(self.siteName, 'machines_requested',
                         int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                             len(self.getSiteMachines(status=self.mr.statusUp)) +
                             len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem(self.siteName, 'condor_nodes',
                         len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem(self.siteName, 'condor_nodes_draining',
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
