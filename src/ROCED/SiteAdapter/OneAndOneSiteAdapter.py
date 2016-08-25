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

from __future__ import unicode_literals, absolute_import

import datetime
import logging
import re
import sys
import time

try:
    from oneandone.client import OneAndOneService, Server, Hdd
except ImportError as import_error:
    self.logger.warning("Could not load OneAndOne client. ERROR: %s" % import_error)
    print(import_error)

from Core import Config
from SiteAdapter.Site import SiteAdapterBase
from Util.PythonTools import Caching
from Util.Logging import JsonLog

PY3 = sys.version_info > (3,)


class OneAndOneSiteAdapter(SiteAdapterBase):
    """
    site adapter for 1and1 cloud

    responsible for booting up and shutting down machines
    """

    # load values from config file
    # -------------------------------------------------------------------------
    configSiteLogger = "logger_name"
    configSiteName = "site_name"
    configSiteDescription = "site_description"
    configMachines = "machines"
    configApiToken = "api_token"
    configApplianceID = "appliance_id"
    configFirewallPolicyID = "firewall_policy_id"
    configMonitoringPolicyID = "monitoring_policy_id"
    configHddSize = "hdd_size"
    configVcores = "vcores"
    configCoresPerProcessor = "cores_per_processor"
    configRam = "ram"
    configPassword = "password"
    configServiceMachines = "service_machines"
    configTimeStart = "time_start"
    configTimeDrain = "time_drain"
    configTimeEnd = "time_end"
    configDelete = "delete_machines"
    configMaxMachinesPerCycle = "max_machines_per_cycle"
    configMaxMachines = "max_machines"
    configPrefix = "prefix"
    # -------------------------------------------------------------------------

    # keywords for machine registry
    # -------------------------------------------------------------------------
    reg_site_server_id = "reg_site_server_id"
    reg_site_server_status = "reg_site_server_status"
    reg_site_server_name = "reg_site_server_name"
    reg_site_server_condor_name = "reg_site_server_condor_name"
    reg_site_server_datacenter = "reg_site_server_datacenter"
    reg_site_server_network = "reg_site_server_network"
    reg_site_server_ip = "reg_site_server_ip"
    # -------------------------------------------------------------------------

    # OneAndOne specific keywords
    # -------------------------------------------------------------------------
    id = "id"
    ip = "ip"
    ips = "ips"
    status = "status"
    state = "state"
    name = "name"
    datacenter = "datacenter"
    servers = "servers"
    available_datacenter = "available_datacenters"
    network = "network"
    appliance = "appliance"
    state_deploying = "DEPLOYING"
    state_powering_on = "POWERING_ON"
    state_powered_on = "POWERED_ON"
    state_powering_off = "POWERING_OFF"
    state_powered_off = "POWERED_OFF"
    state_deleting = "DELETING"
    command_power_on = "POWER_ON"
    command_power_off = "POWER_OFF"
    command_delete = "DELETE"
    method_software = "SOFTWARE"
    method_hardware = "HARDWARE"
    # -------------------------------------------------------------------------

    # alternating index for load balancing between different data centers
    id_selector = 0

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
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary,
                                     description="Machine type")
        self.addCompulsoryConfigKeys(self.configApiToken, Config.ConfigTypeString,
                                     description="API Token for login in to OneAndOne Cloud")
        self.addCompulsoryConfigKeys(self.configApplianceID, Config.ConfigTypeString,
                                     description="Appliance to boot VM")
        self.addOptionalConfigKeys(self.configFirewallPolicyID, Config.ConfigTypeString,
                                   description="Firewall policy for VM",
                                   default="")
        self.addOptionalConfigKeys(self.configMonitoringPolicyID, Config.ConfigTypeString,
                                   description="Monitoring policy for VM",
                                   default=None)
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
        self.addOptionalConfigKeys(self.configServiceMachines, Config.ConfigTypeString,
                                   description="Service machine IDs",
                                   default=str())
        self.addOptionalConfigKeys(self.configPrefix, Config.ConfigTypeString,
                                   description="Prefix that should be used to name the VMs",
                                   default="roced-")
        self.addOptionalConfigKeys(self.configTimeStart, Config.ConfigTypeString,
                                   description="Define time that should be used to start VMs",
                                   default=None)
        self.addOptionalConfigKeys(self.configTimeDrain, Config.ConfigTypeString,
                                   description="Define time that should be used to set VMs to drain",
                                   default=None)
        self.addOptionalConfigKeys(self.configTimeEnd, Config.ConfigTypeString,
                                   description="Define time that should be used to shut down VMs",
                                   default=None)
        self.addOptionalConfigKeys(self.configDelete, Config.ConfigTypeBoolean,
                                   description="Shutdown instead of terminating VMs",
                                   default=False)
        self.addOptionalConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                   description="Number of machines booted per cycle", default=10)
        self.addOptionalConfigKeys(self.configMaxMachines, Config.ConfigTypeInt,
                                   description="limit amount of machines",
                                   default=None)

    def init(self):
        super(OneAndOneSiteAdapter, self).init()

        # disable urllib3 logging
        if PY3:
            urllib3_logger = logging.getLogger("urllib3.connectionpool")
        else:
            urllib3_logger = logging.getLogger("requests.packages.urllib3.connectionpool")
        urllib3_logger.setLevel(logging.CRITICAL)

        self.mr.registerListener(self)

        # set name of Site Adapter for ROCED output
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))

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
            self.logger.warning("Could not establish connection to 1&1 Cloud Site. ERROR: %s" % e)
            return
        return client

    def getOneAndOneMachines(self, oao_machine_state=None):
        """
        return a list of all running servers at 1and1
        :return: client.list_servers()
        """
        # Try getting a list of all machines running on 1and1 and raise an exception if it fails
        try:
            client = self.getOneAndOneClient()
            # get a list of all running machines
            machines = client.list_servers()
            # wait 1 second to not getting blocked by the firewall (1 request/second)
            time.sleep(1)
            # get a list of all private networks
            networks = client.list_private_networks()
            # wait again 1 second due to 1&1's firewall settings
            time.sleep(1)
        except Exception as exception:
            self.logger.warning("Could not establish connection to 1&1 Cloud Site. ERROR: %s" % exception)
            raise exception

        # filter for all machines with state X
        if oao_machine_state is None:
            oao_machines = {vm[self.id]: vm for vm in machines if
                            vm[self.id] not in self.getConfig(self.configServiceMachines).split()}
        # else return all machines
        else:
            oao_machines = {vm[self.id]: vm for vm in machines if
                            vm[self.id] not in self.getConfig(self.configServiceMachines).split() and vm[self.status][
                                self.state] == oao_machine_state}

        # set up a dictionary containing all the requested machine informations like attached private networks
        for id in oao_machines:
            for network in networks:
                netw_id = network[self.id]
                for server in network[self.servers]:
                    if id == server[self.id]:
                        oao_machines[id][self.network] = netw_id

        return oao_machines

    def getFreeIndex(self, machines, requested):
        """
        return a list of unused indices
        :param machines: all running machines
        :param requested: amount of requested machines
        :return: index

        """
        # generate a list of all used indices
        used_indices = [int(re.findall("roced-([0-9]+)$", machines[mid][self.name])[0]) for mid in machines]
        # return a list of all unused indices in the range between 0 and (amount of used indices + amount of requested)
        return ["{0:0>3}".format(index) for index in range(len(used_indices) + requested) if index not in used_indices]

    def getCondorName(self, mid):
        """
        This function generates the machine name that is used to sign in to HTCondor

        :param ip: ip address as 128.14.123.182
        :return: ip address as string 128014123182
        """
        return str().join(
            ["{0:0>3}".format(_ip_part) for _ip_part in self.mr.machines[mid][self.reg_site_server_ip].split(".")])

    @Caching(validityPeriod=8 * 60 * 60, redundancyPeriod=10 * 60 * 60)
    def generateIdList(self):
        """
        This function generates and caches all IDs for datacenters, networks and appliances
        :return:
        """
        client = self.getOneAndOneClient()
        try:
            # generate a list containing a dictionary of IDs for every appliance
            oao_appliances = [
                {self.id: appliance[self.id], self.datacenter: appliance[self.available_datacenter][0]}
                for
                appliance in client.list_appliances() if
                appliance[self.id] in self.getConfig(self.configApplianceID).split()]
            # the same for private networks
            oao_networks = [
                {self.id: network[self.id], self.datacenter: network[self.datacenter][self.id]} for
                network in client.list_private_networks()]
        except Exception as exception:
            self.logger.warning(exception)
            return
        id_list = {}
        # combine the two lists to one list containing all the information
        for appliance in oao_appliances:
            appliance_id = appliance[self.id]
            datacenter_id = appliance[self.datacenter]
            network_id = ""
            for network in oao_networks:
                if network[self.datacenter] == datacenter_id:
                    network_id = network[self.id]
            id_list[appliance_id] = {self.appliance: appliance_id, self.datacenter: datacenter_id,
                                     self.network: network_id}

        return id_list

    def getIDs(self, key=None, value=None):
        """
        return the corresponding image_id, datacenter_id and network_id
        :return:
        """
        id_dicts = self.generateIdList()
        # if key or value aren't set, use the id selector that alternates between all possibilities
        if key is None and value is None:
            id_list = []
            for appliance_id in id_dicts:
                id_list.append(id_dicts[appliance_id])
            id_dict = id_list[self.id_selector]
            appliance_id = id_dict[self.appliance]
            network_id = id_dict[self.network]
            datacenter_id = id_dict[self.datacenter]
            self.id_selector = (self.id_selector + 1) % len(id_list)
        # if key or value are set, search for the corresponding IDs
        else:
            for appliance_id in id_dicts:
                if id_dicts[appliance_id][key] == value:
                    appliance_id = id_dicts[appliance_id][self.appliance]
                    network_id = id_dicts[appliance_id][self.network]
                    datacenter_id = id_dicts[appliance_id][self.datacenter]
        return appliance_id, datacenter_id, network_id

    def modifyMachineStatus(self, mid, action, method=method_software):
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
        if action in [self.command_power_on, self.command_power_off]:
            try:
                client.modify_server_status(server_id=self.mr.machines[mid][self.reg_site_server_id], action=action,
                                            method=method)
            except Exception as exception:
                self.logger.warning(exception)
                raise exception
        # check if machine should be deleted
        if action is self.command_delete:
            try:
                client.delete_server(server_id=self.mr.machines[mid][self.reg_site_server_id])
            except Exception as exception:
                self.logger.warning(exception)
                raise exception
        time.sleep(1)
        return

    def assignPrivateNetwork(self, mid):
        """ Assign VM to a private network
        The VMs have to be assigned to a private network manually due to the missing possibility to assign it while
        requesting the VM.
        :param mid:
        :param netw_id:
        :return:
        """

        # get 1and1 client
        client = self.getOneAndOneClient()
        try:
            client.assign_private_network(server_id=self.mr.machines[mid][self.reg_site_server_id],
                                          private_network_id=self.mr.machines[mid][self.reg_site_server_network])
        except Exception as exception:
            self.logger.warning(exception)
            raise exception

        return

    @property
    def runningMachines(self):
        """
        Remapping of runningMachines to cloudOccupyingMachines
        Both are defined in SiteAdapterBase in Site.py
        :return:
        """
        return self.cloudOccupyingMachines

    def spawnMachines(self, machineType, requested):
        """
        spawn VMs for 1and1 cloud service
        :param machineType:
        :param requested:
        :return: count
        """
        # check if machine type is requested machine type
        if not machineType == list(self.getConfig(self.configMachines).keys())[0]:
            return

        # check if spawning is allowed
        start_time = datetime.datetime.strptime(self.getConfig(self.configTimeStart), "%H:%M").time()
        drain_time = datetime.datetime.strptime(self.getConfig(self.configTimeDrain), "%H:%M").time()
        current_time = datetime.datetime.now().time()
        # if current_time < start_time and current_time > drain_time:
        if drain_time < current_time < start_time:
            self.logger.info("Request not permitted due to limitations: Booting VMs begins at %s" % start_time)
            return

        # check if requested number of VMs is higher than the allowed number of machines per cycle
        # and if so, limit it to the allowed number
        if requested > self.getConfig(self.configMaxMachinesPerCycle):
            self.logger.info("Request exceeds maximum number of allowed machines per cycle on this site (%d>%d)!" %
                             (requested, self.getConfig(self.configMaxMachinesPerCycle)))
            requested = self.getConfig(self.configMaxMachinesPerCycle)
            self.logger.info("Will spawn %d machines." % requested)

        # get list of all shut down machines
        # to start the shutdown servers at first
        machines_down = self.mr.getMachines(status=self.mr.statusDown)
        if requested > len(machines_down) > 0:
            requested = len(machines_down)
        for mid in list(machines_down.keys())[0:requested]:
            # start the machine
            try:
                vm = machines_down[mid]
                self.modifyMachineStatus(mid, self.command_power_on)
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
                vm[self.reg_site_server_status] = self.state_powered_off
            except Exception as excpetion:
                self.logger.info(excpetion)
                return
            requested -= 1

        # check if any other machines are needed
        if requested == 0:
            return

        # if so check if the the amount of requested machines would exceed the number of allowed machines
        # and limit if necessary
        if (requested + self.cloudOccupyingMachinesCount) > self.getConfig(self.configMaxMachines):
            self.logger.info(
                "Number of requested machines would exceed number of allowed machines (%d>%d). Will request %d machines" % requested + self.cloudOccupyingMachinesCount,
                self.getConfig(self.configMaxMachines),
                self.getConfig(self.configMaxMachines) - self.cloudOccupyingMachinesCount)
            requested = self.getconfig(self.configMaxMachines) - self.cloudOccupyingMachinesCount

        # get a list of unused indices
        try:
            oao_machines = self.getOneAndOneMachines()
        except Exception as exception:
            return
        free_index = self.getFreeIndex(oao_machines, requested)

        # request new machines
        for count in range(requested):
            # assign vm name based on the generated free index list
            vm_name = self.getConfig(self.configPrefix) + free_index.pop(0)
            # get the IDs
            appliance_id, datacenter_id, network_id = self.getIDs()
            # initialize the machine
            server = Server(name=vm_name,
                            appliance_id=appliance_id,
                            datacenter_id=datacenter_id,
                            vcore=self.getConfig(self.configVcores),
                            cores_per_processor=self.getConfig(self.configCoresPerProcessor),
                            ram=self.getConfig(self.configRam),
                            firewall_policy_id=self.getConfig(self.configFirewallPolicyID),
                            monitoring_policy_id=self.getConfig(self.configMonitoringPolicyID),
                            power_on=False
                            )
            # init Hdd
            hdd = Hdd(size=self.getConfig(self.configHddSize), is_main=True)
            hdds = [hdd]
            # request machine at 1and1
            try:
                client = self.getOneAndOneClient()
                vm = client.create_server(server=server, hdds=hdds)
            except Exception as exception:
                self.logger.warning("Could not start server on OneAndOne Cloud Service:\n%s\nTrying again next cycle."
                                    % exception)
                return

            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.machines[mid][self.reg_site_server_name] = vm[self.name]
            self.mr.machines[mid][self.reg_site_server_id] = vm[self.id]
            self.mr.machines[mid][self.reg_site_server_status] = vm[self.status][self.state]
            self.mr.machines[mid][self.reg_site_server_datacenter] = datacenter_id
            self.mr.machines[mid][self.reg_site_server_network] = network_id
            self.mr.machines[mid][self.reg_site_server_condor_name] = str()

            # update machine status
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        return

    def terminateMachines(self, machineType, count):
        pass

    def manage(self, cleanup=False):
        """
        managing machine states that change dependant of the state changes on 1and1 cloud site run once per cycle
        :return:
        """
        try:
            oao_machines = self.getOneAndOneMachines()
        except Exception:
            return

        # loop over all machines in machine registry
        for mid in self.mr.getMachines(self.siteName):
            machine = self.mr.machines[mid]

            # remove the corresponding machine from the 1and1 machine list
            try:
                oao_machine = oao_machines.pop(machine[self.reg_site_server_id])
            except KeyError:
                self.mr.removeMachine(mid)
                continue

            # check for status which is handled by integration adapter
            if machine[self.mr.regStatus] in [self.mr.statusIntegrating]:
                continue

            # manage machine in status booting
            if machine[self.mr.regStatus] == self.mr.statusBooting:
                # if the 1and1 machine is in status powered
                if oao_machine[self.status][self.state] == self.state_powered_off:
                    # check if a private network is assigned
                    # if not then assign the right network
                    if self.network not in oao_machine:
                        try:
                            self.assignPrivateNetwork(mid=mid)
                        except Exception:
                            break
                    # if the private network is assigned to the 1and1 machine, add it to the machine registry
                    elif self.reg_site_server_network not in machine:
                        machine[self.reg_site_server_network] = machine[self.network]
                    # if everything is done, start the machine
                    else:
                        try:
                            self.modifyMachineStatus(mid=mid, action=self.command_power_on)
                        except Exception:
                            break
                        machine[self.reg_site_server_status] = self.state_powering_on

                # it the 1and1machine is powered on, update the ip address, state and the condor name
                # at the end update the machine status in the machine registry
                elif oao_machine[self.status][self.state] == self.state_powered_on:
                    machine[self.reg_site_server_status] = self.state_powered_on
                    machine[self.reg_site_server_ip] = oao_machine[self.ips][0][self.ip]
                    machine[self.reg_site_server_condor_name] = self.getCondorName(mid=mid)
                    self.mr.updateMachineStatus(mid=mid, newStatus=self.mr.statusUp)

            # manage machine in status working or pending disintegration
            elif machine[self.mr.regStatus] == self.mr.statusWorking or machine[
                self.mr.regStatus] == self.mr.statusPendingDisintegration:
                # if the 1and1 machine is powered on and it is later than "stop time"
                # move the machine to disintegrating
                if oao_machine[self.status][self.state] == self.state_powered_on:
                    start_time = datetime.datetime.strptime(self.getConfig(self.configTimeStart), "%H:%M").time()
                    stop_time = datetime.datetime.strptime(self.getConfig(self.configTimeEnd), "%H:%M").time()
                    drain_time = datetime.datetime.strptime(self.getConfig(self.configTimeDrain), "%H:%M").time()
                    current_time = datetime.datetime.now().time()
                    if stop_time < current_time < start_time:
                        self.mr.updateMachineStatus(mid=mid, newStatus=self.mr.statusDisintegrating)
                # if the 1and1 machine is powering off or powered off, move it to disintegrating
                elif oao_machine[self.status][self.state] in [self.state_powering_off, self.state_powered_off]:
                    machine[self.reg_site_server_status] = oao_machine[self.status][self.state]
                    self.mr.updateMachineStatus(mid=mid, newStatus=self.mr.statusDisintegrating)

            # manage machine in status disintegrating
            elif machine[self.mr.regStatus] == self.mr.statusDisintegrating:
                # if the machine is still powered on, shut it off
                if oao_machine[self.status][self.state] == self.state_powered_on:
                    try:
                        self.modifyMachineStatus(mid=mid, action=self.command_power_off)
                    except Exception:
                        break
                    machine[self.reg_site_server_status] = self.state_powering_off

            # manage machine in status disintegrated
            elif machine[self.mr.regStatus] == self.mr.statusDisintegrated:
                # if the 1and1 machine is powered off, set it to status down
                if oao_machine[self.status][self.state] == self.state_powered_off:
                    machine[self.reg_site_server_status] = self.state_powered_off
                    self.mr.updateMachineStatus(mid=mid, newStatus=self.mr.statusDown)

            # manage machine in status down
            elif machine[self.mr.regStatus] == self.mr.statusDown:
                # if the 1and1 machine is powered off, and the delete option is enabled, delete the 1and1 machine
                if oao_machine[self.status][self.state] == self.state_powered_off:
                    if self.getConfig(self.configDelete) is True:
                        try:
                            self.modifyMachineStatus(mid=mid, action=self.command_delete)
                        except Exception:
                            break
                        machine[self.reg_site_server_status] = self.state_deleting

        # add all machines remaining in machine list from 1&1
        for oao_machine in oao_machines:
            # check if machine is already in machine registry
            if oao_machine in [machine[self.reg_site_server_id] for machine in \
                               self.mr.getMachines(self.siteName).values()]:
                continue

            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = self.getConfig(self.configMachines).keys()[0]  # machineType
            self.mr.machines[mid][self.reg_site_server_name] = oao_machines[oao_machine][self.name]
            self.mr.machines[mid][self.reg_site_server_id] = oao_machines[oao_machine][self.id]
            self.mr.machines[mid][self.reg_site_server_status] = oao_machines[oao_machine][self.status][self.state]
            self.mr.machines[mid][self.reg_site_server_datacenter] = oao_machines[oao_machine][self.datacenter][self.id]
            self.mr.machines[mid][self.reg_site_server_network] = \
                self.getIDs(key=self.datacenter, value=oao_machines[oao_machine][self.datacenter][self.id])[2]
            self.mr.machines[mid][self.reg_site_server_condor_name] = ""

            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # add current amounts of machines to Json log file
        # self.logger.info("Current machines running at %s: %d" % (self.siteName, self.runningMachinesCount))
        self.logger.info("Current machines running at %s: %d"
                         % (self.siteName, self.runningMachinesCount[
            list(self.getConfig(self.configMachines).keys())[0]]))  # ["vm-default"]))
        json_log = JsonLog()
        json_log.addItem(self.siteName, "machines_requested",
                         int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                             len(self.getSiteMachines(status=self.mr.statusUp)) +
                             len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem(self.siteName, "condor_nodes",
                         len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem(self.siteName, "condor_nodes_draining",
                         len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))

    def onEvent(self, mid):
        """
        event handler, called when a machine state changes
        :param mid:
        :return:
        """
        pass
