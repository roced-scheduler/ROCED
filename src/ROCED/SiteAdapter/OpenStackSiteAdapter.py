# ==============================================================================
#
# Copyright (c) 2010, 2011, 2015, 2016 by Guenther Erli, Frank Fischer
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
import uuid

try:
    from novaclient.client import Client
    from novaclient.v1_1.hypervisors import HypervisorManager
except ImportError as e:
    print(e)

from Core import MachineRegistry, Config
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog


class OpenStackSiteAdapter(SiteAdapterBase):
    """
    Site Adapter for OpenStack

    responsible for spawning new machines and setting status from booting to up,
    from up to integrating, from pending disintegrating to disintegrating,
    from disintegrating to disintegrated and from disintegrated to down
    """

    # Name of Site Adapter in ROCED output
    configSiteLogger = "logger_name"

    # OpenStack connection related information
    configKeystoneServer = "openstack_Keystone_Server"
    configUser = "openstack_User"
    configPass = "openstack_Password"
    configTenant = "openstack_Tenant"
    configTimeout = "openstack_Timeout"

    configAdmin = "openstack_admin"
    configAdminPass = "openstack_admin_password"
    configAdminTenant = "openstack_admin_tenant"

    # machine specific settings
    # configMachineName = "machine_name"
    configMachines = "machines"
    # configMachineType = "machine_type"
    configMaxMachinesPerCycle = "max_machines_per_cycle"
    configMaxMachines = "max_machines"
    configUseTime = "openstack_use_time"
    configMachinePercentage = "openstack_usage_daytime"
    configDay = "openstack_daytime"
    configNight = "openstack_nighttime"
    configImage = "openstack_image_id"
    configFlavor = "openstack_flavor_id"
    configKeypair = "openstack_keypair_id"
    configNetwork = "openstack_network_id"
    configUserData = "user_data"

    # name, id and status of VMs at OpenStack
    reg_site_server_name = "reg_site_server_name"
    reg_site_server_condor_name = "reg_site_server_condor_name"
    reg_site_server_id = "reg_site_server_id"
    reg_site_server_status = "reg_site_server_status"
    reg_site_server_hypervisor = "reg_site_server_hypervisor"

    # OpenStack state declarations, same as in OpenStack dashboard
    reg_site_server_status_active = "ACTIVE"
    reg_site_server_status_error = "ERROR"
    reg_site_server_status_shutoff = "SHUTOFF"

    reg_status_change_history = "state_change_history"

    def __init__(self):
        """
        Load config keys from config files

        :return:
        """
        super(OpenStackSiteAdapter, self).__init__()

        # load Site Adapter name for ROCED output from config file
        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of Site Adapter", default="OS_Site")

        # load OpenStack login data from config file
        self.addCompulsoryConfigKeys(self.configKeystoneServer, Config.ConfigTypeString,
                                     "OpenStack server address")
        self.addCompulsoryConfigKeys(self.configUser, Config.ConfigTypeString,
                                     "OpenStack user name")
        self.addCompulsoryConfigKeys(self.configPass, Config.ConfigTypeString, "OpenStack password")
        self.addCompulsoryConfigKeys(self.configTenant, Config.ConfigTypeString,
                                     "OpenStack tenant information")
        self.addOptionalConfigKeys(self.configTimeout, Config.ConfigTypeInt,
                                   description="OpenStack connection timeout", default=300)
        self.addOptionalConfigKeys(self.configAdmin, Config.ConfigTypeString,
                                   description="OpenStack admin user name",
                                   default=None)
        self.addOptionalConfigKeys(self.configAdminPass, Config.ConfigTypeString,
                                   description="OpenStack admin password", default=None)
        self.addOptionalConfigKeys(self.configAdminTenant, Config.ConfigTypeString,
                                   description="OpenStack admin tenant", default=None)

        # TODO check if this is really needed...
        # init ConfigMachineType with empty dictionary
        # self.setConfig(self.configMachineType, dict())
        # self.addCompulsoryConfigKeys(self.configMachineType, Config.ConfigTypeDictionary)

        # TODO check if this is really needed...
        # init ConfigMachines with empty dictionary
        # self.setConfig(self.configMachines, dict())
        # self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary, "Machine dictionary")
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary, "Machine name")

        # load machine specific settings from config file
        self.addOptionalConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                   description="Number of machines booted per cycle", default=5)
        self.addOptionalConfigKeys(self.configMaxMachines, Config.ConfigTypeInt,
                                   description="Number of machines allowed on site", default=22)
        self.addOptionalConfigKeys(self.configDay, Config.ConfigTypeString,
                                   description="Defines when day begins",
                                   default="08:00")
        self.addOptionalConfigKeys(self.configNight, Config.ConfigTypeString,
                                   description="Defines when night begins",
                                   default="20:00")
        self.addOptionalConfigKeys(self.configUseTime, Config.ConfigTypeBoolean,
                                   description="Consider different amounts of machines at day/night",
                                   default=False)
        self.addOptionalConfigKeys(self.configMachinePercentage, Config.ConfigTypeFloat,
                                   description="Percentage of machines loaded at day", default=0.5)
        self.addCompulsoryConfigKeys(self.configImage, Config.ConfigTypeString,
                                     "Defines the image to be loaded")
        self.addOptionalConfigKeys(self.configFlavor, Config.ConfigTypeString,
                                   description="Defines the flavor to be selected",
                                   default="m1.large")
        self.addCompulsoryConfigKeys(self.configNetwork, Config.ConfigTypeString,
                                     description="Defines the network to be selected")
        self.addOptionalConfigKeys(self.configKeypair, Config.ConfigTypeString,
                                   description="Defines the keypair that shoud be used",
                                   default=None)
        self.addOptionalConfigKeys(self.configUserData, Config.ConfigTypeString,
                                   description="Defines the storage path of user data scripts for cloud-init",
                                   default=None)

    def init(self):
        super(OpenStackSiteAdapter, self).init()

        # if admin access is enabled, get number of max machines from number of hypervisors
        if self.getConfig(self.configUseTime) is True and not self.getConfig(
                self.configMaxMachines):
            self.setConfig(self.configMaxMachines, self.getMaxMachines())

        # set name of Site Adapter for ROCED output
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))

        # disable urllib3 logging
        urllib3_logger = logging.getLogger("requests.packages.urllib3.connectionpool")
        urllib3_logger.setLevel(logging.CRITICAL)

        self.mr.registerListener(self)

        self._machineType = list(self.getConfig(self.configMachines).keys())[0]

    def getMaxMachines(self):
        """
        Get maximum number of allowed VMs on site

        :return: maxMachines
        """
        # if admin access is enabled, get number of max machines from number of hypervisors
        if self.getConfig(self.configUseTime):
            flavor_cores = \
                self.__getNovaApi().flavors.find(name=self.getConfig(self.configFlavor)).__dict__[
                    "vcpus"]
            host_list = HypervisorManager(
                self.__getNovaApi(self.getConfig(self.configUseTime))).list()
            maxMachines = 0
            for host in host_list:
                maxMachines = maxMachines + (host.__dict__["vcpus"] / flavor_cores)
            return maxMachines

    @property
    def runningMachines(self):
        """
        Returns a dictionary containing all running machines

        The number of running machines needs to be recalculated when using status integrating and
        pending disintegration. Machines pending disintegration are still running and can accept
        new jobs. Machines integrating are counted as running machines by default.

        :return: machineList
        """
        return self.cloudOccupyingMachines

    def spawnMachines(self, machineType, requested):
        """Function to spawn requested amount of machines

        This function spawns the requested number of new machines, unless the number exceeds the
        maximal number of machines allowed to spawn per management cycle. The requested amount of
        machines is also limited by SiteAdapterBase.applyMachineDecision(), so that the number of
        requested + running machines will not exceed the number of overall allowed machines.

        If time-dependent spawning of machines is activated, it will not spawn more machines
        in total than (percentage of machines at day) * (number of machines allowed per site).

        If spawning is not possible or fails (due to connection failures, OpenStack quota limits,..)
        it does nothing (ROCED will try to spawn new machines in next managment cycle).

        :param machineType:
        :param requested: requested number of machines
        :return: count
        """

        if not machineType == self._machineType:  # self.getConfig(self.configMachines):
            return 0

        try:
            nova = self.__getNovaApi()

            # important to give a specifc network due to bug in nova api:
            netw = self.getConfig(self.configNetwork)
            fls = self.getConfig(self.configFlavor)  # nova.flavors.find(name=self.getConfig(self.configFlavor))
            img = self.getConfig(self.configImage)  # nova.images.find(name=self.getConfig(self.configImage))
            key = self.getConfig(self.configKeypair)  # nova.keypairs.list()
            name_prefix = str(self.siteName + "-")
            print(self.getConfig(self.configUserData))
            user_data = open(self.getConfig(self.configUserData), "r")
            print(user_data)
            daytime = datetime.datetime.strptime(self.getConfig(self.configDay), "%H:%M")
            nighttime = datetime.datetime.strptime(self.getConfig(self.configNight), "%H:%M")

            ###
            # Time dependent adaptions:
            #   Daytime:
            #   - number of allowed machines = (percentage * max_machines)
            #   - if requested machines + running machines > number of allowed machines:
            #       number of requested machines = limit
            #   Nighttime:
            #   - nothing to do
            ###
            if self.getConfig(self.configUseTime):
                max_machine_count = int(self.getConfig(self.configMaxMachines) *
                                        self.getConfig(self.configMachinePercentage))
                if daytime.time() <= datetime.datetime.now().time() <= nighttime.time():
                    if (requested + self.cloudOccupyingMachinesCount) > max_machine_count:
                        self.logger.info("Request exceeds maximum number of allowed machines for daytime (%d>%d)!" %
                                         (requested + self.cloudOccupyingMachinesCount, max_machine_count))
                        requested = max_machine_count - self.cloudOccupyingMachinesCount
                        self.logger.info("Will spawn %d machines." % requested)

            # requested amount > allowed number of machines per cycle
            if requested > self.getConfig(self.configMaxMachinesPerCycle):
                self.logger.info("Request exceeds maximum number of allowed machines per cycle on this site (%d>%d)!" %
                                 (requested, self.getConfig(self.configMaxMachinesPerCycle)))
                # set requested equals the number of machines per cycle
                requested = self.getConfig(self.configMaxMachinesPerCycle)
                self.logger.info("Will spawn %d machines." % requested)

            ###
            # spawn machines
            ###
            for count in range(requested):
                # init new machine in machine registry
                mid = name_prefix + str(uuid.uuid4())
                self.mr.newMachine(mid)
                # spawn machine at site
                if not key is None:
                    vm = nova.servers.create(mid, img, fls, nics=[{"net-id": netw}], userdata=user_data,
                                             key_name=key)
                else:
                    vm = nova.servers.create(mid, img, fls, nics=[{"net-id": netw}], userdata=user_data)

                # set some machine information in machine registry
                self.mr.machines[mid][self.mr.regSite] = self.siteName
                self.mr.machines[mid][self.mr.regSiteType] = self.siteType
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.reg_site_server_id] = vm.id
                self.mr.machines[mid][self.reg_site_server_status] = vm.status
                self.mr.machines[mid][self.reg_site_server_condor_name] = mid
                self.mr.machines[mid][self.reg_site_server_name] = mid
                # if admin account is set, also set the hypervisor
                if self.getConfig(self.configUseTime):
                    # time.sleep(1)
                    self.mr.machines[mid][self.reg_site_server_hypervisor] = self.__getHypervisor(
                        vm.id)

                # TODO: set machine information, like openstack id
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)

            # all machines booted
            return requested

        # if spawning fails, do nothing
        except Exception as e:
            self.logger.warning("Spawning machines failed. Exception: %s" % e)

    def __openstackTerminateMachines(self, mid):
        """Terminate machines in OpenStack

        This function will terminate/delete the machine in OpenStack, which means it will clean up
        the used resources in OpenStack, and remove the machine from the machine registry.

        If terminating the machine fails, for example due to connection failures, nothing happens
        until the next management cycle.

        :param mid: id of machine to terminate
        :return:
        """
        try:
            # initialize the NovaAPI
            nova = self.__getNovaApi()
            # send terminate/delete command
            nova.servers.find(id=self.mr.machines[mid.id][self.reg_site_server_id]).delete()
            # remove from machine registry
            self.mr.removeMachine(mid)
        except Exception:
            pass

    def __openstackStopMachine(self, mid):
        """Stop machines in OpenStack

        This function stops the machine in OpenStack. It will now regularly shut down and therefore
        log out of condor. A direct terminate would result in the machine not logging out of condor
        and therefore stay in condor status idle, although it doesn't exist any more.

        If stopping the machine fails, for example due to connection failures, nothing happens until
        the next management cycle.

        :param mid: id of machine to stop
        :return:
        """
        try:
            # initialize the NovaAPI
            nova = self.__getNovaApi()
            # send the stop command for shutting down
            nova.servers.find(id=self.mr.machines[mid][self.reg_site_server_id]).stop()
        except Exception:
            pass

    def __openstackTimeDepStopMachine(self):
        """
        function to terminate running machines if time dependant machine management is enabled
        :return:
        """

        daytime = datetime.datetime.strptime(self.getConfig(self.configDay), "%H:%M")
        nighttime = datetime.datetime.strptime(self.getConfig(self.configNight), "%H:%M")
        # between certain times, just use a set percentage of machines
        if daytime.time() <= datetime.datetime.now().time() <= nighttime.time():

            hypervisor_machines = {}

            for mid in self.mr.getMachines(self.siteName):
                # if hypervisor is not set or None, set the hypervisor correctly
                if (self.reg_site_server_hypervisor not in self.mr.machines[mid] or
                            self.mr.machines[mid][self.reg_site_server_hypervisor] is None):
                    self.mr.machines[mid][self.reg_site_server_hypervisor] = self.__getHypervisor(
                        self.mr.machines[mid][self.reg_site_server_id])
                # get the hypervisor from machine registry
                hypervisor = self.mr.machines[mid][self.reg_site_server_hypervisor]
                # append machine to hypervisor in hypvisor list
                if hypervisor in hypervisor_machines:
                    hypervisor_machines[
                        self.mr.machines[mid][self.reg_site_server_hypervisor]].append(
                        mid)
                else:
                    hypervisor_machines[self.mr.machines[mid][self.reg_site_server_hypervisor]] = [
                        mid]

            for hypervisor in hypervisor_machines:
                # check if there are more machines running on specific hypervisor and if so, get the least one used and
                # terminate it
                if len(hypervisor_machines[hypervisor]) > self.getConfig(
                        self.configMachinePercentage) * (
                            self.getConfig(self.configMaxMachines) / len(hypervisor_machines)):
                    # self.logger.info("Need to terminate machines due to daytime use...")
                    to_terminate = None
                    for mid in hypervisor_machines[hypervisor]:
                        if to_terminate is None:
                            to_terminate = mid
                        # prefer machines booting and not ones working
                        if self.mr.machines[mid][self.mr.regStatus] in [self.mr.statusUp,
                                                                        self.mr.statusBooting,
                                                                        self.mr.statusIntegrating]:
                            to_terminate = mid
                            break
                        # if all machines are working, get the least one used
                        if (self.mr.machines[mid][self.reg_site_server_status] !=
                                self.reg_site_server_status_error):
                            if (self.mr.regMachineLoad in self.mr.machines[mid] and
                                        self.mr.regMachineLoad in self.mr.machines[to_terminate] and
                                        self.mr.machines[mid][self.mr.regMachineLoad] <
                                        self.mr.machines[to_terminate][self.mr.regMachineLoad]):
                                to_terminate = mid
                        # if all are used the same, just take the first one
                        else:
                            to_terminate = mid
                            break
                    # set machine to disintegrating, which means they will be shut down immediately
                    self.mr.updateMachineStatus(to_terminate, self.mr.statusDisintegrating)

    def manage(self, cleanup=False):
        """Managing machine states, run once per cycle

        This function takes care of the machine status and manages state changes:
        booting -> up
        disintegrating -> disintegrated

        It uses machine states in OpenStack and the machine registry machine states to trigger state changes.

        :return:
        """
        nova_machines = self.__getNovaMachines()

        # Look for each machine in machine registry and perform necessary status change(s).
        #
        # In the process we delete each machine that's in the machine registry from nova_machines.
        # As a result, nova_machines then contains a list of MISSING machines.
        #
        # -> Add these machines to the machines registry.
        # This can happen, if (somehow) machines boot up at OpenStack without being requested...

        for mid in self.mr.getMachines(self.siteName):
            # machine not listed in OpenStack -> remove from machine registry
            if len(nova_machines) == 0 or mid not in nova_machines:
                self.mr.removeMachine(mid)
                continue

            # check if condor name is set
            if not self.reg_site_server_condor_name in self.mr.machines[mid]:
                self.mr.machines[self.reg_site_server_condor_name] = mid

            # if machine is in error state, move it to disintegrating
            if nova_machines[mid][self.reg_site_server_status] in [
                self.reg_site_server_status_error, self.reg_site_server_status_shutoff]:
                self.mr.machines[mid][self.reg_site_server_status] = \
                    self.reg_site_server_status_error
                self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

            # status handled by Integration Adapter
            if self.mr.machines[mid][self.mr.regStatus] in [self.mr.statusIntegrating,
                                                            self.mr.statusWorking,
                                                            self.mr.statusPendingDisintegration]:
                del nova_machines[mid]
            # if status = down, machine was terminated at OpenStack -> remove from machine registry
            elif self.mr.machines[mid][self.mr.regStatus] == self.mr.statusDown:
                self.mr.removeMachine(mid)
                continue
            # check if machine could be started correctly
            elif self.mr.machines[mid][self.mr.regStatus] == self.mr.statusBooting:
                # they started correctly when OpenStack state changes to active
                if nova_machines[mid][
                    self.reg_site_server_status] == self.reg_site_server_status_active:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    self.mr.machines[mid][self.reg_site_server_status] = nova_machines[mid][
                        self.reg_site_server_status]
                if mid in nova_machines:
                    del nova_machines[mid]
            # check if machines is disintegrating
            elif self.mr.machines[mid][self.mr.regStatus] == self.mr.statusDisintegrating:
                # check if machine is in status active (OpenStack status), if so, send stop command
                if nova_machines[mid][
                    self.reg_site_server_status] == self.reg_site_server_status_active:
                    self.__openstackStopMachine(mid)
                # if machine is in status shutoff (OpenStack), update to disintegrated
                if nova_machines[mid][
                    self.reg_site_server_status] == self.reg_site_server_status_shutoff:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)
                if mid in nova_machines:
                    del nova_machines[mid]

        # add running nova machines and information to machine registry
        for mid in nova_machines:
            if mid not in self.mr.getMachines(self.siteName):
                new = self.mr.newMachine(mid)
                self.mr.machines[new][self.mr.regSite] = self.siteName
                self.mr.machines[new][self.mr.regSiteType] = self.siteType
                # TODO: handle different machine types
                self.mr.machines[new][self.mr.regMachineType] = self._machineType  # self.getConfig(
                # self.configMachines)  # "vm-default"
                self.mr.machines[new][self.reg_site_server_id] = nova_machines[mid][
                    self.reg_site_server_id]
                self.mr.machines[new][self.reg_site_server_status] = nova_machines[mid][
                    self.reg_site_server_status]
                self.mr.machines[new][self.reg_site_server_name] = mid
                self.mr.machines[new][self.reg_site_server_condor_name] = mid
                # self.mr.machines[new][self.mr.regMachineCores] = self.getConfig(self.configMachineType)["vm-default"][
                #    "cores"]

                if nova_machines[mid][
                    self.reg_site_server_status] == self.reg_site_server_status_error:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                else:
                    self.mr.updateMachineStatus(mid, self.mr.statusWorking)

        if self.getConfig(self.configUseTime):
            self.__openstackTimeDepStopMachine()

        ###
        # Write Json log file:
        #  requested machines, nodes, draining nodes.
        ###
        self.logger.info("Current machines running at %s: %d" %
                         (self.siteName, self.runningMachinesCount[self.getConfig(self.configMachines).keys()[0]]))
        json_log = JsonLog()
        json_log.addItem(self.siteName, "machines_requested",
                         int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                             len(self.getSiteMachines(status=self.mr.statusUp)) +
                             len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem(self.siteName, "condor_nodes", len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem(self.siteName, "condor_nodes_draining",
                         len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))

    def onEvent(self, mid):
        """Event handler

        Handle machine status changes. Called every time a machine status changes and checks if the new status is
        disintegrating or disintegrated.

        :param mid: id of machine with changed status
        :return:
        """

        # check correct site etc...
        if isinstance(mid, MachineRegistry.StatusChangedEvent):
            if self.mr.machines[mid.id].get(self.mr.regSite) == self.siteName:
                if self.mr.machines[mid.id].get(self.mr.regSite) == self.siteName:
                    # if new status is disintegrating, halt machine
                    if mid.newStatus == self.mr.statusDisintegrating:
                        self.__openstackStopMachine(mid.id)
                    # if new status is disintegrated, machine is already shut down, so kill it
                    if mid.newStatus == self.mr.statusDisintegrated:
                        self.__openstackTerminateMachines(mid)
                        self.mr.updateMachineStatus(mid.id, self.mr.statusDown)

    def __getHypervisor(self, id_):
        """
        get the corresponding hypervisor to the requested id

        :return:vm_hypervisor
        """

        nova = self.__getNovaApi(self.getConfig(self.configUseTime))
        nova_machines = [(vm.id, vm.__dict__["OS-EXT-SRV-ATTR:host"]) for vm in
                         nova.servers.list(search_opts={"all_tenants": True})]

        # return the corresponding hypervisor
        for (vm_id, vm_hypervisor) in nova_machines:
            if str(vm_id) == str(id_):
                # if vm_hypervisor == "None":

                return vm_hypervisor

    def __getNovaApi(self, admin_access=False):
        """Nova Client API

        initialize the Nova Client API with the specified settings

        :rtype: Client
        :return: NovaClient
        """

        # login in data to OpenStack
        if admin_access:
            user = self.getConfig(self.configAdmin)
            password = self.getConfig(self.configAdminPass)
            tenant = self.getConfig(self.configAdminTenant)
        else:
            user = self.getConfig(self.configUser)
            password = self.getConfig(self.configPass)
            tenant = self.getConfig(self.configTenant)
        keystone = self.getConfig(self.configKeystoneServer)
        time_out = self.getConfig(self.configTimeout)

        # client = __import__("novaclient", globals(), locals(), [], 0)
        return Client(2, user, password, tenant, keystone, timeout=time_out)

    def __getNovaMachines(self):
        """Get list of machines from OpenStack

        :return: nova_machines
        """

        # init NovaClient
        nova = self.__getNovaApi()

        # get list of servers
        try:
            nova_results = [(x.id, x.name, x.status) for x in nova.servers.list()]
            # except Exception:
            #    pass

            nova_machines = {}

            if len(nova_results) >= 1:
                for (id_, name, status) in nova_results:
                    # set some machine information
                    nova_machines[name] = {}
                    nova_machines[name][self.reg_site_server_id] = id_
                    nova_machines[name][self.reg_site_server_status] = status

            return nova_machines

        except Exception as e:
            logging.warning(e)
            return {}
