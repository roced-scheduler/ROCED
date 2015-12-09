# ==============================================================================
#
# Copyright (c) 2010, 2011, 2015 by Guenther Erli
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
from collections import defaultdict

import datetime, time
from Core import MachineRegistry, Config
from SiteAdapter.Site import SiteAdapterBase
from Util.ScaleTools import JsonLog, JsonStats
# Open Stack API
from novaclient.client import Client


class OpenStackSiteAdapter(SiteAdapterBase):
    """
    Site Adapter for OpenStack

    responsible for spawning new machines and setting the status from booting to up, from up to integrating,
    from pending disintegrating to disintrating, from disintegrating to disintegrated and from disintegrated to down
    """

    # Name of Site Adapter in ROCED output
    configSiteLogger = "site_logger"

    # OpenStack connection related information
    configKeystoneServer = "openstack_Keystone_Server"
    configUser = "openstack_User"
    configPass = "openstack_Password"
    configTenant = "openstack_Tenant"
    configTimeout = "openstack_Timeout"

    # machine specific settings
    configMachines = "machines"
    configMachineType = "machine_type"
    configMaxMachinesPerCycle = "max_machines_per_cycle"
    configMaxMachines = "max_machines"
    configIgnoreTime = "ignore_time"
    configMachinePercentage = "max_usage_daytime"
    configDay = "daytime"
    configNight = "nighttime"

    # timestamps and status changes for statistic files
    reg_status_changed_to_booting = "status_changed_to_booting"
    reg_status_changed_to_up = "status_changed_to_up"
    reg_status_changed_to_integrating = "status_changed_to_integrating"
    reg_status_changed_to_working = "status_changed_to_working"
    reg_status_changed_to_pending_disintegration = "status_changed_to_pending_disintegration"
    reg_status_changed_to_disintegrating = "status_changed_to_disintegrating"
    reg_status_changed_to_disintegrated = "status_changed_to_disintegrated"
    reg_status_changed_to_down = "status_changed_to_down"

    # name, id and status of VMs at OpenStack
    reg_site_server_name = "open_stack_server_name"
    reg_site_server_id = "open_stack_server_id"
    reg_site_server_status = "open_stack_server_status"

    # OpenStack state declarations, same as in OpenStack dashboard
    reg_site_server_status_active = "ACTIVE"
    reg_site_server_status_error = "ERROR"
    reg_site_server_status_shutoff = "SHUTOFF"

    def __init__(self):
        """Init function

        Load config keys from config files

        :return:
        """
        SiteAdapterBase.__init__(self)

        # load Site Adapter name for ROCED output from config file
        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of Site Adapter", default="Site")

        # TODO check if this is really needed...
        # init ConfigMachines with empty dictionary
        self.setConfig(self.configMachines, dict())

        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary, "Machine dictionary")

        # load OpenStack login data from config file
        self.addCompulsoryConfigKeys(self.configKeystoneServer, Config.ConfigTypeString, "OpenStack server address")
        self.addCompulsoryConfigKeys(self.configUser, Config.ConfigTypeString, "OpenStack user name")
        self.addCompulsoryConfigKeys(self.configPass, Config.ConfigTypeString, "OpenStack password")
        self.addCompulsoryConfigKeys(self.configTenant, Config.ConfigTypeString, "OpenStack tenant information")
        self.addOptionalConfigKeys(self.configTimeout, Config.ConfigTypeInt,
                                   description="OpenStack connection timeout", default=300)

        # TODO check if this is really needed...
        # init ConfigMachineType with empty dictionary
        self.setConfig(self.configMachineType, dict())
        self.addCompulsoryConfigKeys(self.configMachineType, Config.ConfigTypeDictionary)

        # load machine specific settings from config file
        self.addCompulsoryConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                     "Number of machines booted per cycle")
        self.addCompulsoryConfigKeys(self.configMaxMachines, Config.ConfigTypeInt, "Number of machines allowed on site")
        self.addCompulsoryConfigKeys(self.configDay, Config.ConfigTypeString, "Defines when day begins")
        self.addCompulsoryConfigKeys(self.configNight, Config.ConfigTypeString, "Defines when night begins")
        self.addCompulsoryConfigKeys(self.configIgnoreTime, Config.ConfigTypeBoolean,
                                     "Consider different amounts of machines at day/night")
        self.addCompulsoryConfigKeys(self.configMachinePercentage, Config.ConfigTypeFloat,
                                     "Percentage of machines loaded at day")

        # init Machine Registry
        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        # set name of Site Adapter for ROCED output
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))

        self.mr.registerListener(self)

    def getSiteMachines(self, status=None, machineType=None):
        """
        Get machines running at Freiburg site

        :param status:
        :param machineType:
        :return: machine_registry
        """
        return self.mr.getMachines(self.getSiteName(), status, machineType)

    def getRunningMachines(self):
        """Returns a dictionary containing all running machines

        The number of running machines needs to be recalculated when using status integrating and pending
        disintegration. Machines pending disintegration are still running an can accept new jobs. Machines integrating
        are counted as running machines by default.

        :return: machineList
        """

        myMachines = self.getSiteMachines()
        machineList = dict()

        for i in self.getConfig(self.configMachines):
            machineList[i] = []

        # filter for machines in status booting, up, integrating, working or
        # pending disintegration
        for (k, v) in myMachines.iteritems():
            if (v.get(self.mr.regStatus) == self.mr.statusBooting) or \
                    (v.get(self.mr.regStatus) == self.mr.statusUp) or \
                    (v.get(self.mr.regStatus) == self.mr.statusIntegrating) or \
                    (v.get(self.mr.regStatus) == self.mr.statusWorking) or \
                    (v.get(self.mr.regStatus) == self.mr.statusPendingDisintegration):
                # will later hold specific information, like id, ip etc
                machineList[v[self.mr.regMachineType]].append(k)

        return machineList

    def getRunningMachinesCount(self):
        """
        This function returns a dictionary containing the number running machines for each machine type

        :return: running_machines_count
        """
        # this function return the a dictionary containing the number of running
        # machines for each machine type
        running_machines = self.getRunningMachines()
        running_machines_count = dict()

        for machine_type in running_machines:
            running_machines_count[machine_type] = len(running_machines[machine_type])

        return running_machines_count

    def spawnMachines(self, machineType, requested):
        """Function to spawn requested amount of machines

        This function spawns the requested number of new machines, unless the number exceeds the maximal number of
        machines allowed to spawn per management cycle. The requested amount of machines is also limited by
        SiteAdapterBase.applyMachineDecision(), so that the number of requested machines plus running machines will not
        exceed the number of overall allowed machines.

        If the timedependent spawning of machines is activated, it will not spawn more machines totally than
        (percentage of machines at day) * (number of machines allowed per site).

        If spawning is not possible of fails (due to connection failures, OpenStack quota limits,...) it does nothing
        (ROCED will try to spawn new machines in next managment cycle).

        :param machineType:
        :param requested: requested number of machines
        :return: count
        """

        try:
            nova = self.getNovaApi()

            # important to give a specifc network due to bug in nova api:
            netw = nova.networks.list()[0]
            fls = nova.flavors.find(name='m1.large')
            img = nova.images.find(name='gridka_test')

            name_prefix = 'id-'

            daytime = datetime.datetime.strptime(self.getConfig(self.configDay), "%H:%M")
            nightime = datetime.datetime.strptime(self.getConfig(self.configNight), "%H:%M")

            # check if timedependent spawning is activated
            if not self.getConfig(self.configIgnoreTime):
                # check if it is day or night
                if daytime.time() <= datetime.datetime.now().time() <= nightime.time():
                    # if the amount of requested machines plus running machines exceed the number of maximally allowed
                    # machines at day, set the number of requested machines so that it fits the limits
                    # also: if it is daytime, the amount of allowed machines is set to (percentage * max_machines)
                    # if it is nighttime, nothing happens
                    if (requested + len(self.getSiteMachines())) > (
                                self.getConfig(self.configMaxMachines) * self.getConfig(self.configMachinePercentage)):
                        self.logger.info("Request exceeds maximum number of allowed machines for daytime (" +
                                         str(requested + len(self.getSiteMachines())) + ">" + str(
                            int(self.getConfig(self.configMaxMachines) * self.getConfig(
                                self.configMachinePercentage))) +
                                         ")! Will spawn " + str(int(
                            (self.getConfig(self.configMaxMachines) * self.getConfig(
                                self.configMachinePercentage)) - len(
                                self.getSiteMachines()))) +
                                         " machines")
                        requested = int(
                            (self.getConfig(self.configMaxMachines) * self.getConfig(
                                self.configMachinePercentage)) - len(
                                self.getSiteMachines()))

            # check if the requested amount of machines exceeds the allowed number of machines per cycle
            if requested > self.getConfig(self.configMaxMachinesPerCycle):
                self.logger.info("Request exceeds maximum number of allowed machines per cycle on this site (" +
                                 str(requested) + ">" + str(
                    self.getConfig(self.configMaxMachinesPerCycle)) + ")! Will spawn " +
                                 str(self.getConfig(self.configMaxMachinesPerCycle)) + " machines")
                # set requested equals the number of machines per cycle
                requested = self.getConfig(self.configMaxMachinesPerCycle)

            # now spawn the machines
            for count in xrange(requested):
                # init new machine in machine registry
                mid = self.mr.newMachine()
                # spawn machine at site
                sv = nova.servers.create(name_prefix + str(mid), img, fls, nics=[{"net-id": netw.id}])

                # set some machine information in machine registry
                self.mr.machines[mid][self.mr.regSite] = self.getSiteName()
                self.mr.machines[mid][self.mr.regSiteType] = self.getSiteType()
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.reg_site_server_id] = sv.id

                # TODO: set these information in updateMachineStatus()
                self.mr.machines[mid][self.reg_status_changed_to_booting] = datetime.datetime.now()

                # TODO: set machine information, like openstack id
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)

            # all machines booted
            return count

        # if spawning fails, do nothing
        except:
            pass

    def openstackTerminateMachines(self, mid):
        """Terminate machines in OpenStack

        This function will terminate/delete the machine in OpenStack, which means it will clean up the used resources in
        OpenStack, and remove the machine from the machine registry.

        If terminating the machine fails, for example due to connection failures, nothing happens until the next
        management cycle

        :param mid: id of machine to terminate
        :return:
        """
        try:
            # initialize the NovaAPI
            nova = self.getNovaApi()
            # send terminate/delete command
            nova.servers.find(id=self.mr.machines[mid.id][self.reg_site_server_id]).delete()
            # remove from machine registry
            self.mr.removeMachine()
        except:
            pass

    def openstackStopMachine(self, mid):
        """Stop machines in OpenStack

        This function stops the machine in OpenStack. It will now regularly shut down and therefore log out of condor.
        A direct terminate would result in the machine not logging out of condor and therefore stay in condor status
        idle, although it doesn't exist any more.

        If stopping the machine fails, for example due to connection failures, nothing happens until the next management
        cycle.

        :param mid: id of machine to stop
        :return:
        """
        try:
            # initialize the NovaAPI
            nova = self.getNovaApi()
            # send the stop command for shutting down
            nova.servers.find(id=self.mr.machines[mid][self.reg_site_server_id]).stop()
        except:
            pass

    def manage(self):
        """Managing machine states, run once per cycle

        This function takes care of the machine status and manages state changes:
        booting -> up
        disintegrating -> disintegrated

        It uses machine states in OpenStack and the machine registry machine states to trigger state changes.

        :return:
        """
        nova_machines = self.getNovaMachines()

        mr_machines = self.getSiteMachines()

        # look after each machine in machine registry and perform state changes. if machine appears also in nova
        # machines, delete it from nova machines. this will result in all machines appearing in machine registry will
        # be removed from nova machines and the remaining machines will be add to the machines registry
        # this could happen, if somehow machines will boot up at OpenStack without being requested...
        for mid in mr_machines:
            # if machine is not listed in OpenStack, remove it from machine registry
            """#if mid not in nova_machines:
            #    self.mr.removeMachine(mid)
            #    continue"""

            # status handled by Integration Adapter
            if mr_machines[mid][self.mr.regStatus] in [self.mr.statusIntegrating, self.mr.statusWorking,
                                                       self.mr.statusPendingDisintegration]:
                del nova_machines[mid]

            # if status is down, machine is terminated at OpenStack, so remove it from machine registry
            if mr_machines[mid][self.mr.regStatus] == self.mr.statusDown:
                # write the statics to file
                json_stats = JsonStats()
                #json_stats.add_item("mid", mid)
                json_stats.add_item(mid, self.reg_status_changed_to_booting,
                                    int(time.mktime(mr_machines[mid][self.reg_status_changed_to_booting].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_up,
                                    int(time.mktime(mr_machines[mid][self.reg_status_changed_to_up].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_integrating,
                                    int(time.mktime(mr_machines[mid][
                                                        self.reg_status_changed_to_integrating].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_working,
                                    int(time.mktime(mr_machines[mid][self.reg_status_changed_to_working].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_pending_disintegration,
                                    int(time.mktime(mr_machines[mid][self.reg_status_changed_to_pending_disintegration].
                                        timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_disintegrating,
                                    int(time.mktime(mr_machines[mid][
                                                        self.reg_status_changed_to_disintegrating].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_disintegrated,
                                    int(time.mktime(mr_machines[mid][
                                                        self.reg_status_changed_to_disintegrated].timetuple())))
                json_stats.add_item(mid, self.reg_status_changed_to_down,
                                    int(time.mktime(mr_machines[mid][self.reg_status_changed_to_down].timetuple())))
                json_stats.write_stats()
                # actually delete machine from machine registry
                self.mr.removeMachine(mid)
                continue

            # check if machine could be started correctly
            if mr_machines[mid][self.mr.regStatus] == self.mr.statusBooting:
                # they started correctly when the OpenStack state changes to active
                if nova_machines[mid][self.reg_site_server_status] == self.reg_site_server_status_active:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    self.mr.machines[mid][self.reg_status_changed_to_up] = datetime.datetime.now()
                    self.mr.machines[mid][self.reg_site_server_status] = nova_machines[mid][self.reg_site_server_status]
                if mid in nova_machines:
                    del nova_machines[mid]

            # check if machines is disintegrating
            if mr_machines[mid][self.mr.regStatus] == self.mr.statusDisintegrating:
                # check if machine is in status active (OpenStack status), if so, send stop command
                if nova_machines[mid][self.reg_site_server_status] == self.reg_site_server_status_active:
                    self.openstackStopMachine(mid)
                # if machine is in status shutoff (OpenStack), update to disintegrated
                if nova_machines[mid][self.reg_site_server_status] == self.reg_site_server_status_shutoff:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)
                    self.mr.machines[mid][self.reg_status_changed_to_disintegrated] = datetime.datetime.now()
                if mid in nova_machines:
                    del nova_machines[mid]

        # add running nova machines and information to machine registry if they were not listed there before
        for mid in nova_machines:
            if mid not in mr_machines:
                new = self.mr.newMachine(mid)
                self.mr.machines[new][self.mr.regSite] = self.getSiteName()
                self.mr.machines[new][self.mr.regSiteType] = self.getSiteType()
                # TODO: handle different machine types
                self.mr.machines[new][self.mr.regMachineType] = "vm-default"
                self.mr.machines[new][self.reg_site_server_id] = nova_machines[mid][self.reg_site_server_id]
                self.mr.machines[new][self.reg_site_server_status] = nova_machines[mid][self.reg_site_server_status]
                # self.mr.machines[new][self.mr.regMachineCores] = self.getConfig(self.configMachineType)["vm-default"][
                #    "cores"]

                if nova_machines[mid][self.reg_site_server_status] == self.reg_site_server_status_error:
                    self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                    self.mr.machines[mid][self.reg_status_changed_to_disintegrating] = datetime.datetime.now()
                else:
                    self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                    self.mr.machines[mid][self.reg_status_changed_to_working] = datetime.datetime.now()

        # add current amounts of machines to Json log file
        self.logger.info("Current machines running at GridKa: " + str(self.getRunningMachinesCount()["vm-default"]))
        json_log = JsonLog()
        json_log.addItem('machines_requested', int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                                                   len(self.getSiteMachines(status=self.mr.statusUp)) +
                                                   len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem('condor_nodes', len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem('condor_nodes_draining', len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))

    def onEvent(self, mid):
        """Event handler

        Handle machine status changes. Called every time a machine status changes and checks if the new status is
        disintegrating or disintegrated.

        :param mid: id of machine with changed status
        :return:
        """

        # check correct site etc...
        if isinstance(mid, MachineRegistry.StatusChangedEvent):
            if self.mr.machines[mid.id].get(self.mr.regSite) == self.getSiteName():
                # if new status is disintegrating, halt machine
                if mid.newStatus == self.mr.statusDisintegrating:
                    self.openstackStopMachine(mid.id)
                # if new status is disintegrated, machine is alreade shut down, so kill it
                if mid.newStatus == self.mr.statusDisintegrated:
                    self.openstackTerminateMachines(mid)
                    self.mr.updateMachineStatus(mid.id, self.mr.statusDown)
                    self.mr.machines[mid.id][self.reg_status_changed_to_down] = datetime.datetime.now()

    """private part"""

    def getNovaApi(self):
        """Nova Client API

        initialize the Nova Client API with the specified settings

        :return: NovaClient
        """

        # login in data to OpenStack
        user = self.getConfig(self.configUser)
        password = self.getConfig(self.configPass)
        tenant = self.getConfig(self.configTenant)
        keystone = self.getConfig(self.configKeystoneServer)
        time_out = self.getConfig(self.configTimeout)

        #client = __import__('novaclient', globals(), locals(), [], 0)
        return Client(2, user, password, tenant, keystone, timeout=time_out)

    def getNovaMachines(self):
        """Get list of machines from OpenStack

        :return: nova_machines
        """

        # init NovaClient
        nova = self.getNovaApi()
        # get list of servers
        nova_results = nova.servers.list()
        nova_machines = defaultdict()
        # find all machines running on OpenStack matching the name scheme "id-" + uuid4(), but just the uuid4() is
        # written to tmp_nova_machines
        tmp_nova_machines = re.findall(ur'id-([-a-h0-9]+)', str(nova_results), re.MULTILINE)

        if len(tmp_nova_machines) >= 1:
            for mid in tmp_nova_machines:
                open_stack_server_name = str("id-" + mid)
                # get a virtual machine, not just its name
                try:
                    vm = nova.servers.find(name=open_stack_server_name)
                except:
                    # if it somehow fails, for example network connection failures, do nothing
                    continue
                open_stack_server_id = vm.id
                open_stack_server_status = vm.status

                # for each virtual machine, write some information into dictionary
                nova_machines[mid] = dict()
                nova_machines[mid][self.reg_site_server_name] = open_stack_server_name
                nova_machines[mid][self.reg_site_server_id] = open_stack_server_id
                nova_machines[mid][self.reg_site_server_status] = open_stack_server_status

        return nova_machines

