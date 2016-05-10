# ================================================================================
#
# Copyright (c) 2010, 2011, 2016 by Thomas Hauth, Stephan Riedel and Guenther Erli
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
# ================================================================================


import logging
import re
import time
import sys
import boto3

from Core import Config, MachineRegistry
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog

PY3 = sys.version_info > (3,)


class Ec2SiteAdapter(SiteAdapterBase):
    """
    Site Adapter for Amazon EC2 Cloud

    responsible for booting up and shutting down machines
    """

    # name for this adapter to be shown in ROCED output
    configSiteLogger = 'logger_name'

    # machine specific settings
    configMachines = 'machines'
    configImageID = 'image_id'
    configUserData = 'user_data'
    configSecurityGroupIDs = 'security_group_ids'
    configInstanceType = 'instance_type'

    # site settings
    configMinCount = 'min_count'
    configMaxCount = 'max_count'

    # keywords for EC2
    ec2 = 'ec2'
    ec2_instance_statuses = 'InstanceStatuses'
    ec2_instance_id = 'InstanceId'
    ec2_instance_state = 'InstanceState'
    ec2_instance_status = 'InstanceStatus'
    ec2_availability_zone = 'AvailabilityZone'
    ec2_system_status = 'SystemStatus'

    # keywords for machine registry
    reg_site_server_id = "reg_site_server_id"
    reg_site_server_status = "reg_site_server_status"
    reg_site_server_name = "reg_site_server_name"
    reg_site_server_condor_name = "reg_site_server_condor_name"

    stop = 'stop'
    terminate = 'terminate'

    def __init__(self):
        """Init function

        load config keys from config files

        :return:
        """
        super(Ec2SiteAdapter, self).__init__()

        # load SiteAdapter name
        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description='Logger name of SiteAdapter',
                                   default='EC2_Site')

        # load machine specific settings
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary,
                                     description='Machine type')
        self.addCompulsoryConfigKeys(self.configImageID, Config.ConfigTypeString,
                                     description='ID of Image to boot')
        self.addOptionalConfigKeys(self.configUserData, Config.ConfigTypeString,
                                   description='Path to Cloud-Init Script',
                                   default=None)
        self.addOptionalConfigKeys(self.configSecurityGroupIDs, Config.ConfigTypeString,
                                   description='ID of Security group to apply',
                                   default=None)
        self.addOptionalConfigKeys(self.configInstanceType, Config.ConfigTypeString,
                                   description='Instance type',
                                   default='t2.large')
        self.addOptionalConfigKeys(self.configMinCount, Config.ConfigTypeInt,
                                   description='Number of machines to boot at least',
                                   default=None)
        self.addOptionalConfigKeys(self.configMaxCount, Config.ConfigTypeInt,
                                   description='Number of machines to boot max')

        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        super(Ec2SiteAdapter, self).init()

        # disable urllib3 logging
        if PY3:
            urllib3_logger = logging.getLogger('botocore.vendored.requests.packages.urllib3.connectionpool')
            botocore_logger = logging.getLogger('botocore')
        else:
            urllib3_logger = logging.getLogger('requests.packages.urllib3.connectionpool')

        boto3_logger = logging.getLogger('boto3')
        botocore_logger = logging.getLogger('botocore')

        boto3_logger.setLevel(logging.CRITICAL)
        botocore_logger.setLevel(logging.CRITICAL)
        urllib3_logger.setLevel(logging.CRITICAL)

        self.mr.registerListener(self)

    def getRunningMachines(self):
        """
        Returns a dictionary containing all running machines

        The number of running machines needs to be recalculated when using status integrating and pending
        disintegration. Machines pending disintegration are still running an can accept new jobs. Machines integrating
        are counted as running machines by default.

        :return: machines_
        """

        # get all machines running on site
        tmp_machines_ = self.getSiteMachines()
        machines_ = dict()

        # generate empty list for machines running on 1and1
        machines_[list(self.getConfig(self.configMachines).keys())[0]] = []

        # filter for machines in status booting, up, integrating, working or pending disintegration
        for (k, v) in list(tmp_machines_.items()):
            if v.get(self.mr.regStatus) in [self.mr.statusBooting, self.mr.statusUp,
                                            self.mr.statusIntegrating, self.mr.statusWorking,
                                            self.mr.statusPendingDisintegration]:
                # add machine to previously defined list
                machines_[v[self.mr.regMachineType]].append(k)

        return machines_

    def getEC2Machines(self):
        """
        return all machines running on EC2
        :return: ec2_machines_
        """
        ec2 = boto3.resource(self.ec2)

        tmp_ec2_machines_ = ec2.instances.all()
        ec2_machines_list_ = list()
        for instance_ in tmp_ec2_machines_:
            ec2_machines_list_.append(instance_.id)

        tmp_ec2_machines_ = ec2.meta.client.describe_instance_status()[self.ec2_instance_statuses]
        ec2_machines_status_ = dict()

        for machine_ in tmp_ec2_machines_:
            instance_id_ = machine_[self.ec2_instance_id]
            instance_state_ = machine_[self.ec2_instance_state]
            instance_status_ = machine_[self.ec2_instance_status]
            availability_zone_ = machine_[self.ec2_availability_zone]
            system_status_ = machine_[self.ec2_system_status]
            ec2_machines_status_[instance_id_] = {self.ec2_instance_state: instance_state_,
                                                  self.ec2_instance_status: instance_status_,
                                                  self.ec2_availability_zone: availability_zone_,
                                                  self.ec2_system_status: system_status_}

        return (ec2_machines_status_, ec2_machines_list_)

    def spawnMachines(self, machineType, requested):
        """
        spawn VMs for 1and1 cloud service
        :param machineType:
        :param requested:
        :return:
        """

        # check if machine type is requested machine type
        if not machineType == list(self.getConfig(self.configMachines).keys())[0]:
            return 0

        # get EC2 client
        ec2 = boto3.resource(self.ec2)
        userdata = open(self.getConfig(self.configUserData), "r").read()

        if requested > self.getConfig(self.configMaxCount):
            max_count_ = self.getConfig(self.configMaxCount)
        else:
            max_count_ = requested

        if self.getConfig(self.configMinCount) is None:
            min_count_ = requested
        else:
            min_count_ = self.getConfig(self.configMinCount)

        new_machines_ = ec2.create_instances(ImageId=self.getConfig(self.configImageID),
                                             MinCount=min_count_,
                                             MaxCount=max_count_,
                                             UserData=userdata,
                                             SecurityGroupIds=self.getConfig(self.configSecurityGroupIDs).split(),
                                             InstanceType=self.getConfig(self.configInstanceType))

        for machine_ in new_machines_:
            # create new machine in machine registry
            mid = self.mr.newMachine()

            # set some machine specific entries in machine registry
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            # self.mr.machines[mid][self.reg_site_server_name] = machine_.id
            self.mr.machines[mid][self.reg_site_server_id] = machine_.id
            # self.mr.machines[mid][self.reg_site_server_status] = vm[self.oao_status][self.oao_state]
            self.mr.machines[mid][self.reg_site_server_condor_name] = machine_.id

            # update machine status
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        return

    def cleanupEC2(self):
        ec2 = boto3.resource(self.ec2)

        available_volumes_ = ec2.volumes.filter(Filters=[{'Name': 'status', 'Values': ['available']}])
        for volume_ in available_volumes_:
            volume_.delete()

        #available_snapshots_ = ec2.snapshots.filter(OwnerIds=["181010420550"])
        #for snapshot_ in available_snapshots_:
        #    snapshot_.delete()

        images = ec2.images.all()
        images = [image.id for image in images]
        for snapshot in ec2.snapshots.filter(OwnerIds=["181010420550"]):
            r = re.match(r".*for (ami-.*) from.*", snapshot.description)
            if r:
                if r.groups()[0] not in images:
                    snapshot.delete()

    def terminateEC2Machine(self, state, *mids):
        """
        terminate machine on EC2 cloud site
        :param mid: machine id in machine registry
        """

        if len(mids[0]) > 0:
            instance_ids_ = []

            for mid in mids[0]:
                instance_ids_.append(self.mr.machines[mid][self.reg_site_server_id])

            ec2 = boto3.resource(self.ec2)
            if state == self.stop:
                ec2.instances.filter(InstanceIds=instance_ids_).stop()
            if state == self.terminate:
                ec2.instances.filter(InstanceIds=instance_ids_).terminate()
                self.cleanupEC2()

        return

    def manage(self):
        """
        managing machine states that change dependant of the state changes on 1and1 cloud site run once per cycle

        :return:
        """

        # get machines from EC2
        ec2_machines_status_, ec2_machines_list_ = self.getEC2Machines()

        machines_to_stop_ = list()
        machines_to_terminate_ = list()

        # if something fails while receiving response from EC2 a type 'None' will be returned
        if ec2_machines_status_ is None:  # or (len(oao_machines) == 0):
            return

        for mid in self.mr.getMachines(self.siteName):
            machine_ = self.mr.machines[mid]

            # check if machine is already deleted on site and remove it from machine registry
            #if not machine_[self.reg_site_server_id] in ec2_machines_status_:
            #    self.mr.removeMachine(mid)
            #    continue

            # check for status which is handled by integration adapter
            if machine_[self.mr.regStatus] in [self.mr.statusUp,
                                               self.mr.statusIntegrating,
                                               self.mr.statusWorking,
                                               self.mr.statusPendingDisintegration]:
                del ec2_machines_status_[machine_[self.reg_site_server_id]]

            # down
            # if machine status in machine registry is down and machine is still listed on EC2 cloud, terminate machine
            if machine_[self.mr.regStatus] == self.mr.statusDown:
                if not machine_[self.reg_site_server_id] in ec2_machines_list_:
                    self.mr.removeMachine(mid)
                # del ec2_machines_status_[machine_[self.reg_site_server_id]]

            if machine_[self.mr.regStatus] == self.mr.statusDisintegrated:
                if not machine_[self.reg_site_server_id] in ec2_machines_status_:
                    machines_to_terminate_.append(mid)
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                else:
                    del ec2_machines_status_[machine_[self.reg_site_server_id]]

            if machine_[self.mr.regStatus] == self.mr.statusDisintegrating:
                # self.terminateEC2Machine(self.stop, mid)
                if machine_[self.reg_site_server_id] in ec2_machines_status_:
                    machines_to_stop_.append(mid)
                    del ec2_machines_status_[machine_[self.reg_site_server_id]]

            # TODO: use this status transition from up to integrating instead of the one used in integration adapter.onEvent
            #if machine_[self.mr.regStatus] == self.mr.statusUp:
            #    if ec2_machines_status_[machine_[self.reg_site_server_id]][self.ec2_instance_status][
            #        'Status'] == 'initializing':
            #        self.mr.updateMachineStatus(mid, self.mr.statusIntegrating)
            #    del ec2_machines_status_[machine_[self.reg_site_server_id]]

            # booting -> up
            # check if machine status booting
            if machine_[self.mr.regStatus] == self.mr.statusBooting:
                if machine_[self.reg_site_server_id] in ec2_machines_status_:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                else:
                    continue
                if ec2_machines_status_[machine_[self.reg_site_server_id]][self.ec2_instance_status][
                    'Status'] == 'initializing':
                    pass
                elif ec2_machines_status_[machine_[self.reg_site_server_id]][self.ec2_instance_status][
                    'Status'] == 'ok':
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                del ec2_machines_status_[machine_[self.reg_site_server_id]]

        self.terminateEC2Machine(self.stop, machines_to_stop_)
        self.terminateEC2Machine(self.terminate, machines_to_terminate_)

        # add all machines remaining in machine list from 1&1
        for machine_ in ec2_machines_status_:
            # create new machine in machine registry
            mid = self.mr.newMachine()
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = self.ec2  # machineType
            # self.mr.machines[mid][self.reg_site_server_name] = oao_machines[vm][self.oao_name]
            self.mr.machines[mid][self.reg_site_server_id] = machine_
            # self.mr.machines[mid][self.reg_site_server_status] = ec2_machines_status_[machine_][self.ec2_instance_status]
            self.mr.machines[mid][self.reg_site_server_condor_name] = machine_

            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

        # add current amounts of machines to Json log file
        self.logger.info("Current machines running at %s: %d"
                         % (self.siteName, self.runningMachinesCount[
            list(self.getConfig(self.configMachines).keys())[0]]))  # ["vm-default"]))
        json_log = JsonLog()
        json_log.addItem(self.siteName, 'machines_requested',
                         int(len(self.getSiteMachines(status=self.mr.statusBooting)) +
                             len(self.getSiteMachines(status=self.mr.statusUp)) +
                             len(self.getSiteMachines(status=self.mr.statusIntegrating))))
        json_log.addItem(self.siteName, 'condor_nodes',
                         len(self.getSiteMachines(status=self.mr.statusWorking)))
        json_log.addItem(self.siteName, 'condor_nodes_draining',
                         len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))


    def onEvent(self, mid):
        """
        event handler, called when a machine state changes
        :param mid:
        :return:
        """
        pass