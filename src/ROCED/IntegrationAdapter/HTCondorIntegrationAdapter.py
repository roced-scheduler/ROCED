# ===============================================================================
#
# Copyright (c) 2015 by Guenther Erli and Georg Fleig
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


import datetime
import logging
import re
from collections import defaultdict

from Core import MachineRegistry, Config
from Util import ScaleTools

from Integration import IntegrationAdapterBase


class HTCondorIntegrationAdapter(IntegrationAdapterBase):
    configIntLogger = "logger_name"
    configCondorName = "site_name"
    configCondorRequirement = "condor_requirement"
    configCondorUser = "condor_user"
    configCondorKey = "condor_key"
    configCondorServer = "condor_server"
    configCondorWaitPD = "condor_wait_pd"
    configCondorWaitWorking = "condor_wait_working"
    configCondorDeadline = "condor_deadline"

    # possible slot status
    condorStatusClaimed = "Claimed"
    condorStatusUnclaimed = "Unclaimed"

    # list of the different slot states for each machine, e.g. [slot1,slot2,...]
    reg_site_condor_status = "condor_slot_status"

    reg_status_last_update = "status_last_update"

    def __init__(self):
        """Init function

        Load config keys from config file

        :return:
        """

        IntegrationAdapterBase.__init__(self)
        self.mr = MachineRegistry.MachineRegistry()
        self.addOptionalConfigKeys(self.configIntLogger, Config.ConfigTypeString, description="logger name",
                                   default="HTC_Int")
        self.addCompulsoryConfigKeys(self.configCondorRequirement, Config.ConfigTypeString,
                                     description="requirement for condor")
        self.addCompulsoryConfigKeys(self.configCondorUser, Config.ConfigTypeString, description="username")
        self.addCompulsoryConfigKeys(self.configCondorKey, Config.ConfigTypeString, description="ssh key")
        self.addCompulsoryConfigKeys(self.configCondorServer, Config.ConfigTypeString, description="server")
        self.addCompulsoryConfigKeys(self.configCondorName, Config.ConfigTypeString,
                                     description="site name")
        self.addOptionalConfigKeys(self.configCondorWaitPD, Config.ConfigTypeInt,
                                   description="wait for x mintues before changing to disintegrating", default=0)
        # self.add...(self.configCondorWaitPD, Config.ConfigTypeInt, discription="",default=1)
        # self.addCompulsoryConfigKeys(self.configCondorIgnoreWaitPD, Config.ConfigTypeBoolean)
        self.addOptionalConfigKeys(self.configCondorWaitWorking, Config.ConfigTypeInt,
                                   description="wait for x minutes befor changing to pending disintegration", default=0)
        # self.addCompulsoryConfigKeys(self.configCondorIgnoreWaitWorking, Config.ConfigTypeBoolean)
        self.addCompulsoryConfigKeys(self.configCondorDeadline, Config.ConfigTypeInt,
                                     description="deadline for dying machines")

    def init(self):
        """

        Register logger and listener

        :return:
        """
        self.logger = logging.getLogger(self.getConfig(self.configIntLogger))
        self.mr.registerListener(self)

    def getSiteName(self):
        """
        Get site name of OpenStack site

        :return: site_name
        """
        return self.getConfig(self.configCondorName)

    def getSiteMachines(self, status=None, machineType=None):
        """
        Get machines running at OpenStack site

        :param status:
        :param machineType:
        :return: machine_registry
        """
        return self.mr.getMachines(self.getSiteName(), status, machineType)

    def manage(self):
        """Managing machine status

        Called every cycle to check on machine states and change them according to their requires

        This managment function will work with status changes:
        booting
        up
        integrating
        working
        pending disintegration

        :return:
        """

        # get a list of condor machines and information about their validity
        condor_machines_tuple = self.getCondorList()
        condor_machines = condor_machines_tuple[0]
        valid_condor_info = condor_machines_tuple[1]

        # get a list of machines from machine registry
        mr_machines = self.getSiteMachines()

        # check if condor info is valid. if for example condor is not reachable, it could be not valid
        # loop over all machines in machine registry and check their status
        if len(mr_machines) != 0 and valid_condor_info:
            for mid in mr_machines:

                # check if machine is stuck on booting, up or integrating
                if mr_machines[mid][self.mr.regStatus] == self.mr.statusBooting or \
                                mr_machines[mid][self.mr.regStatus] == self.mr.statusUp or \
                                mr_machines[mid][self.mr.regStatus] == self.mr.statusIntegrating:
                    # check the time since its last status update and compare it to deadline
                    if datetime.datetime.now() > (mr_machines[mid][self.mr.regStatusLastUpdate]
                                                      + datetime.timedelta(
                                minutes=self.getConfig(self.configCondorDeadline))):
                        # if the machine is stuck in this state for more than (deadline) minutes, run the shutdown cycle
                        self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)

                    # check if machine is stuck while disintegrating
                    # TODO: this is kind of strange... Does this do anything different than the lines above?
                    if datetime.datetime.now() > (mr_machines[mid][self.mr.regStatusLastUpdate]
                                                      + datetime.timedelta(
                                minutes=self.getConfig(self.configCondorDeadline))):
                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

                # check if machines integerating are completely started up
                if mr_machines[mid][self.mr.regStatus] == self.mr.statusIntegrating:
                    # if machines in status integrating appear in condor, the are integrated so change status to working
                    if mid in condor_machines:
                        self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                        # update condor slot status in machine registry
                        self.mr.machines[mid][self.reg_site_condor_status] = condor_machines[mid]

                # check if machines with status pending disintegration can be shut down
                if mr_machines[mid][self.mr.regStatus] == self.mr.statusPendingDisintegration:
                    # check if machine is still listed in condor machines
                    if mid in condor_machines:
                        # update condor slot status in machine registy
                        mr_machines[mid][self.reg_site_condor_status] = condor_machines[mid]
                        cores_claimed = 0.0
                        # loop over all slots and check wether they are claimed (busy) or unclaimed (idle)
                        for core in xrange(len(mr_machines[mid][self.reg_site_condor_status])):
                            if mr_machines[mid][self.reg_site_condor_status][core][0] == self.condorStatusClaimed:
                                cores_claimed += 1
                        # machine load represents the percentage of claimed cores
                        mr_machines[mid][self.mr.regMachineLoad] = cores_claimed / len(
                                mr_machines[mid][self.reg_site_condor_status])
                        self.mr.machines[mid][self.mr.regMachineLoad] = mr_machines[mid][self.mr.regMachineLoad]
                        # if the time passed since the last machine status update is higher than the time it should stay
                        # in thie state (it can sometimes happen, that condor needs quite some time to distribute jobs
                        # to unclaimed slots/cores
                        if (datetime.datetime.now() > (mr_machines[mid][self.reg_status_last_update] + \
                                                               datetime.timedelta(
                                                                       minutes=self.getConfig(
                                                                               self.configCondorWaitPD)))):
                            # if the machine load is higher than zero, it means at least one slot is claimed and
                            # therefore the machine should be update to status working
                            if mr_machines[mid][self.mr.regMachineLoad] > 0.1:
                                self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                            # otherwise the machine is unclaimed and can be shut down/disintegrated
                            else:
                                self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)
                    else:
                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrating)

                # if the machines are in status working it should be checked if the machine load is smaller than 0.1
                # if so the machine is unclaimed and the machine can be set to pending disintegration
                if mr_machines[mid][self.mr.regStatus] == self.mr.statusWorking:
                    if mid in condor_machines:
                        self.mr.machines[mid][self.reg_site_condor_status] = condor_machines[mid]
                        mr_machines[mid][self.reg_site_condor_status] = condor_machines[mid]
                        cores_claimed = 0.0
                        # go over all slots and check if they are claimed or unclaimed
                        for core in xrange(len(mr_machines[mid][self.reg_site_condor_status])):
                            if mr_machines[mid][self.reg_site_condor_status][core][0] == self.condorStatusClaimed:
                                cores_claimed = cores_claimed + 1
                                # set a timestamp on this event
                                self.mr.machines[mid][self.reg_status_last_update] = datetime.datetime.now()
                        # update the machine load in machine registry with the newly calculated machine load
                        mr_machines[mid][self.mr.regMachineLoad] = cores_claimed / len(
                                mr_machines[mid][self.reg_site_condor_status])
                        self.mr.machines[mid][self.mr.regMachineLoad] = mr_machines[mid][self.mr.regMachineLoad]
                        self.mr.machines[mid][self.reg_site_condor_status] = mr_machines[mid][
                            self.reg_site_condor_status]
                        # if the time passed since the last machine status change (timestamp) is longer than the time it
                        # should wait in this status for new jobs and also the machine load is smaller than 0.1:
                        # change the machine state to pending disintegration
                        if (datetime.datetime.now() > (mr_machines[mid][self.reg_status_last_update] + \
                                                               datetime.timedelta(minutes=self.getConfig(
                                                                       self.configCondorWaitWorking)))):
                            if mr_machines[mid][self.mr.regMachineLoad] <= 0.1:
                                self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
                                self.mr.machines[mid][self.reg_status_last_update] = datetime.datetime.now()
                    else:
                        self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)

                # if a machine is in status disintegrating it is shutting down right now
                # if it is not listed in condor it is done shutting down so the status is set to disintegrated
                if mr_machines[mid][self.mr.regStatus] == self.mr.statusDisintegrating:
                    if mid not in condor_machines:
                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

        self.logger.debug("Content of machine registry:\n" + str(self.getSiteMachines()))

    def onEvent(self, evt):
        """Event handler

        Handle machine status changes. Called every time a machine status changes and checks if the new status is up

        :param mid: id of machine with changed status
        :return:
        """

        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            # machines in status up are set to integrating
            if evt.newStatus == self.mr.statusUp:
                if self.mr.machines[evt.id].get(self.mr.regSite) == self.getSiteName():
                    self.mr.updateMachineStatus(evt.id, self.mr.statusIntegrating)

    def getDescription(self):
        return "HTCondorIntegrationAdapter"

    def ssh_debug(self, scope, result):
        """SSH debugging module

        Logger output for ssh connection

        :param scope:
        :param result:
        :return:
        """
        self.logger.debug("[" + scope + "] SSH return code: " + str(result[0]))
        self.logger.debug("[" + scope + "] SSH stdout: " + str(result[1].strip()))
        if result[2]:
            self.logger.debug("[" + scope + "] SSH stderr: " + str(result[2].strip()))

    def getCondorList(self):
        """

        Return a tuple of all condor machines and a valid condor information

        :return: (condor_machines, valid_condor_info)
        """

        # load the connection settings from config
        condor_server = self.getConfig(self.configCondorServer)
        condor_user = self.getConfig(self.configCondorUser)
        condor_key = self.getConfig(self.configCondorKey)
        condor_requirement = self.getConfig(self.configCondorRequirement)
        condor_ssh = ScaleTools.Ssh(condor_server, condor_user, condor_key)

        # get a list of the condor machines via ssh connection
        condor_result = condor_ssh.executeRemoteCommand(
                "condor_status -constraint '" + condor_requirement + "' -autoformat: Machine State Activity")
        self.ssh_debug("EKP-manage", condor_result)

        # condor_result is invalid if there was a connection problem
        valid_condor_info = True
        if not condor_result[0] == 0:
            self.logger.warning("SSH connection to HTCondor collector could not be established.")
            valid_condor_info = False

        # prepare list of condor machines
        tmp_condor_machines = re.findall(ur'([a-z-0-9]+).* ([a-zA-Z]+) ([a-zA-Z]+)', condor_result[1], re.MULTILINE)

        # transform list into dictionary with one list per slot {mid/OpenStackName : [[state, activity], [state, activity], ..]}
        condor_machines = defaultdict(list)
        if len(tmp_condor_machines) > 1 and any(tmp_condor_machines[0]):
            for job_id, state, activity in tmp_condor_machines:
                condor_machines[job_id].append([state, activity])

        # return a tuple containing the needed information
        return (condor_machines, valid_condor_info)
