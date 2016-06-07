# ==============================================================================
#
# Copyright (c) 2015, 2016 by Georg Fleig, Frank Fischer
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

from Core import MachineRegistry, Config
from IntegrationAdapter.HTCondorIntegrationAdapter import HTCondorIntegrationAdapter as HTCondor
from SiteAdapter.Site import SiteAdapterBase
from Util import ScaleTools
from Util.Logging import JsonLog


class FreiburgSiteAdapter(SiteAdapterBase):
    """Site Adapter for Freiburg bwForCluster ENM OpenStack setup."""

    configSiteLogger = "logger_name"
    configFreiburgUser = "freiburg_user"
    configFreiburgUserGroup = "freiburg_user_group"
    configFreiburgKey = "freiburg_key"
    configFreiburgServer = "freiburg_server"
    configMaxMachinesPerCycle = "max_machines_per_cycle"
    configIgnoreDrainingMachines = "ignore_draining_machines"
    configDrainWorkingMachines = "drain_working_machines"

    reg_site_server_condor_name = HTCondor.reg_site_server_condor_name
    regMachineJobId = "batch_job_id"
    __vmStartScript = "startVM.py"
    """Python script to be executed in Freiburg. This starts the VM with the corresponding image.
    This python script has to be adapted on the server with user name, OpenStack Dashboard PW,
    image GUID, etc.
    """
    __condorNamePrefix = "moab-vm-"
    """VM machine name. This is also used as machine name in condor by us."""

    def __init__(self):
        super(FreiburgSiteAdapter, self).__init__()

        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of Site Adapter", default="FRSite")
        self.addCompulsoryConfigKeys(self.configFreiburgUser, Config.ConfigTypeString,
                                     "User name for bwForCluster")
        self.addOptionalConfigKeys(self.configFreiburgUserGroup, Config.ConfigTypeString,
                                   description="User group for bwForCluster. Used when querying "
                                               "for running/completed jobs.", default=None)
        self.addCompulsoryConfigKeys(self.configFreiburgKey, Config.ConfigTypeString,
                                     "SSH Key for bwForCluster")
        self.addCompulsoryConfigKeys(self.configFreiburgServer, Config.ConfigTypeString,
                                     "SSH Server for bwForCluster")
        self.addCompulsoryConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                     "Maximum number of machines to boot in a management cycle")
        self.addOptionalConfigKeys(self.configIgnoreDrainingMachines, Config.ConfigTypeBoolean,
                                   description="Draining (pending-disintegration) machines are "
                                               "counted as working machines (True) or are only "
                                               "partially counted, depending on their slot "
                                               "occupation (False)", default=False),
        self.addOptionalConfigKeys(self.configDrainWorkingMachines, Config.ConfigTypeBoolean,
                                   description="Should ROCED set working machines to drain mode, "
                                               "if it has to terminate machines?", default=False)

        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        self.mr.registerListener(self)
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))
        super(FreiburgSiteAdapter, self).init()

        # TODO: This information is lost, when loading the previous machine registry.
        ###
        # Try to add "booting" machines (submitted batch jobs) to machine registry.
        ###
        try:
            idleJobs = self.__idleJobs
            runningJobs = self.__runningJobs
            completedJobs = self.__completedJobs
        except ValueError:
            if idleJobs is None:
                idleJobs = []
            if runningJobs is None:
                runningJobs = {}
            if completedJobs is None:
                completedJobs = {}

        for mid, machine_ in self.getSiteMachines(status=self.mr.statusBooting).items():
            try:
                idleJobs.remove(machine_[self.regMachineJobId])
            except ValueError:
                if machine_[self.regMachineJobId] in runningJobs:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                elif machine_[self.regMachineJobId] in completedJobs:
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                else:
                    self.logger.debug("Couldn't assign machine %s." % machine_[self.regMachineJobId])
        for jobId in idleJobs:
            mid = self.mr.newMachine()
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = "fr-default"
            self.mr.machines[mid][self.regMachineJobId] = jobId
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getCondorName(
                jobId)
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)
        self.logger.debug("Content of machine registry:\n%s" % self.getSiteMachines())

    def spawnMachines(self, machineType, count):
        """Request machines in Freiburg via batch job containing startVM script.

        Batch job configuration is done via config file.
        All OpenStack parameters (user login, image name, ..) are set in startVM script.

        :param machineType:
        :param count:
        :return:
        """
        super(FreiburgSiteAdapter, self).spawnMachines(machineType, count)

        maxMachinesPerCycle = self.getConfig(self.configMaxMachinesPerCycle)
        machineSettings = self.getConfig(self.ConfigMachines)[machineType]

        if count > maxMachinesPerCycle:
            self.logger.info("%d machines requested, limited to %d for this cycle." % (count, maxMachinesPerCycle))
            count = maxMachinesPerCycle
        for i in range(count):
            # send batch jobs to boot machines
            result = self.__execCmdInFreiburg("msub -l walltime=%s,mem=%s,nodes=1:ppn=%d %s"
                                              % (machineSettings["walltime"],
                                                 machineSettings["memory"],
                                                 machineSettings["cores"], self.__vmStartScript))

            # std_out = batch job id
            if result[0] == 0 and result[1].strip().isdigit():
                mid = self.mr.newMachine()
                self.mr.machines[mid][self.mr.regSite] = self.siteName
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.regMachineJobId] = result[1].strip()
                self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getCondorName(
                    result[1].strip())
                self.mr.machines[mid][self.mr.regSiteType] = self.siteType
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
            else:
                self.logger.warning("A (connection) problem occurred while requesting VMs. "
                                    "Stopping requesting new machines for now. "
                                    "RC %d: stdout: %s, stderr: %s" % (result[0], result[1], result[2]))
                break

    def terminateMachines(self, machineType, count):
        """Terminate machines in Freiburg.

        Working machines are untouched by default, but they may get put into drain mode if
        the configuration is set accordingly.

        :param machineType:
        :param count:
        :return:
        """
        # booting machines, sorted by request time (newest first).
        bootingMachines = self.getSiteMachines(self.mr.statusBooting, machineType)
        try:
            bootingMachines = sorted(bootingMachines.values(),
                                     key=lambda machine_: machine_[self.mr.regStatusLastUpdate],
                                     reverse=True)
        except KeyError:
            bootingMachines = []

        # Running machines, sorted by load (idle first). These machines are put into drain mode
        if self.getConfig(self.configDrainWorkingMachines) is True:
            workingMachines = self.__merge_dicts(
                self.getSiteMachines(self.mr.statusIntegrating, machineType),
                self.getSiteMachines(self.mr.statusWorking, machineType),
                self.getSiteMachines(self.mr.statusPendingDisintegration, machineType))
            try:
                workingMachines = sorted(workingMachines.values(),
                                         key=lambda machine_: HTCondor.calcMachineLoad(machine_[1]),
                                         reverse=True)
            except KeyError:
                workingMachines = []
            # Merge lists
            machinesToRemove = bootingMachines + workingMachines
        else:
            machinesToRemove = bootingMachines

        # needed amount of machines
        machinesToRemove = machinesToRemove[0:count]

        # list of batch job ids to terminate/drain
        idsToTerminate = []
        idsToDrain = []
        idsRemoved = []
        idsInvalidated = []

        for machine in machinesToRemove:
            if machine[self.mr.regStatus] == self.mr.statusBooting:
                # booting machines can be terminated immediately
                idsToTerminate.append(machine[self.regMachineJobId])
            elif self.getConfig(self.configDrainWorkingMachines):
                if HTCondor.calcDrainStatus(machine)[1] is True:
                    continue
                # working machines should be set to drain mode
                idsToDrain.append(machine[self.regMachineJobId])

        self.logger.debug("Machines to terminate (%d): %s" % (len(idsToTerminate), ", ".join(idsToTerminate)))
        if idsToTerminate:
            idsRemoved, idsInvalidated = self.__cancelFreiburgMachines(idsToTerminate)

        self.logger.debug("Machines to drain (%d): %s" % (len(idsToDrain), ", ".join(idsToDrain)))
        if idsToDrain:
            for batchJobID in idsToDrain:
                [HTCondor.drainMachine(machine) for machine in self.getSiteMachines().values()
                 if machine[self.regMachineJobId] == batchJobID]

        if len(idsRemoved + idsInvalidated) > 0:
            # update status
            [self.mr.updateMachineStatus(mid, self.mr.statusDown) for mid, machine
             in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsRemoved + idsInvalidated]

    @property
    def runningMachinesCount(self):
        """Return dictionary with number of machines running at Freiburg. Depending on config file
        this may account for draining slots (claimed|retiring = working vs. claimed|idle = offline).

        The number of running machines needs to be recalculated when accounting for draining slots.
        Claimed but retiring slots are still being counted as working slots and thus contributing
        to the number of running machines -> remove idle draining slots from running machines
        and recalculate the actual number of running machines.

        :return {machine_type: integer, ...}:
        """
        # fall back to base method if required
        if self.getConfig(self.configIgnoreDrainingMachines) is True:
            return super(FreiburgSiteAdapter, self).runningMachinesCount
        else:
            runningMachines = self.runningMachines
            runningMachinesCount = dict()
            for machineType in runningMachines:
                # calculate number of drained slots (idle and not accepting new jobs -> not usable)
                nDrainedSlots = 0

                for mid in runningMachines[machineType]:
                    nDrainedSlots += HTCondor.calcDrainStatus(self.mr.machines[mid])[0]
                nCores = self.getConfig(self.ConfigMachines)[machineType]["cores"]
                nMachines = len(runningMachines[machineType])
                # Calculate the number of available slots
                # Little trick: floor division with negative values: -9//4 = -3
                nDrainedSlots = -nDrainedSlots
                runningMachinesCount[machineType] = nMachines + nDrainedSlots // nCores
                if nDrainedSlots != 0:
                    self.logger.debug("%s: running: %d, drained slots: %d"
                                      " -> recalculated running machines count: %s"
                                      % (machineType, nMachines, nDrainedSlots,
                                         runningMachinesCount[machineType]))
            return runningMachinesCount

    def onEvent(self, evt):
        # type: (MachineRegistry.MachineEvent) -> None
        """Event handler: Handles machine status changes.

        Freiburg has some special logic here, since machines shutdown themselves after a 5 minute
        delay. This means we only have to cancel jobs, if we change to "Disintegrated" outside the
        regular execution.
        """
        try:
            if self.mr.machines[evt.id].get(self.mr.regSite, None) != self.siteName:
                return
        except KeyError:
            return

        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            self.logger.debug("Status Change Event: %s (%s->%s)"
                              % (evt.id, evt.oldStatus, evt.newStatus))
            if evt.newStatus == self.mr.statusDisintegrated:
                # Disintegrated information comes from integration adapter. Skipping a status only happens when a
                # machine timed out -> cancel VM batch job.
                if (self.mr.machines[evt.id].get(self.regMachineJobId) in self.__runningJobs and
                        evt.oldStatus != self.mr.statusDisintegrating):
                    self.__cancelFreiburgMachines([self.mr.machines[evt.id].get(
                        self.regMachineJobId)])
                self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def manage(self):
        # type: () -> None
        """Manages status changes of machines by checking  jobs in Freiburg.

        Booting = Freiburg batch job for machine was submitted
        Up      = Freiburg batch job is running, VM is Booting,
                  HTCondorIntegrationAdapter switches this to "integrating" and "working".
        Disintegrated & Down

        HTCondorIntegrationAdapter is responsible for handling Integrating, Working,
        PendingDisintegration, Disintegrating
        """
        frJobsRunning = self.__runningJobs
        frJobsCompleted = self.__completedJobs
        mr = self.getSiteMachines()
        for mid in mr:
            batchJobId = mr[mid][self.regMachineJobId]
            # Status handled by Integration Adapter
            if mr[mid][self.mr.regStatus] in [self.mr.statusIntegrating, self.mr.statusWorking,
                                              self.mr.statusPendingDisintegration,
                                              self.mr.statusDisintegrating]:
                try:
                    frJobsRunning.pop(batchJobId)
                    continue
                except KeyError:
                    pass
            # Machines which failed to boot/died/got canceled (return code != 0) -> down
            # -> ROCED becomes aware of failed VM requests and asks for new ones.
            # A machine MAY fail to boot with return code 0. Could be regular shutdown -> shutdown
            if mr[mid][self.mr.regStatus] != self.mr.statusDown:
                if batchJobId in frJobsCompleted:
                    if mr[mid][self.mr.regStatus] == self.mr.statusBooting:
                        self.logger.info("VM (%s) failed to boot!" % batchJobId)
                    else:
                        if frJobsCompleted[batchJobId] != "0":
                            self.logger.info("VM (%s) died!" % batchJobId)
                        else:
                            self.logger.debug("VM (%s) died with status 0!" % batchJobId)
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
            elif batchJobId in frJobsCompleted:
                self.mr.removeMachine(mid)
                continue
            elif self.mr.calcLastStateChange(mid) > 60 and batchJobId in frJobsRunning:
                # machine in status down, but job still has not completed after 60 seconds.
                self.__cancelFreiburgMachines(batchJobId)
                frJobsRunning.pop(batchJobId)
                continue

            # batch job running: machine -> up
            if mr[mid][self.mr.regStatus] is self.mr.statusBooting:
                if batchJobId in frJobsRunning:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    frJobsRunning.pop(batchJobId)

        # All remaining unaccounted batch jobs
        for batchJobId in frJobsRunning:
            mid = self.mr.newMachine()
            # TODO: try to identify machine type, using cores & wall-time
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = "fr-default"
            self.mr.machines[mid][self.regMachineJobId] = batchJobId
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getCondorName(batchJobId)
            self.mr.updateMachineStatus(mid, self.mr.statusUp)

        self.logger.info("Machines using resources (Freiburg): %d" % self.cloudOccupyingMachinesCount)

        with JsonLog() as jsonLog:
            jsonLog.addItem(self.siteName, "condor_nodes",
                            len(self.getSiteMachines(status=self.mr.statusWorking)))
            jsonLog.addItem(self.siteName, "condor_nodes_draining",
                            len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))
            jsonLog.addItem(self.siteName, "machines_requested",
                            len(self.getSiteMachines(status=self.mr.statusBooting)) +
                            len(self.getSiteMachines(status=self.mr.statusUp)))

    def __execCmdInFreiburg(self, cmd):
        """Execute command on Freiburg login node via SSH.

        :param cmd:
        :return: Tuple: (return_code, std_out, std_err)
        """
        frSsh = ScaleTools.Ssh(host=self.getConfig(self.configFreiburgServer),
                               username=self.getConfig(self.configFreiburgUser),
                               key=self.getConfig(self.configFreiburgKey))
        return frSsh.handleSshCall(call=cmd, quiet=True)

    def __cancelFreiburgMachines(self, batchJobIds):
        """Cancel batch job (VM) in Freiburg.

        :param batchJobIds:
        :type batchJobIds: list
        :return: [idsRemoved], [idsInvalidated]
        """
        # It is also possible to use just one single command with multiple ids, but no machine gets
        # cancelled if a single id is invalid! This can happen when the VM fails to boot due to
        # network problems.
        command = ""
        if not isinstance(batchJobIds, (list, tuple)):
            batchJobIds = [batchJobIds]
        for batchJobId in batchJobIds:
            command += "mjobctl -c %s; " % batchJobId
        result = self.__execCmdInFreiburg(command)

        # catch 0:"successful" and 1:"invalid job id" return codes
        # the return code of the first cancellation command is returned here, we can handle them
        # both to remove cancelled and invalid machines
        idsRemoved = []
        idsInvalidated = []
        if result[0] <= 1:
            ScaleTools.Ssh.debugOutput(self.logger, "FR-terminate", result)
            idsRemoved += re.findall("\'(\d+)\'", result[1])
            idsInvalidated += re.findall("invalid job specified \((\d+)", result[2])
            if len(idsRemoved) > 0:
                self.logger.info("Terminated machines (%d): %s" % (len(idsRemoved), ", ".join(idsRemoved)))
            if len(idsInvalidated) > 0:
                self.logger.warning("Removed invalid machines (%d): %s"
                                    % (len(idsInvalidated), ", ".join(idsInvalidated)))
            if (len(idsRemoved) + len(idsInvalidated)) == 0:
                self.logger.warning("A problem occurred while canceling VMs (RC %d):\n%s" % (result[0], result[2]))
        else:
            self.logger.warning("A problem occurred while canceling VMs (RC %d):\n%s" % (result[0], result[2]))
        return idsRemoved, idsInvalidated

    @classmethod
    def __getCondorName(cls, batchJobId):
        """Build condor name for communication with HTCondorIntegrationAdapter.

        Machine registry value "reg_site_server_condor_name" is used to communicate with
        HTCondorIntegrationAdapter. In Freiburg this name is built from the batch job id."""
        return cls.__condorNamePrefix + batchJobId

    @property
    def __userString(self):
        # type: () -> str
        """User string for Freiburg SSH query """
        frUser = self.getConfig(self.configFreiburgUser)
        frGroup = self.getConfig(self.configFreiburgUserGroup)

        if frGroup is None:
            res = "-w user=%s" % frUser
        else:
            res = "-w group=%s" % frGroup

        return res

    @property
    @ScaleTools.Caching(validityPeriod=-1, redundancyPeriod=300)
    def __runningJobs(self):
        # type: () -> dict
        """Get list of running batch jobs, filtered by user ID."""
        cmd = "showq -r %s" % self.__userString

        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            # returns a list containing all running batch jobs in Freiburg
            frJobsRunning = {jobid: {"cores": cores, "walltime": time_limit}
                             for jobid, cores, time_limit
                             in re.findall("""
                                    ^                  # Line start
                                    (\d+)              # batch job id (digits) = result 1
                                    \s+                # whitespace(s)
                                    R                  # Job = Running
                                    \s+.+\s            # waste between whitespaces and next regex
                                    (\d)               # cores = result 2
                                    \s+                # whitespace(s)
                                    ((?:\d{1,2}:?){3,4}) # time limit: 3-4 digit pairs = res3
                                    \s+.+              # more waste
                                    """, frResult[1], re.MULTILINE | re.VERBOSE)}
        elif frResult[0] == 255:
            frJobsRunning = {}
            self.logger.warning("SSH connection (showq -r) could not be established.")
            raise ValueError("SSH connection (showq -r) could not be established.")
        else:
            frJobsRunning = {}
            self.logger.warning("Problem running remote command (showq -r) (RC %d):\n%s" % (frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (showq -r) (RC %d):\n%s" % (frResult[0], frResult[2]))

        self.logger.debug("Running:\n%s" % frJobsRunning)
        return frJobsRunning

    @property
    @ScaleTools.Caching(validityPeriod=-1, redundancyPeriod=300)
    def __idleJobs(self):
        # type: () -> list
        """Get list of idle (submitted, but not yet started) batch jobs, filtered by user ID."""
        cmd = "showq -i %s" % self.__userString

        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            # returns a list containing all running batch jobs in Freiburg
            # Negative lookahead for "eligible", otherwise he catches "0 eligible jobs".
            frJobsIdle = re.findall("^(\d+)\s+(?!eligible)", frResult[1], re.MULTILINE)
        elif frResult[0] == 255:
            frJobsIdle = []
            self.logger.warning("SSH connection (showq -i) could not be established.")
            raise ValueError("SSH connection (showq -i) could not be established.")
        else:
            frJobsIdle = []
            self.logger.warning("Problem running remote command (showq -i) (RC %d):\n%s" % (frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (showq -i) (RC %d):\n%s" % (frResult[0], frResult[2]))

        self.logger.debug("Idle:\n%s" % frJobsIdle)
        return frJobsIdle

    @property
    @ScaleTools.Caching(validityPeriod=-1, redundancyPeriod=300)
    def __completedJobs(self):
        # type: () -> dict
        """Get list of completed batch jobs, filtered by user ID."""
        cmd = "showq -c %s" % self.__userString

        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            # returns a dict: {batch job id: return code/status, ..}
            frJobsCompleted = {jobid: rc for jobid, rc in
                               re.findall("""
                               ^             # Match at the beginning of lines
                               (\d+)         # batch job id (digits) = result 1
                               \s+           # whitespace/tab
                               [RCV]         # job state: completed, vacated, removed
                               \s+           # whitespace/tab
                               ([-A-Z0-9]+)  # Return-code = result 2: +/- int or CNCLD
                               \s+.+         # useless rest
                               """, frResult[1], re.MULTILINE | re.VERBOSE)}
        elif frResult[0] == 255:
            frJobsCompleted = {}
            self.logger.warning("SSH connection (showq -c) could not be established.")
            raise ValueError("SSH connection (showq -c) could not be established.")
        else:
            frJobsCompleted = {}
            self.logger.warning("Problem running remote command (showq -c) (RC %d):\n%s" % (frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (showq -c) (RC %d):\n%s" % (frResult[0], frResult[2]))

        self.logger.debug("Completed:\n%s" % frJobsCompleted)
        return frJobsCompleted

    @staticmethod
    def __merge_dicts(*dict_args):
        # type: (*dict) -> dict
        """Given any number of dicts, shallow copy and merge into a new dict.
        Precedence goes to key value pairs in latter dicts.
        """
        result = {}
        for dictionary in dict_args:
            result.update(dictionary)
        return result
