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
from __future__ import unicode_literals, absolute_import

import logging
import re

from Core import MachineRegistry, Config
from IntegrationAdapter.HTCondorIntegrationAdapter import HTCondorIntegrationAdapter as HTCondor
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog
from Util.PythonTools import Caching, merge_dicts
from Util.ScaleTools import Ssh


class FreiburgSiteAdapter(SiteAdapterBase):
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

    __Jobname = "ROCED_VM"
    __vmStartScript = "startVM.py"
    """Python script to be executed in Freiburg. This starts the VM with the corresponding image.
    This python script has to be adapted on the server with user name, OpenStack Dashboard PW,
    image GUID, etc.
    """
    __hostNamePrefix = "moab-vm-"
    """VM machine name. This is also used as machine name in condor by us."""

    def __init__(self):
        """Site Adapter for Freiburg bwForCluster ENM OpenStack setup."""
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

        self.__default_machine = "vm-default"

    def init(self):
        self.mr.registerListener(self)
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))
        super(FreiburgSiteAdapter, self).init()

        # Machines that are found running get this type by default
        self.__default_machine = list(self.getConfig(self.ConfigMachines).keys())[0]

    def spawnMachines(self, machineType, count):
        """Request machines via MOAB batch job (which executes startVM script).

        Batch job configuration is done via ROCED config file.
        OpenStack parameters (user login, image name, ..) are set in startVM script on Freiburg login node..
        """
        super(FreiburgSiteAdapter, self).spawnMachines(machineType, count)

        maxMachinesPerCycle = self.getConfig(self.configMaxMachinesPerCycle)
        machineSettings = self.getConfig(self.ConfigMachines)[machineType]

        if count > maxMachinesPerCycle:
            self.logger.info("%d machines requested, limited to %d for this cycle." % (count, maxMachinesPerCycle))
            count = maxMachinesPerCycle

        result = self.__execCmdInFreiburg("msub -m p -l walltime=%s, mem=%s, nodes=1:ppn%d "
                                          # Submit a job array with size "count"
                                          "-t %s[1-%d] %s"
                                          % (machineSettings["walltime"], machineSettings["memory"],
                                             machineSettings["cores"], self.__Jobname,
                                             count, self.__vmStartScript))
        if result[0] != 0:
            self.logger.warning("A problem occurred while requesting VMs. RC: %d; stdout: %s; stderr: %s"
                                % (result[0], result[1], result[2]))
            return

        for job_id in self.__resolveJobArray(result[1].strip(), count):
            mid = self.mr.newMachine()
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regMachineType] = machineType
            self.mr.machines[mid][self.regMachineJobId] = job_id
            self.mr.machines[mid]["cluster_size"] = count
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getVmHostName(job_id)
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

    def terminateMachines(self, machineType, count):
        """Terminate machines. Only **queued** jobs/machines are killed. Running machines may be drained."""
        # booting machines, sorted by request time (newest first).
        bootingMachines = self.getSiteMachines(self.mr.statusBooting, machineType)
        try:
            bootingMachines = sorted(bootingMachines.items(),
                                     key=lambda machine_: machine_[1][self.mr.regStatusLastUpdate],
                                     reverse=True)
        except KeyError:
            bootingMachines = []

        # Running machines, sorted by load (idle first). These machines are put into drain mode
        if self.getConfig(self.configDrainWorkingMachines) is True:
            workingMachines = merge_dicts(
                self.getSiteMachines(self.mr.statusIntegrating, machineType),
                self.getSiteMachines(self.mr.statusWorking, machineType),
                self.getSiteMachines(self.mr.statusPendingDisintegration, machineType))
            try:
                workingMachines = sorted(workingMachines.items(),
                                         key=lambda machine_: HTCondor.calcMachineLoad(machine_[0]),
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

        for mid, machine in machinesToRemove:
            if machine[self.mr.regStatus] == self.mr.statusBooting:
                idsToTerminate.append(machine[self.regMachineJobId])
            elif self.getConfig(self.configDrainWorkingMachines):
                # TODO: Remove hard HTCondor dependency
                if HTCondor.calcDrainStatus(mid)[1] is True:
                    continue
                idsToDrain.append(machine[self.regMachineJobId])

        self.logger.debug("Machines to terminate (%d): %s" % (len(idsToTerminate), ", ".join(idsToTerminate)))
        if idsToTerminate:
            idsRemoved, idsInvalidated = self.__cancelFreiburgMachines(idsToTerminate)

        self.logger.debug("Machines to drain (%d): %s" % (len(idsToDrain), ", ".join(idsToDrain)))
        if idsToDrain:
            [HTCondor.drainMachine(mid) for mid, machine in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsToDrain]

        if len(idsRemoved + idsInvalidated) > 0:
            [self.mr.updateMachineStatus(mid, self.mr.statusDown) for mid, machine in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsRemoved + idsInvalidated]

    @property
    def runningMachinesCount(self):
        """
        Return number of machines running, possibly accounting for draining slots (claimed|retiring vs. drained|idle).

        Claimed but retiring slots are counted as working slots ( = running machine(s))
         -> Recalculate running machines without *idle* drained slots

        :return {machine_type: integer, ...}:
        """
        if self.getConfig(self.configIgnoreDrainingMachines) is True:
            return super(FreiburgSiteAdapter, self).runningMachinesCount
        else:
            runningMachines = self.runningMachines
            runningMachinesCount = dict()
            for machineType in runningMachines:
                # calculate number of drained slots (idle and not accepting new jobs -> not usable)
                nDrainedSlots = 0

                for mid in runningMachines[machineType]:
                    # TODO: Get rid of hard dependency to run Condor in VMs // more generic approach
                    nDrainedSlots += HTCondor.calcDrainStatus(mid)[0]
                nCores = self.getConfig(self.ConfigMachines)[machineType]["cores"]
                nMachines = len(runningMachines[machineType])
                # Number of available slots: floor division with negative values: -9//4 = -3
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

        - Machines (should) disintegrate automatically, if they are idle for a certain amount of time.
        - After disintegrating, the machines (should) shut down automatically.

        -> Only cancel jobs, if a machines changes to "Disintegrated" _outside_ the regular order.
        """
        try:
            if self.mr.machines[evt.id].get(self.mr.regSite, None) != self.siteName:
                return
        except KeyError:
            return

        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            self.logger.debug("Status Change Event: %s (%s->%s)" % (evt.id, evt.oldStatus, evt.newStatus))
            if evt.newStatus == self.mr.statusDisintegrated:
                try:
                    if evt.oldStatus != self.mr.statusDisintegrating:
                        if self.mr.machines[evt.id].get(self.regMachineJobId) in self.__runningJobs:
                            self.__cancelFreiburgMachines([self.mr.machines[evt.id].get(self.regMachineJobId)])
                except Exception as err:
                    self.logger.warning("Canceling machine failed with exception %s" % err)
                self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def manage(self, cleanup=False):
        # type: (bool) -> None
        """Manages machine state changes by checking Freiburg MOAB job states.

        Booting       = Batch job for machine was submitted
        Up            = Batch job is running, VM is Booting.

        IntegrationAdapter handles Integrating, Working, PendingDisintegration, Disintegrating

        Disintegrated = Option 1: Condor shut down regularly because system passed idle threshold.
                                  VM will shut down shortly after.
                        Option 2: VM job crashed/was canceled completely.
        Down          = Job is in status completed.
                        **Caution**: Job update interval may be slower than OpenStack (Machine is shut down,
                                     but job keeps running for up to 30 seconds).
        """
        try:
            frJobsRunning = self.__runningJobs
            if frJobsRunning is None:
                raise ValueError
        except ValueError:
            frJobsRunning = {}
        try:
            frJobsCompleted = self.__completedJobs
            if frJobsCompleted is None:
                raise ValueError
        except ValueError:
            frJobsCompleted = {}
        try:
            frJobsIdle = self.__idleJobs
            if frJobsIdle is None:
                raise ValueError
        except ValueError:
            frJobsIdle = {}

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
                except (KeyError, AttributeError):
                    # AttributeError: frJobsRunning is Empty
                    # KeyError: batchJobId not in frJobsRunning
                    pass
            # Machines which failed to boot/died/got canceled (return code != 0) -> down
            # A machine MAY fail to boot with return code 0 or we missed some states -> regular shutdown
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
            elif batchJobId in frJobsCompleted or self.mr.calcLastStateChange(mid) > 24 * 60 * 60:
                # Remove machines, which are:
                # 1. finished in ROCED & Freiburg // 2. Finished for more than 1 day [= job history purge time]
                self.mr.removeMachine(mid)
                continue
            elif batchJobId in frJobsRunning:
                # ROCED machine down, but job still running
                frJobsRunning.pop(batchJobId)
                if self.mr.calcLastStateChange(mid) > 5*60:
                    self.__cancelFreiburgMachines(batchJobId)
                continue

            if mr[mid][self.mr.regStatus] == self.mr.statusBooting:
                # batch job running: machine -> up
                if batchJobId in frJobsRunning:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    frJobsRunning.pop(batchJobId)
                # Machine "disappeared". If the machine later appears again, it will be added automatically.
                elif batchJobId not in frJobsIdle and batchJobId not in frJobsCompleted:
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)

        # All remaining unaccounted batch jobs
        for batchJobId in frJobsRunning:
            mid = self.mr.newMachine()
            # TODO: try to identify machine type, using cores & wall-time
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = self.__default_machine
            self.mr.machines[mid][self.regMachineJobId] = batchJobId
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getVmHostName(batchJobId)
            self.mr.updateMachineStatus(mid, self.mr.statusUp)

        self.logger.info("Machines using resources (Freiburg): %d" % self.cloudOccupyingMachinesCount)

        with JsonLog() as jsonLog:
            jsonLog.addItem(self.siteName, "condor_nodes", len(self.getSiteMachines(status=self.mr.statusWorking)))
            jsonLog.addItem(self.siteName, "condor_nodes_draining",
                            len([mid for mid in self.getSiteMachines(status=self.mr.statusPendingDisintegration)
                                 if HTCondor.calcDrainStatus(mid)[1] is True]))
            jsonLog.addItem(self.siteName, "machines_requested",
                            len(self.getSiteMachines(status=self.mr.statusBooting)) +
                            len(self.getSiteMachines(status=self.mr.statusUp)) +
                            len(self.getSiteMachines(status=self.mr.statusIntegrating)))

    def __execCmdInFreiburg(self, cmd):
        # type: (str) -> Tuple[int, str, str]
        """Execute command on Freiburg login node via SSH.

        :return: Tuple: (return_code, std_out, std_err)
        """
        frSsh = Ssh(host=self.getConfig(self.configFreiburgServer),
                    username=self.getConfig(self.configFreiburgUser),
                    key=self.getConfig(self.configFreiburgKey))
        return frSsh.handleSshCall(call=cmd, quiet=True)

    def __cancelFreiburgMachines(self, batchJobIds):
        """Cancel MOAB batch job (VM).

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
            Ssh.debugOutput(self.logger, "FR-terminate", result)
            idsRemoved += re.findall("\'(\d+)\'", result[1])
            idsInvalidated += re.findall("invalid job specified \((\d+)", result[2])
            if len(idsRemoved) > 0:
                self.logger.info("Terminated machines (%d): %s" % (len(idsRemoved), ", ".join(idsRemoved)))
            if len(idsInvalidated) > 0:
                self.logger.warning("Removed invalid machines (%d): %s"
                                    % (len(idsInvalidated), ", ".join(idsInvalidated)))
            if (len(idsRemoved) + len(idsInvalidated)) == 0:
                self.logger.warning("A problem occurred while canceling VMs (RC: %d)\n%s" % (result[0], result[2]))
        else:
            self.logger.warning("A problem occurred while canceling VMs (RC: %d)\n%s" % (result[0], result[2]))
        return idsRemoved, idsInvalidated

    @classmethod
    def __getVmHostName(cls, batchJobId):
        # type: (str) -> str
        """Build VM host name for communication with Integration Adapter.

        Name is built from static prefix and batch job id."""
        # Get rid of [] via translate "deletechars"
        # TODO: This is not compatible between python versions
        return cls.__hostNamePrefix + batchJobId.translate(None, b"[]")

    @property
    def __userString(self):
        # type: () -> str
        """User string for Freiburg SSH queries."""
        frUser = self.getConfig(self.configFreiburgUser)
        frGroup = self.getConfig(self.configFreiburgUserGroup)

        if frGroup is None:
            res = "-w user=%s" % frUser
        else:
            res = "-w group=%s" % frGroup

        return res

    @property
    @Caching(validityPeriod=30, redundancyPeriod=300)
    def __Jobs(self):
        # type: () -> dict
        """Get list of running and recently completed batch jobs (current user)."""
        cmd = "qstat -r -l -t -u %s" % self.getConfig(self.configFreiburgUser)
        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            frJobs = {jobid: {"status": status, "cores": cores, "memory": memory, "wall_time": wall_time}
                      for jobid, cores, memory, wall_time, status
                      in re.findall("""^                        # Line start; \s+ or .+ denotes separators/trash
                                       (\d+(\[\d+\])?).+        # JobId + Array
                                       (?:%s).+                 # Username (non-capturing group)
                                       (?:%s)\s+                # Job Name (non-capturing group)
                                       (?:\S+\s+){2}            # 2 x trash + separator/stuff (non-capturing group)
                                       (\d+)\s+                 # Number of CPU cores
                                       (\d+)gb\s+               # Memory
                                       ((?:\d{1,2}:?){3,4})\s+  # Wall Time; 3-4 groups of 00:
                                       ([CEHQRTWS]).+$          # Job status (single char) // trash, line end
                                    """ % (self.getConfig(self.configFreiburgUser), self.__vmStartScript),
                                    frResult[1], re.IGNORECASE | re.MULTILINE | re.VERBOSE)}
        elif frResult[0] == 255:
            self.logger.warning("SSH connection (%s) could not be established." % cmd)
            raise ValueError("SSH connection (%s) could not be established." % cmd)
        else:
            self.logger.warning("Problem running remote command (%s) (RC %d):\n%s" % (cmd, frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (%s) (RC %d):\n%s" % (cmd, frResult[0], frResult[2]))

        self.logger.debug("Running:\n%s" % frJobs.keys())
        return frJobs

    @property
    @Caching(validityPeriod=-1, redundancyPeriod=300)
    def __idleJobs(self):
        # type: () -> list
        """Get a list of idle (submitted, but not yet started) batch jobs, filtered by user ID."""
        result = []
        # 1. Non-blocking Query
        # 2. Last 4 lines show totals
        cmd = "showq -i --noblock %s | head -n -4" % self.__userString

        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            result.extend(self.__resolveJobArray(jobid, size) for jobid, size in
                          re.findall("^(\d+)\[(\d+)\]", frResult[1], re.MULTILINE))
        elif frResult[0] == 255:
            self.logger.warning("SSH connection (showq -i) could not be established.")
            raise ValueError("SSH connection (showq -i) could not be established.")
        else:
            self.logger.warning("Problem running remote command (showq -i) (RC %d):\n%s" % (frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (showq -i) (RC %d):\n%s" % (frResult[0], frResult[2]))

        cmd = "checkjob all"
        frResult = self.__execCmdInFreiburg(cmd)

        self.logger.debug("Idle:\n%s" % result)
        return result

    @staticmethod
    def __resolveJobArray(job_id, size=1):
        # type (str, str) -> list
        """Resolve Freiburg job_id and array size to a list of job numbers"""
        return ["%s[%s]" % (job_id, x) for x in range(1, int(size))]

        # @property
        # @Caching(validityPeriod=-1, redundancyPeriod=300)
        # def __completedJobs(self):
        #     # type: () -> dict
        #     """Get list of completed batch jobs, filtered by user ID."""
        #     cmd = "showq -c %s" % self.__userString
        #
        #     frResult = self.__execCmdInFreiburg(cmd)
        #
        #     if frResult[0] == 0:
        #         # returns a dict: {batch job id: return code/status, ..}
        #         frJobsCompleted = {jobid: rc for jobid, rc in
        #                            re.findall("""
        #                            ^             # Match at the beginning of lines
        #                            (\d+)         # batch job id (digits) = result 1
        #                            (?:\(\d+\))?  # Array job
        #                            \s+           # whitespace/tab
        #                            [RCV]         # job state: completed, vacated, removed
        #                            \s+           # whitespace/tab
        #                            ([-A-Z0-9]+)  # Return-code = result 2: +/- int or CNCLD
        #                            \s+.+$        # useless rest
        #                            """, frResult[1], re.MULTILINE | re.VERBOSE)}
        #     elif frResult[0] == 255:
        #         frJobsCompleted = {}
        #         self.logger.warning("SSH connection (showq -c) could not be established.")
        #         raise ValueError("SSH connection (showq -c) could not be established.")
        #     else:
        #         frJobsCompleted = {}
        #         self.logger.warning("Problem running remote command (showq -c) (RC %d):\n%s" % (frResult[0], frResult[2]))
        #         raise ValueError("Problem running remote command (showq -c) (RC %d):\n%s" % (frResult[0], frResult[2]))
        #
        #     self.logger.debug("Completed:\n%s" % frJobsCompleted)
        #     return frJobsCompleted
