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
from Util.Adaptive import Moab, Torque
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

    # TODO: Can we safely switch to MachineRegistry.regHostname? (+ adapt HTCondor adapters; diff machine & hostname?)
    reg_site_server_condor_name = HTCondor.reg_site_server_condor_name
    regMachineJobId = "batch_job_id"

    # Python script (running in FR) which starts the VM. Script has to be adapted with user name, PW, image GUID, etc.
    __vmStartScript = "startVM.py"
    # VM host name (defined in start script)
    __hostNamePrefix = "moab-vm-"
    # Moab job name
    __Jobname = "ROCED_VM"

    def __init__(self):
        """Site Adapter for Freiburg bwForCluster ENM OpenStack setup.

        The adapter interfaces with a Moab batch system (via SSH CLI) by submitting VMs as batch jobs.
        These batch jobs query OpenStack to boot a VM using "their" resources.

        VM handling is mostly automated, in order to be fail-safe and network independent:
            - VMs will automatically shutdown when they don't get a job for a certain time
            - VMs will refuse jobs which run longer than their lifetime and/or switch to drain mode
              when close to the maximum allowed lifetime.
        -> Handling of these cases is only implemented as a fall-back.
        """
        super(FreiburgSiteAdapter, self).__init__()

        self.addCompulsoryConfigKeys(self.configFreiburgUser, Config.ConfigTypeString,
                                     description="Account name for bwForCluster login node")
        self.addCompulsoryConfigKeys(self.configFreiburgKey, Config.ConfigTypeString,
                                     description="SSH Key for bwForCluster login node")
        self.addCompulsoryConfigKeys(self.configFreiburgServer, Config.ConfigTypeString,
                                     description="Hostname of bwForCluster login node")

        self.addCompulsoryConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                     "Maximum number of machines to boot in a management cycle")

        self.addOptionalConfigKeys(self.configSiteLogger, Config.ConfigTypeString,
                                   description="Logger name of Site Adapter", default="FRSite")

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
        super(FreiburgSiteAdapter, self).init()
        self.mr.registerListener(self)
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))

        # Machines that are found running get this type by default
        self.__default_machine = list(self.getConfig(self.ConfigMachines).keys())[0]

    def spawnMachines(self, machineType, count):
        """Request machines via Moab batch job (which executes startVM script).

        Batch job configuration is done via ROCED config file.
        OpenStack parameters (user login, image name, ..) are set in startVM script on Freiburg login node..
        """
        super(FreiburgSiteAdapter, self).spawnMachines(machineType, count)

        maxMachinesPerCycle = self.getConfig(self.configMaxMachinesPerCycle)
        machineSettings = self.getConfig(self.ConfigMachines)[machineType]

        if count > maxMachinesPerCycle:
            self.logger.info("%d machines requested, limited to %d for this cycle." % (count, maxMachinesPerCycle))
            count = maxMachinesPerCycle

        result = self.__execCmdInFreiburg("msub -m p -l walltime=%s, mem=%s, nodes=1:ppn=%d "
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
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getVmHostName(job_id)
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)

    def terminateMachines(self, machineType, count):
        """Terminate machines. Only *queued* jobs/machines are killed. Running machines may get drained."""
        try:
            bootingMachines = self.getSiteMachines(self.mr.statusBooting, machineType)
            # Sort by request time (newest first)
            bootingMachines = sorted(bootingMachines.items(),
                                     key=lambda machine_: machine_[1][self.mr.regStatusLastUpdate],
                                     reverse=True)
        except KeyError:
            bootingMachines = []

        if self.getConfig(self.configDrainWorkingMachines) is True:
            try:
                workingMachines = merge_dicts(
                    self.getSiteMachines(self.mr.statusIntegrating, machineType),
                    self.getSiteMachines(self.mr.statusWorking, machineType),
                    self.getSiteMachines(self.mr.statusPendingDisintegration, machineType))
                # Sort by load (idle first).
                workingMachines = sorted(workingMachines.items(),
                                         key=lambda machine_: HTCondor.calcMachineLoad(machine_[0]),
                                         reverse=True)
            except KeyError:
                workingMachines = []

            # Merge lists
            machinesToRemove = bootingMachines + workingMachines
        else:
            machinesToRemove = bootingMachines

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
            # TODO: Remove hard HTCondor dependency
            [HTCondor.drainMachine(mid) for mid, machine in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsToDrain]

        if len(idsRemoved + idsInvalidated) > 0:
            [self.mr.updateMachineStatus(mid, self.mr.statusDown) for mid, machine in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsRemoved + idsInvalidated]

    @property
    def runningMachinesCount(self):
        """Number of machines running, (possibly) accounting for draining slots (claimed|retiring vs. drained|idle).

        This property is used by the Broker, when deciding if/where new machines should be booted.
        Claimed | retiring slots are (correctly) counted as available resource.
        Drained | idle slots are counted as available resource, too, although they won't accept new jobs.
        -> Configuration allows to recalculate running machines without counting drained | idle slots.

        :return {machine_type: integer, ...}:
        """
        if self.getConfig(self.configIgnoreDrainingMachines) is True:
            return super(FreiburgSiteAdapter, self).runningMachinesCount
        else:
            runningMachines = self.runningMachines
            runningMachinesCount = dict()
            for machineType in runningMachines:
                nDrainedSlots = 0

                for mid in runningMachines[machineType]:
                    # TODO: Get rid of hard dependency to run Condor // use more generic approach (cores/slots)
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
        """Event handler: Reacts on Machine Registry machine state changes.

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
        """Manages machine state changes by checking Freiburg Moab job states.

        Booting       = Batch job for machine was submitted
        Up            = Batch job is running, VM is Booting.

        IntegrationAdapter handles Integrating, Working, PendingDisintegration, Disintegrating

        Disintegrated = Option 1: Condor shut down regularly because system passed idle threshold.
                                  VM will shut down shortly after.
                        Option 2: VM job crashed/was canceled completely.
        Down          = Job is in status completed.
                        **Caution**: Job update interval is slower than OpenStack (Machine is shut down,
                                     but job keeps running for up to 30 seconds).

        The process relies on querying Moab/Torque for information via qstat or checkjob all.
        * Qstat is evaluated each cycle, looking at running and recently completed jobs.
          Finished jobs are only moved to status Down.
        * Checkjob is evaluated each cleanup cycle. It looks at idle & finished jobs and
          cleans the Machine Registry accordingly.
        """
        frJobs = self.__Jobs

        if cleanup:
            all_jobs = self.__allJobs
        else:
            all_jobs = None

        ###
        # Step 1: Loop known machines and handle state changes
        # - Get (Freiburg) job id & state
        # o Disappeared booting machines are cleaned up in cleanup cycle
        # o All other disappeared machines are moved to down
        # If batch job (still) exists, pop it from list of all running jobs
        ###
        for mid, machine in self.getSiteMachines().items():

            job_id = machine[self.regMachineJobId]
            try:
                job_state = frJobs[job_id].get("status")
            except KeyError:
                job_state = None
                if machine[self.mr.regStatus] == self.mr.statusBooting and cleanup is False:
                    continue
                elif machine[self.mr.regStatus] != self.mr.statusDown:
                    self.logger.info("VM (%s) disappeared!" % job_id)
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                    if cleanup is False:
                        continue

            if machine[self.mr.regStatus] in self.integration_states and job_state in Torque.all_job_running:
                # Integration adapter(s) handle running jobs
                frJobs.pop(job_id)
                continue
            elif machine[self.mr.regStatus] == self.mr.statusBooting:
                if job_state == Torque.all_job_running:
                    # batch job running: machine -> up
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    frJobs.pop(job_id)
                elif job_state == Torque.all_job_finished:
                    self.logger.info("VM (%s) failed to boot!" % job_id)
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
            elif machine[self.mr.regStatus] != self.mr.statusDown:
                if job_state in Torque.all_job_finished:
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                elif job_state in Torque.all_job_running:
                    frJobs.pop(job_id)

            ###
            # Step 1.5: Cleanup cycle
            ###
            if cleanup is True:
                if machine[self.mr.regStatus] == self.mr.statusBooting:
                    # is batch job still submitted?
                    if all_jobs[job_id].get("status") != "submitted":
                        continue
                    else:
                        # if frJobsCompleted[batchJobId] != "0":
                        #     self.logger.info("VM (%s) died!" % batchJobId)
                        # else:
                        #     self.logger.debug("VM (%s) died with status 0!" % batchJobId)
                        # self.mr.updateMachineStatus(mid, self.mr.statusDown)
                        pass
                elif machine[self.mr.regStatus] != self.mr.statusDown:
                    # timed out?
                    pass
                elif machine[self.mr.regStatus] == self.mr.statusDown:
                    # cleanup:
                    # job finished > X minutes
                    # machine down (evaluate RC)
                    pass

        ###
        # Step 2: Handle (yet) unaccounted batch jobs
        ###
        for job_id in frJobs:
            if frJobs[job_id].get("status") not in Torque.all_job_running:
                continue
            mid = self.mr.newMachine()
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            # TODO: try to identify machine type, using cores & wall-time
            self.mr.machines[mid][self.mr.regMachineType] = self.__default_machine
            self.mr.machines[mid][self.regMachineJobId] = job_id
            self.mr.machines[mid][self.reg_site_server_condor_name] = self.__getVmHostName(job_id)
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
        # type: (Union[str, unicode]) -> Tuple[int, str, str]
        """Execute command on Freiburg login node via SSH.

        :return: Tuple: (return_code, std_out, std_err)
        """
        frSsh = Ssh(host=self.getConfig(self.configFreiburgServer),
                    username=self.getConfig(self.configFreiburgUser),
                    key=self.getConfig(self.configFreiburgKey))
        return frSsh.handleSshCall(call=cmd, quiet=True)

    def __cancelFreiburgMachines(self, batchJobIds):
        """Cancel Moab batch job (VM).

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

    @property
    @Caching(validityPeriod=30, redundancyPeriod=300, default=dict())
    def __Jobs(self):
        # type: () -> dict
        """List of running and recently completed batch jobs (current user)."""
        # a/r: all/running jobs // l: long job name // t: Expand output to array // u: user filter
        cmd = "qstat -altu %s" % self.getConfig(self.configFreiburgUser)
        frResult = self.__execCmdInFreiburg(cmd)

        if frResult[0] == 0:
            frJobs = Torque.parse_qstat(frResult)
        elif frResult[0] == 255:
            self.logger.warning("SSH connection (%s) could not be established." % cmd)
            raise ValueError("SSH connection (%s) could not be established." % cmd)
        else:
            self.logger.warning("Problem running remote command (%s) (RC %d):\n%s" % (cmd, frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (%s) (RC %d):\n%s" % (cmd, frResult[0], frResult[2]))

        self.logger.debug("Running:\n%s" % frJobs.keys())
        return frJobs

    @property
    @Caching(validityPeriod=-1, redundancyPeriod=300, default=dict())
    def __allJobs(self):
        # type: () -> list
        """Details on all batch jobs. Including resolved arrays, matchmaking, etc."""
        frResult = self.__execCmdInFreiburg("checkjob -v all --flags=complete")
        result = list(Moab.parse_checkjob(frResult))
        return result

    @classmethod
    def __getVmHostName(cls, batchJobId):
        # type: (str) -> str
        """Build VM host name for communication with Integration Adapter.

        Name is built from static prefix and batch job id."""
        # Get rid of []
        return cls.__hostNamePrefix + batchJobId.translate({91: None, 93: None})

    @staticmethod
    def __resolveJobArray(job_id, size=1):
        # type (str, str) -> list
        """Resolve job_id and array size to a list of Moab batch job numbers"""
        return ["%s[%s]" % (job_id, x) for x in range(1, int(size))]
