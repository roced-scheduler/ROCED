# ==============================================================================
#
# Copyright (c) 2015 by Georg Fleig
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


import datetime
import logging
import math
import re
from collections import defaultdict

from Core import MachineRegistry, Config
from SiteAdapter.Site import SiteAdapterBase
from Util import ScaleTools
from Util.Logging import JsonLog


class FreiburgSiteAdapter(SiteAdapterBase):
    """
    Site Adapter for Freiburg bwForCluster ENM OpenStack setup.
    """

    # possible improvements:
    # - use new HTCondor integration adapter for status changes (minimize manage function)
    # - possibly use machine load calculation from HTCondor integration adapter

    configFreiburgUser = "freiburg_user"
    configFreiburgKey = "freiburg_key"
    configFreiburgServer = "freiburg_server"
    configMaxMachinesPerCycle = "max_machines_per_cycle"
    configIgnoreDrainingMachines = "ignore_draining_machines"
    configDrainWorkingMachines = "drain_working_machines"
    configCondorUser = "condor_user"
    configCondorKey = "condor_key"
    configCondorServer = "condor_server"

    regMachineJobId = "batch_job_id"
    regMachineCondorSlotStatus = "condor_slot_status"

    def __init__(self):
        SiteAdapterBase.__init__(self)

        self.addCompulsoryConfigKeys(self.configFreiburgUser, Config.ConfigTypeString, "User name for bwForCluster")
        self.addCompulsoryConfigKeys(self.configFreiburgKey, Config.ConfigTypeString, "Password for bwForCluster")
        self.addCompulsoryConfigKeys(self.configFreiburgServer, Config.ConfigTypeString, "SSH Key for bwForCluster")
        self.addCompulsoryConfigKeys(self.configMaxMachinesPerCycle, Config.ConfigTypeInt,
                                     "Maximum number of machines to boot in a management cycle")
        self.addCompulsoryConfigKeys(self.configIgnoreDrainingMachines, Config.ConfigTypeBoolean,
                                     "Draining (pending-disintegration) machines are counted as working machines (True) or are only partially counted, depending on their slot occupation (False)")
        self.addCompulsoryConfigKeys(self.configDrainWorkingMachines, Config.ConfigTypeBoolean,
                                     "In case a working machine should get terminated, set it to drain mode (True) or do noting (False)")
        self.addCompulsoryConfigKeys(self.configCondorUser, Config.ConfigTypeString, "User name for HTCondor host")
        self.addCompulsoryConfigKeys(self.configCondorKey, Config.ConfigTypeString, "SSH Key for HTCondor host")
        self.addCompulsoryConfigKeys(self.configCondorServer, Config.ConfigTypeString, "Password for HTCondor host")

        self.logger = logging.getLogger("FRSite")

        self.mr = MachineRegistry.MachineRegistry()

    def init(self):
        self.mr.registerListener(self)

    def spawnMachines(self, machineType, count):
        """
        Request machines in Freiburg via batch job containing startVM script.

        Batch job configuration is done via config file.
        All OpenStack parameters (user login, image name, ..) is set in startVM script.

        :param machineType:
        :param count:
        :return:
        """
        self.logger.info("Spawning " + str(count) + " of type " + str(machineType))

        maxMachinesPerCycle = self.getConfig(self.configMaxMachinesPerCycle)
        machineSettings = self.getConfig(self.ConfigMachines)[machineType]

        if count > maxMachinesPerCycle:
            self.logger.info(str(count) + " machines requested, limited to " +
                             str(maxMachinesPerCycle) + " for this cycle")
            count = maxMachinesPerCycle
        for i in range(count):
            # send batch jobs to boot machines
            result = self.execCmdInFreiburg(
                "msub -l walltime=" + str(machineSettings["walltime"]) + ",mem=" + str(machineSettings["memory"]) +
                ",nodes=1:ppn=" + str(machineSettings["cores"]) + " startVM_0.2.py")
            # ScaleTools.sshDebugOutput(self.logger, "FR-spawn", result)

            if result[0] == 0 and result[1].strip().isdigit():
                mid = self.mr.newMachine()
                self.mr.machines[mid][self.mr.regSite] = self.getSiteName()
                self.mr.machines[mid][self.mr.regSiteType] = self.getSiteType()
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.regMachineJobId] = result[1].strip()  # get batch job id from SSH result
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
                # todo: can be removed if there is no need to be aware of number of slots of machines
                self.mr.machines[mid][self.mr.regMachineCores] = machineSettings["cores"]
            else:
                self.logger.warning(
                    "A (connection) problem occurred while requesting a new VM via msub in Freiburg, stop requesting new machines for now." +
                    " (" + str(result[0]) + "): stdout: " + str(result[1]) + ", stderr:" + str(result[2]))
                break

    def rateWorkingMachines(self, machine):
        """
        Calculate score based on the utilization of a machine which is then used to select suitable machines
        for termination (drain mode). The more slots on a machine are used, the less likely it gets drained.

        :param machine:
        :return: -1 (idle) and 0 (fully loaded)
        """
        nSlotsFree = 0
        nSlotsTotal = 0
        if self.regMachineCondorSlotStatus in machine.keys():
            for state, activity in machine[self.regMachineCondorSlotStatus]:
                nSlotsTotal += 1
                if state == "Unclaimed":
                    nSlotsFree += 1
        return 0 if nSlotsTotal == 0 else -nSlotsFree / float(nSlotsTotal)

    def terminateMachines(self, machineType, count):
        """
        Terminate (booting) machines in Freiburg. Working machines untouched by default, but they cloud get put into
        drain mode.

        :param machineType:
        :param count:
        :return:
        """
        # get a list of tuples of suitable machines that can be terminated
        # first select booting machines and sort them by request time (new machines first)
        bootingMachines = self.getSiteMachines(self.mr.statusBooting, machineType)
        bootingMachines = sorted(bootingMachines.items(), key=lambda v: (v[1][self.mr.regStatusLastUpdate]),
                                 reverse=True)
        if self.getConfig(self.configDrainWorkingMachines):
            # then get working machines and sort them by load (idle machines first) if requested
            workingMachines = self.getSiteMachines(self.mr.statusWorking, machineType)
            workingMachines = sorted(workingMachines.items(), key=lambda v: self.rateWorkingMachines(v[1]))
            # merge both lists of tuples
            machinesToRemove = bootingMachines + workingMachines
        else:
            machinesToRemove = bootingMachines
        # only pick the needed amount of machines
        machinesToRemove = machinesToRemove[0:count]

        # prepare list of machine ids to terminate/drain
        idsToTerminate = []
        idsToDrain = []
        idsRemoved = []
        idsInvalidated = []
        for machine in machinesToRemove:
            if machine[1][self.mr.regStatus] == self.mr.statusBooting:
                # booting machines can be terminated immediately
                idsToTerminate.append(machine[1][self.regMachineJobId])
            elif self.getConfig(self.configDrainWorkingMachines):
                # working machines should be set to drain mode
                idsToDrain.append(machine[1][self.regMachineJobId])
        self.logger.debug("Machines to terminate (" + str(len(idsToTerminate)) + "): " + ", ".join(idsToTerminate))
        self.logger.debug("Machines to drain (" + str(len(idsToDrain)) + "): " + ", ".join(idsToDrain))

        if idsToTerminate:
            idsRemoved, idsInvalidated = self.cancelFreiburgMachines(idsToTerminate)

        if idsToDrain:
            # todo: connect to HTCondor collector and send drain command to nodes, most likely not needed.
            self.logger.warning("Send draining command to VM not yet implemented")
            pass

        if len(idsRemoved + idsInvalidated) > 0:
            mr = self.getSiteMachines()

            # loop over machine registry and set status of terminated (cancelled and invalid) machines to shutdown
            for mid in mr:
                if mr[mid][self.regMachineJobId] in idsRemoved + idsInvalidated:
                    self.mr.updateMachineStatus(mid, self.mr.statusShutdown)

    def getSiteMachines(self, status=None, machineType=None):
        """
        Get machines running at Freiburg site

        :param status:
        :param machineType:
        :return: machine_registry
        """
        return self.mr.getMachines(self.getSiteName(), status, machineType)

    def getRunningMachinesCount(self):
        """
        Recalculate number of machines running in Freiburg (optional).

        The number of running machines needs to be recalculated when taking draining slots into account.
        Remove idle draining slots from running machines and recalculate the actual number of running machines.
        Claimed but retiring slots are still being counted as working slots and thus contributing to the number of
        running machines.

        :return: runningMachinesCount
        """
        # fall back to base method if required
        if self.getConfig(self.configIgnoreDrainingMachines):
            SiteAdapterBase.getRunningMachines()

        else:
            # get all site machines
            mr = self.getSiteMachines()
            runningMachines = dict()

            # create dict containing running machines grouped by machine_type
            for i in self.getConfig(self.ConfigMachines):
                runningMachines[i] = dict()

            # fill dict with running machines
            for (k, v) in mr.iteritems():
                if (v.get(self.mr.regStatus) == self.mr.statusBooting) or \
                        (v.get(self.mr.regStatus) == self.mr.statusUp) or \
                        (v.get(self.mr.regStatus) == self.mr.statusWorking) or \
                        (v.get(self.mr.regStatus) == self.mr.statusPendingDisintegration):
                    runningMachines[v[self.mr.regMachineType]][k] = v

            runningMachinesCount = dict()
            for machineType in runningMachines:
                # calculate number of drained slots (idle and not accepting new jobs -> not usable)
                nDrainedSlots = 0
                for (mid, machine) in runningMachines[machineType].iteritems():
                    for slot in machine.get(self.regMachineCondorSlotStatus, []):
                        if slot[0] == "Drained":
                            nDrainedSlots += 1
                nCores = self.getConfig(self.ConfigMachines)[machineType]["cores"]
                nMachines = len(runningMachines[machineType])
                # calculate the actual number of machines available to run jobs
                runningMachinesCount[machineType] = int(math.floor(nMachines - nDrainedSlots / float(nCores)))
                if nDrainedSlots is not 0:
                    self.logger.debug(str(machineType) + ": running: " + str(nMachines) + ", drained slots: "
                                      + str(nDrainedSlots) + " -> recalculated running machines count: "
                                      + str(runningMachinesCount[machineType]))
            return runningMachinesCount

    def onEvent(self, evt):
        """
        Event handler: handles disintegrated machines.

        :param evt:
        :return:
        """
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            if self.mr.machines[evt.id].get(self.mr.regSite) == self.getSiteName():
                if evt.newStatus == self.mr.statusDisintegrated:
                    # cancel VM batch job in Freiburg
                    self.cancelFreiburgMachines([self.mr.machines[evt.id].get(self.regMachineJobId)])
                    self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def manage(self):
        """
        Manages all status changes of machines by checking running and completed jobs in Freiburg and machines listed
        in HTCondor (todo: use HTCondorIntegrationAdapter for this part).

        :return:
        """
        # get list of running machines via condor status
        condorServer = self.getConfig(self.configCondorServer)
        condorUser = self.getConfig(self.configCondorUser)
        condorKey = self.getConfig(self.configCondorKey)
        condorSsh = ScaleTools.Ssh(condorServer, condorUser, condorKey)
        condorResult = condorSsh.executeRemoteCommand(
            "condor_status -constraint 'CLOUD_SITE == \"BWFORCLUSTER\"' -autoformat: Machine State Activity")
        ScaleTools.sshDebugOutput(self.logger, "EKP-manage", condorResult)

        # condor info is invalid if there was a connection problem
        validCondorInfo = True
        if not condorResult[0] == 0:
            self.logger.warning("SSH connection to HTCondor collector could not be established.")
            validCondorInfo = False

        # prepare list of condor machines
        tmpCondorMachines = re.findall(r"([0-9]+).* ([a-zA-Z]+) ([a-zA-Z]+)$", condorResult[1], re.MULTILINE)

        # transform list into dictionary with one list per slot {job_id : [[state, activity], [state, activity], ..]}
        condorMachines = defaultdict(list)
        if len(tmpCondorMachines) > 1 and any(tmpCondorMachines[0]):
            for jobId, state, activity in tmpCondorMachines:
                condorMachines[jobId].append([state, activity])
        self.logger.debug("List of condor machines:\n" + str(condorMachines))

        # Freiburg login credentials
        frUser = self.getConfig(self.configFreiburgUser)

        # get list of completed jobs in Freiburg to see which machines failed to boot or died (job return code != 0)
        frResult = self.execCmdInFreiburg("showq -c -w user=" + frUser)
        if frResult[0] == 0:
            # returns a dict: {batch job id: return code/status, ..}
            frJobsCompleted = dict(
                re.findall(r"^([0-9]+)[ \t]+[A-Z]+[ \t]+([-A-Z0-9\(\)]+)", frResult[1], re.MULTILINE))
        elif frResult[0] == 255:
            frJobsCompleted = dict()
            self.logger.warning("SSH connection to Freiburg (showq -c) could not be established.")
        else:
            frJobsCompleted = dict()
            self.logger.warning(
                "Problem running remote command in Freiburg (showq -c) (return code " + str(frResult[0]) + "):\n"
                + str(frResult[2]))

        # get list of running jobs in Freiburg to see which machines booted up
        frResult = self.execCmdInFreiburg("showq -r -w user=" + frUser)
        if frResult[0] == 0:
            # returns a tuple containing ids of all running batch jobs in Freiburg
            frJobsRunning = re.findall(r"^([0-9]+)[ \t]+R", frResult[1], re.MULTILINE)
        elif frResult[0] == 255:
            frJobsRunning = []
            self.logger.warning("SSH connection to Freiburg (showq -r) could not be established.")
        else:
            frJobsRunning = []
            self.logger.warning(
                "Problem running remote command in Freiburg (showq -r) (return code " + str(frResult[0]) + "):\n"
                + str(frResult[2]))

        # get list of machines from machine registry
        mr = self.getSiteMachines()

        # loop over machines in machine registry
        # if everything is fine with a machine it gets removed from the condor_machines list!
        # all machines left in the condor list will be added to the machine registry later
        for mid in mr:
            batchJobId = mr[mid][self.regMachineJobId]
            # find machines that failed to boot, died, got canceled (job return code != 0) and set their status to down
            # this way ROCED is aware of failed VM requests and will ask for new ones that will hopefully boot up
            # sometimes a machine failes to boot but return code is 0. those machines get set to shutdown quietly since
            # this could also be a regular shutdown
            if mr[mid][self.mr.regStatus] is not self.mr.statusShutdown:
                if str(batchJobId) in frJobsCompleted:
                    if mr[mid][self.mr.regStatus] == self.mr.statusBooting:
                        self.logger.info("VM (" + str(batchJobId) + ") failed to boot!")
                    else:
                        if frJobsCompleted[str(batchJobId)] is not "0":
                            self.logger.info("VM (" + str(batchJobId) + ") died!")
                        else:
                            self.logger.debug("VM (" + str(batchJobId) + ") died with status 0!")
                    self.mr.updateMachineStatus(mid, self.mr.statusShutdown)

            # change machine status to up when status of batch job is running
            if mr[mid][self.mr.regStatus] == self.mr.statusBooting:
                if batchJobId in frJobsRunning:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)

            # all following inspections only make sense when the condor_status command was successful.
            if validCondorInfo:
                if mr[mid][self.mr.regStatus] == self.mr.statusPendingDisintegration:
                    # remove machine from registry if it was draining and is now gone
                    if batchJobId not in condorMachines:
                        self.mr.updateMachineStatus(mid, self.mr.statusShutdown)
                    else:
                        # update slot status and remove it from condor list for now
                        self.mr.machines[mid][self.regMachineCondorSlotStatus] = condorMachines[batchJobId]
                        del condorMachines[batchJobId]

                if mr[mid][self.mr.regStatus] == self.mr.statusWorking:
                    # remove machine from registry if it is not listed in valid condor list but status is working
                    if batchJobId not in condorMachines:
                        self.mr.updateMachineStatus(mid, self.mr.statusShutdown)
                    # machine is in both lists, so update slot status, set it to drain status if needed
                    # and remove it from condor list for now
                    else:
                        self.mr.machines[mid][self.regMachineCondorSlotStatus] = condorMachines[batchJobId]
                        # set machine status to draining if either slot activity or state indicate draining
                        # even tough there might be jobs running the machine will not enter in the decision of
                        # the broker resulting in a new machine getting requested -> it's better to have too many
                        # machines than too few
                        for slot in self.mr.machines[mid][self.regMachineCondorSlotStatus]:
                            if slot[0] == "Drained" or slot[1] == "Retiring":
                                self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
                                break  # one match is enough
                        # remove it from condor list for now
                        del condorMachines[batchJobId]

                if mr[mid][self.mr.regStatus] == self.mr.statusUp:
                    # change status from up to working if machine shows up in condor list
                    if batchJobId in condorMachines:
                        self.mr.updateMachineStatus(mid, self.mr.statusWorking)
                        # update slot status
                        self.mr.machines[mid][self.regMachineCondorSlotStatus] = condorMachines[batchJobId]
                        # remove it from condor list for now
                        del condorMachines[batchJobId]
                    # assume machine has connection problems when it stays in status up too long
                    elif (mr[mid][self.mr.regStatusLastUpdate] +
                              datetime.timedelta(minutes=6)) < datetime.datetime.now():
                        self.logger.warning("Batch job in Freiburg is running but machine not listed in HTCondor..")
                        self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

                if mr[mid][self.mr.regStatus] == self.mr.statusShutdown:
                    # if machine has status "shutdown" and is still listed in condor, remove it form condor list for now
                    # and wait until it is also gone in condor
                    if batchJobId in condorMachines:
                        del condorMachines[batchJobId]
                    # finally remove machine from machine registry when it is also gone in condor
                    else:
                        self.mr.removeMachine(mid)

        # add working condor machines + information to registry if they were not listed there before
        for batchJobId in condorMachines:
            mid = self.mr.newMachine()
            self.mr.machines[mid][self.mr.regSite] = self.getSiteName()
            self.mr.machines[mid][self.mr.regSiteType] = self.getSiteType()
            # todo: handle different machine types
            self.mr.machines[mid][self.mr.regMachineType] = "fr-default"
            self.mr.machines[mid][self.regMachineJobId] = batchJobId
            self.mr.machines[mid][self.regMachineCondorSlotStatus] = condorMachines[batchJobId]
            self.mr.updateMachineStatus(mid, self.mr.statusWorking)

        self.logger.info("Machines using resources in Freiburg: " + str(self.getCloudOccupyingMachinesCount()))
        self.logger.debug("Content of machine registry:\n" + str(self.getSiteMachines()))
        jsonLog = JsonLog()
        jsonLog.addItem("condor_nodes", len(self.getSiteMachines(status=self.mr.statusWorking)))
        jsonLog.addItem("condor_nodes_draining", len(self.getSiteMachines(status=self.mr.statusPendingDisintegration)))
        jsonLog.addItem("machines_requested", len(self.getSiteMachines(status=self.mr.statusBooting))
                        + len(self.getSiteMachines(status=self.mr.statusUp)))

    def execCmdInFreiburg(self, cmd):
        """
        Execute command on Freiburg login node via SSH.

        :param cmd:
        :return: (return code, stdout, stderr)
        """
        frServer = self.getConfig(self.configFreiburgServer)
        frUser = self.getConfig(self.configFreiburgUser)
        frKey = self.getConfig(self.configFreiburgKey)
        frSsh = ScaleTools.Ssh(frServer, frUser, frKey)
        return frSsh.executeRemoteCommand(cmd)

    def cancelFreiburgMachines(self, batchJobIds):
        """
        Cancel batch job (VM) in Freiburg

        It is also possible to use just one single command with multiple ids, but no machine gets cancelled if
        a single id is invalid! This can happen when the VM fails to boot due to network problems.

        :param batchJobIds:
        :return: [idsRemoved], [idsInvalidated]
        """
        command = ""
        if not isinstance(batchJobIds, (list, tuple)):
            batchJobIds = [batchJobIds]
        for batchJobId in batchJobIds:
            command += "mjobctl -c " + batchJobId + "; "
        result = self.execCmdInFreiburg(command)

        # catch 0:"successful" and 1:"invalid job id" return codes
        # the return code of the first cancellation command is returned here, we can handle them both to remove
        # cancelled and invalid machines
        idsRemoved = []
        idsInvalidated = []
        if result[0] <= 1:
            ScaleTools.sshDebugOutput(self.logger, "FR-terminate", result)
            idsRemoved += re.findall(r"\'([0-9]+)\'", result[1])
            idsInvalidated += re.findall(r"invalid job specified \(([0-9]+)", result[2])
            if len(idsRemoved) > 0:
                self.logger.info("Terminated machines (" + str(len(idsRemoved)) + "): " + ", ".join(idsRemoved))
            if len(idsInvalidated) > 0:
                self.logger.warning(
                    "Removed invalid machines (" + str(len(idsInvalidated)) + "): " + ", ".join(idsInvalidated))
            if (len(idsRemoved) + len(idsInvalidated)) == 0:
                self.logger.warning(
                    "A problem occurred while canceling VMs in Freiburg (return code " + str(result[0]) + "):\n"
                    + str(result[2]))
        else:
            self.logger.warning(
                "A problem occurred while canceling VMs in Freiburg (return code " + str(result[0]) + "):\n"
                + str(result[2]))
        return idsRemoved, idsInvalidated
