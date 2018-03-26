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
from xml.dom import minidom
import datetime
import time
from collections import Counter

from Core import MachineRegistry, Config
from SiteAdapter.Site import SiteAdapterBase
from Util.Logging import JsonLog
from Util.PythonTools import Caching, merge_dicts
from Util.ScaleTools import Ssh


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
    configVMNamePrefix = "vm_prefix"
    configSSHPrefix = "ssh_prefix"

    regMachineJobId = "batch_job_id"
    __vmStartScript = "startVM.py"
    vanishedVMs = Counter()
    """Python script to be executed in Freiburg. This starts the VM with the corresponding image.
    This python script has to be adapted on the server with user name, OpenStack Dashboard PW,
    image GUID, etc.
    """

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
        self.addCompulsoryConfigKeys(self.configVMNamePrefix, Config.ConfigTypeString,
                                     "prefix for VMs' hostname")
        self.addOptionalConfigKeys(self.configSSHPrefix, Config.ConfigTypeString,
                                     description="prefix for ssh commands", default="")


        self.__default_machine = "vm-default"

    def init(self):
        self.mr.registerListener(self)
        self.logger = logging.getLogger(self.getConfig(self.configSiteLogger))
        super(FreiburgSiteAdapter, self).init()
        self.__readVMNamePrefix()
        self.reg_site_server_node_name = "reg_site_server_node_name"




        # TODO: This information is lost, when loading the previous machine registry.
        ###
        # Try to add "booting" machines (submitted batch jobs) to machine registry.
        ###
        # Getting List of running, completed and idle Machines from the MOAB XML output:
        try:
            frJobs = self.__getJobs
            if frJobs is None:
                raise ValueError
        except ValueError:
            frJobs = [{}, {}, {}, {}]

        idleJobs=merge_dicts(frJobs[0],frJobs[1])
        blockedJobs=frJobs[1]
        runningJobs=frJobs[2]
        completedJobs=frJobs[3]

        # Machines that are found running get this type by default
        self.__default_machine = list(self.getConfig(self.ConfigMachines).keys())[0]

        for mid, machine_ in self.getSiteMachines(status=self.mr.statusBooting).items():
            try:
                idleJobs.remove(machine_[self.regMachineJobId])
            except ValueError:
                if machine_[self.regMachineJobId] in runningJobs:
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                elif machine_[self.regMachineJobId] in completedJobs:
                    self.logger.debug("MOAB job %s is completet: Machine %s is DOWN" % (mr[mid][self.regMachineJobId], mid) )
                    self.mr.updateMachineStatus(mid, self.mr.statusDown)
                else:
                    self.logger.debug("Couldn't assign machine %s." % machine_[self.regMachineJobId])
        if idleJobs is not None:
            for jobId in idleJobs:
                mid = self.mr.newMachine()
                self.mr.machines[mid][self.mr.regSite] = self.siteName
                self.mr.machines[mid][self.mr.regSiteType] = self.siteType
                self.mr.machines[mid][self.mr.regMachineType] = self.__default_machine
                self.mr.machines[mid][self.regMachineJobId] = jobId
                self.mr.machines[mid][self.reg_site_server_node_name] = self.__getVMName(jobId)
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
            result = self.__execCmdInFreiburg("msub -j oe -m p -l walltime=%s,mem=%s,nodes=1:ppn=%d %s" % (machineSettings["walltime"], machineSettings["memory"], machineSettings["cores"], self.__vmStartScript))

            # std_out = batch job id
            if result[0] == 0 and result[1].strip().isdigit():
                mid = self.mr.newMachine()
                self.mr.machines[mid][self.mr.regSite] = self.siteName
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.regMachineJobId] = result[1].strip()
                self.mr.machines[mid][self.reg_site_server_node_name] = self.__getVMName(
                    result[1].strip())
                self.mr.machines[mid][self.mr.regSiteType] = self.siteType
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
            else:
                self.logger.warning("A problem occurred while requesting VMs. Stopping for now."
                                    "RC: %d; stdout: %s; stderr: %s" % (result[0], result[1], result[2]))
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
            bootingMachines = sorted(bootingMachines.items(),
                                     key=lambda machine_: machine_[1][self.mr.regStatusLastUpdate],
                                     reverse=True)
        except KeyError:
            bootingMachines = []

        machinesToRemove = bootingMachines

        # needed amount of machines
        machinesToRemove = machinesToRemove[0:count]

        # list of batch job ids to terminate/drain
        idsToTerminate = []
        idsRemoved = []
        idsInvalidated = []

        for mid, machine in machinesToRemove:
            if machine[self.mr.regStatus] == self.mr.statusBooting:
                # booting machines can be terminated immediately
                idsToTerminate.append(machine[self.regMachineJobId])

        self.logger.debug("Machines to terminate (%d): %s" % (len(idsToTerminate), ", ".join(idsToTerminate)))
        if idsToTerminate:
            idsRemoved, idsInvalidated = self.__cancelFreiburgMachines(idsToTerminate)

        if len(idsRemoved + idsInvalidated) > 0:
            # update status
            [self.logger.debug("Machine %s was terminated an is now DOWN " % mid) for mid, machine
             in self.getSiteMachines().items()
             if machine[self.regMachineJobId] in idsRemoved + idsInvalidated]
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
                    try:
                        nDrainedSlots += self.mr.machines[mid][self.mr.regMachineDrain]*self.mr.machines[mid][self.mr.regMachineCores]
                    except:
                        self.logger.debug("can not count drained slots for machine %s" % mid)
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
            self.logger.debug("Status Change Event: %s (%s->%s)" % (evt.id, evt.oldStatus, evt.newStatus))
            if evt.newStatus == self.mr.statusDisintegrated:
                # Disintegrated information comes from integration adapter. Skipping state only happens with time out.
                try:
                    if (self.mr.machines[evt.id].get(self.regMachineJobId) in self.__runningJobs and
                                evt.oldStatus != self.mr.statusDisintegrating):
                        self.__cancelFreiburgMachines([self.mr.machines[evt.id].get(self.regMachineJobId)])
                except Exception as err:
                    self.logger.warning("Canceling machine failed with exception %s" % err)
                self.logger.debug("Machine %s goes direct from state disintegrated to state down" % evt.id)
                self.logger.debug("Event is %s" % repr(evt))
                if hasattr(evt, '__dict__'):
                    self.logger.debug("Event dict is %s" % repr(evt.__dict__))
                self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def manage(self):
        # type: () -> None
        """Manages status changes of machines by checking  jobs in Freiburg.

        Booting = Freiburg batch job for machine was submitted
        Up      = Freiburg batch job is running, VM is Booting,
                  IntegrationAdapter switches this to "integrating" and "working".
        Disintegrated & Down

        IntegrationAdapter is responsible for handling Integrating, Working,
        PendingDisintegration, Disintegrating
        """

        # Getting List of running, completed and idle Machines from the MOAB XML output:
        try:
            frJobs = self.__getJobs
            if frJobs is None:
                raise ValueError
        except ValueError:
            frJobs = [{}, {}, {}, {}]

        frJobsIdle=merge_dicts(frJobs[0],frJobs[1])
        frJobsBlocked=frJobs[1]
        frJobsRunning=frJobs[2]
        frJobsCompleted=frJobs[3]

        mr = self.getSiteMachines()
        for mid in mr:
            batchJobId = mr[mid][self.regMachineJobId]
            # Status handled by Integration Adapter
            if mr[mid][self.mr.regStatus] in [self.mr.statusIntegrating, self.mr.statusWorking,
                                              self.mr.statusPendingDisintegration,
                                              self.mr.statusDisintegrating]:
                try:
                    frJobsRunning.pop(batchJobId)
                    self.logger.debug('Removing batch-job %s from list of running Jobs' % mr[mid][self.regMachineJobId] )
                    continue
                except (KeyError, AttributeError, IndexError):
                    # AttributeError: frJobsRunning is Empty
                    # KeyError: batchJobId not in frJobsRunning
                    self.logger.debug('Matching between machine registry entry %s and batch-job ID (%s) failed during removal of machines with ignorable states.' % (mid, mr[mid][self.regMachineJobId]))
                    pass
            # Machines which failed to boot/died/got canceled (return code != 0) -> down
            # A machine MAY fail to boot with return code 0 or we just missed some states -> regular shutdown
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
                try:
                    frJobsRunning.pop(batchJobId)
                    self.logger.debug('Removing batch-job (%s) from list of running Jobs' % batchJobId)
                except (KeyError, AttributeError, IndexError):
                    self.logger.debug('Matching between machine registry entry %s and batch-job ID (%s) failed during removal of down machines with still alive MOAB job.' % (mid, mr[mid][self.regMachineJobId]))
                    pass

                if self.mr.calcLastStateChange(mid) > 5*60:
                    self.__cancelFreiburgMachines(batchJobId)
                continue

            if mr[mid][self.mr.regStatus] == self.mr.statusBooting:
                # batch job running: machine -> up
                if batchJobId in frJobsRunning:
                    del self.vanishedVMs[mid]
                    self.mr.updateMachineStatus(mid, self.mr.statusUp)
                    frJobsRunning.pop(batchJobId)
                # Machine disappeared. If the MOAB job is completed.
                elif batchJobId not in frJobsIdle and batchJobId not in frJobsCompleted:
                    self.logger.info('Corresponding MOAB-job (%s) for machine %s was not found (%s retry) ' % (mr[mid][self.regMachineJobId], mid, self.vanishedVMs[mid]))
                    self.vanishedVMs[mid] += 1
                    if self.vanishedVMs[mid] >= 5:
                        self.logger.debug("Corresponding Moab-job %s for machine %s was not found for 3 cycles" % (mr[mid][self.regMachineJobId], mid))
                        self.mr.updateMachineStatus(mid, self.mr.statusDown)
                        del self.vanishedVMs[mid]
                else:
                    del self.vanishedVMs[mid]


        # All remaining unaccounted batch jobs
        for batchJobId in frJobsRunning:
            mid = self.mr.newMachine()
            # TODO: try to identify machine type, using cores & wall-time
            self.mr.machines[mid][self.mr.regSite] = self.siteName
            self.mr.machines[mid][self.mr.regSiteType] = self.siteType
            self.mr.machines[mid][self.mr.regMachineType] = self.__default_machine
            self.mr.machines[mid][self.regMachineJobId] = batchJobId
            self.mr.machines[mid][self.reg_site_server_node_name] = self.__getVMName(batchJobId)
            self.mr.updateMachineStatus(mid, self.mr.statusUp)

        self.logger.info("Machines using resources (Freiburg): %d" % self.cloudOccupyingMachinesCount)

        with JsonLog() as jsonLog:
            jsonLog.addItem(self.siteName, "nodes",
                            len(self.getSiteMachines(status=self.mr.statusWorking)))
            jsonLog.addItem(self.siteName, "nodes_draining",
                            len([mid for mid in self.getSiteMachines(status=self.mr.statusPendingDisintegration)
                                 if self.mr.machines[mid][self.mr.regMachineBusy] is True]))
            jsonLog.addItem(self.siteName, "machines_requested",
                            len(self.getSiteMachines(status=self.mr.statusBooting)) +
                            len(self.getSiteMachines(status=self.mr.statusUp)) +
                            len(self.getSiteMachines(status=self.mr.statusIntegrating)))

    def __execCmdInFreiburg(self, cmd):
        """Execute command on Freiburg login node via SSH.

        :param cmd:
        :return: Tuple: (return_code, std_out, std_err)
        """
        frSsh = Ssh(host=self.getConfig(self.configFreiburgServer),
                    username=self.getConfig(self.configFreiburgUser),
                    key=self.getConfig(self.configFreiburgKey))
        cmd=self.getConfig(self.configSSHPrefix)+' "'+cmd+'"'
        self.logger.debug("ssh command send: %s" % cmd)
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



    def __readVMNamePrefix(self):
        """Read VM name prefix from config file communication with
        IntegrationAdapter."""

        self.__vmNamePrefix = self.getConfig(self.configVMNamePrefix)
        self.logger.debug("VM Prefix: %s" % self.__vmNamePrefix)
        return self.__vmNamePrefix


    def __getVMName(self, batchJobId):
        """Build VM name for communication with IntegrationAdapter.
        Machine registry value "reg_site_server_node_name" is used to communicate with
        IntegrationAdapter. This name must be built from the batch job id."""
        return self.__readVMNamePrefix() + str(batchJobId)


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
    @Caching(validityPeriod=-1, redundancyPeriod=300)
    def __getJobs(self):
        # replaces old __runningJobs, __idleJobs and __completedJobs
        # Getting List of running, completed and idle Machines from the MOAB XML output:
        cmd = "showq --xml %s && showq -c --xml %s" % ( self.__userString, self.__userString )
        frResult = self.__execCmdInFreiburg(cmd)
        if frResult[0] == 0:
            # returns a list containing all batch jobs for a specific user
            # see http://docs.adaptivecomputing.com/maui/commands/showq.php#activeexample
            frJobsIdle = {}
            frJobsBlocked = {}
            frJobsRunning = {}
            frJobsCompleted = {}
            xmlOutput = minidom.parseString(frResult[1].replace('\n','').replace("</Data><Data>", ""))

            xmlJobsList = xmlOutput.getElementsByTagName('queue')

            for queue in xmlJobsList:
                queueJobsList = queue.getElementsByTagName('job')
                for line in queueJobsList:
                        if queue.attributes['option'].value == 'eligible':
                            line.attributes['State'].value == 'Idle' and frJobsIdle.update({str(line.attributes['JobID'].value): {"State": str(line.attributes['State'].value)}})
                        if queue.attributes['option'].value == 'blocked':
                            line.attributes['State'].value == 'Idle' and frJobsBlocked.update({str(line.attributes['JobID'].value): {"State": str(line.attributes['State'].value)}})
                        if queue.attributes['option'].value == 'active':
                            line.attributes['State'].value == 'Running' and frJobsRunning.update({str(line.attributes['JobID'].value): {
                            "walltime": str(datetime.timedelta(seconds=int(line.attributes['StartTime'].value)+int(line.attributes['ReqAWDuration'].value)-int(time.time()))),
                            "cores": int(line.attributes['ReqProcs'].value)}})
                        if queue.attributes['option'].value == 'completed':
                            line.attributes['State'].value == 'Completed' and frJobsCompleted.update({str(line.attributes['JobID'].value): {str(line.attributes['CompletionCode'].value)}})
        elif frResult[0] == 255:
            frJobsIdle = {}
            frJobsBlocked = {}
            frJobsRunning = {}
            frJobsCompleted = {}
            self.logger.warning("SSH connection (showq -r) could not be established.")
            raise ValueError("SSH connection (showq -r) could not be established.")
        else:
            frJobsIdle = {}
            frJobsBlocked = {}
            frJobsRunning = {}
            frJobsCompleted = {}
            self.logger.warning("Problem running remote command (showq --xml) (RC %d):\n%s" % (frResult[0], frResult[2]))
            raise ValueError("Problem running remote command (showq --xml) (RC %d):\n%s" % (frResult[0], frResult[2]))
        frJobs=[frJobsIdle, frJobsBlocked, frJobsRunning, frJobsCompleted]
        self.logger.debug("Idle:\n%s" % frJobs[0])
        self.logger.debug("Blocked:\n%s" % frJobs[1])
        self.logger.debug("Running:\n%s" % frJobs[2])
        self.logger.debug("Completed:\n%s" % frJobs[3])
        return frJobs


    @property
    @Caching(validityPeriod=-1, redundancyPeriod=300)
    def __runningJobs(self):
        # type: () -> dict
        """Get list of running batch jobs, filtered by user ID."""
        cmd = "showq -r --xml %s" % self.__userString
        frResult = self.__execCmdInFreiburg(cmd)
        xmlOutput = minidom.parseString(frResult[1])
        xmlJobsList = xmlOutput.getElementsByTagName('queue')


        if frResult[0] == 0:
            # returns a list containing all running batch jobs
            # see http://docs.adaptivecomputing.com/maui/commands/showq.php#activeexample
            runningJobsList = minidom.parseString(frResult[1]).getElementsByTagName('job')

            frJobsRunning = {}
            for line in runningJobsList:
                frJobsRunning.update(
                    {str(line.attributes['JobID'].value): {
                        "walltime": str(datetime.timedelta(seconds=int(line.attributes['StartTime'].value)+int(line.attributes['ReqAWDuration'].value)-int(time.time()))),
                        "cores": int(line.attributes['ReqProcs'].value)}})
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
