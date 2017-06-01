# ===============================================================================
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
# ===============================================================================
from __future__ import unicode_literals, absolute_import

import getpass
import logging

from Core import Config
from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import Logging, ScaleTools
from Util.PythonTools import Caching


class HTCondorRequirementAdapter(RequirementAdapterBase):
    configMachines = "machines"
    configCondorUser = "condor_user"
    configCondorKey = "condor_key"
    configCondorServer = "condor_server"
    configCondorRequirement = "condor_requirement"
    configCondorConstraint = "condor_constraint"

    # See https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
    condorStatusIdle = 1
    condorStatusRunning = 2

    # class constants for condor_q query:
    _query_constraints = "RoutedToJobId =?= undefined && ( JobStatus == %d || JobStatus == %d )" % \
                         (condorStatusIdle, condorStatusRunning)
    # auto-format string: raw output, separated by comma
    _query_format_string = "-autoformat:r, JobStatus RequestCpus Requirements"

    _CLI_error_strings = frozenset(("Failed to fetch ads from", "Failed to end classad message"))

    def __init__(self):
        """Requirement adapter, connecting to an HTCondor batch system."""
        super(HTCondorRequirementAdapter, self).__init__()

        self.setConfig(self.configMachines, dict())
        self.addCompulsoryConfigKeys(self.configMachines, Config.ConfigTypeDictionary)
        self.addOptionalConfigKeys(key=self.configCondorUser, datatype=Config.ConfigTypeString,
                                   description="Login name for condor collector server.",
                                   default=getpass.getuser())
        self.addOptionalConfigKeys(key=self.configCondorServer, datatype=Config.ConfigTypeString,
                                   description="Hostname of collector server. If machines are connected to connector "
                                               "and have commandline interface installed, localhost can easily be used "
                                               "because we query with \"global\".",
                                   default="localhost")
        self.addOptionalConfigKeys(key=self.configCondorKey, datatype=Config.ConfigTypeString,
                                   description="Path to SSH key for remote login (not necessary with localhost).",
                                   default="~/")
        self.addOptionalConfigKeys(key=self.configCondorRequirement, datatype=Config.ConfigTypeString,
                                   description="Grep filter string on ClassAd Requirement expression",
                                   default="")
        self.addOptionalConfigKeys(key=self.configCondorConstraint, datatype=Config.ConfigTypeString,
                                   description="ClassAd constraint in condor_q expression",
                                   default="True")

        self.logger = logging.getLogger("HTCondorReq")
        self.__str__ = self.description

    def init(self):
        super(HTCondorRequirementAdapter, self).init()

    @property
    def description(self):
        return "HTCondorRequirementAdapter"

    @property
    @Caching(validityPeriod=-1, redundancyPeriod=900)
    def requirement(self):
        ssh = ScaleTools.Ssh(host=self.getConfig(self.configCondorServer),
                             username=self.getConfig(self.configCondorUser),
                             key=self.getConfig(self.configCondorKey))

        # Target.Requirements can't be filtered with -constraints since it would require ClassAd based regex matching.
        # TODO: Find a more generic way to match resources/requirements (condor_q -slotads ??)
        # cmd_idle = "condor_q -constraint 'JobStatus == 1' -slotads slotads_bwforcluster " \
        #            "-analyze:summary,reverse | tail -n1 | awk -F ' ' " \
        #            "'{print $3 "\n" $4}'| sort -n | head -n1"
        constraint = "( %s ) && ( %s )" % (self._query_constraints, self.getConfig(self.configCondorConstraint))

        cmd = ("condor_q -global -allusers -nobatch -constraint '%s' %s" % (constraint, self._query_format_string))
        result = ssh.handleSshCall(call=cmd, quiet=True)
        if result[0] != 0:
            self.logger.warning("Could not get HTCondor queue status! %d: %s" % (result[0], result[2]))
            return None
        elif any(error_string in result[1] for error_string in self._CLI_error_strings):
            self.logger.warning("condor_q request timed out.")
            return None

        queue_line = (entry.split(",", 3) for entry in str(result[1]).splitlines())
        converted_line = ((int(status), int(cores), requirement) for status, cores, requirement in queue_line)
        if self.getConfig(self.configCondorRequirement):
            # TODO: We could use ClassAd bindings, to check requirement(s)
            filtered_line = ((status, cores) for status, cores, requirement in converted_line
                             if self.getConfig(self.configCondorRequirement) in requirement)
        else:
            filtered_line = ((status, cores) for status, cores, requirement in converted_line)

        required_cpus_total = 0
        required_cpus_idle_jobs = 0
        required_cpus_running_jobs = 0
        try:
            for job_status, requested_cpus in filtered_line:
                required_cpus_total += requested_cpus
                if job_status == self.condorStatusIdle:
                    required_cpus_idle_jobs += requested_cpus
                elif job_status == self.condorStatusRunning:
                    required_cpus_running_jobs += requested_cpus
        except ValueError:
            # This error should only occur, if the result was empty AND CondorRequirement is initial
            required_cpus_total = 0
            required_cpus_idle_jobs = 0
            required_cpus_running_jobs = 0

        self.logger.debug("HTCondor queue: Idle: %d; Running: %d." %
                          (required_cpus_idle_jobs, required_cpus_running_jobs))

        # cores->machines: machine definition required for RequirementAdapter
        n_cores = - int(self.getConfig(self.configMachines)[self.getNeededMachineType()]["cores"])
        self._curRequirement = - (required_cpus_total // n_cores)

        with Logging.JsonLog() as json_log:
            json_log.addItem(self.getNeededMachineType(), "jobs_idle", required_cpus_idle_jobs)
            json_log.addItem(self.getNeededMachineType(), "jobs_running", required_cpus_running_jobs)

        return self._curRequirement

    def getNeededMachineType(self):
        # TODO: Handle multiple machine types!
        machineType = list(self.getConfig(self.configMachines).keys())[0]
        if machineType:
            return machineType
        else:
            self.logger.error("No machine type defined for requirement.")
