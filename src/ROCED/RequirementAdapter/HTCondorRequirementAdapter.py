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
import re

from Core import Config
from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import Logging, ScaleTools


class HTCondorRequirementAdapter(RequirementAdapterBase):
    configMachines = "machines"
    configCondorUser = "condor_user"
    configCondorKey = "condor_key"
    configCondorServer = "condor_server"
    configCondorRequirement = "condor_requirement"

    # class constants for condor_q query: condor autoformat string & gawk processing string
    _query_format_string = "'%s,' JobStatus -format '%s,' RequestCpus -format '%s\\n' Requirements"
    _query_processing = "',' '{print $1\",\"$2}'"

    def __init__(self):
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
                                   description="Path to SSH key for remote login. Not necessary with server localhost.",
                                   default="~/")
        self.addCompulsoryConfigKeys(self.configCondorRequirement, Config.ConfigTypeString)

        self.logger = logging.getLogger('HTCondorReq')
        self.__str__ = self.description

    def init(self):
        super(HTCondorRequirementAdapter, self).init()

    @property
    def description(self):
        return "HTCondorRequirementAdapter"

    @property
    @ScaleTools.Caching(validityPeriod=-1, redundancyPeriod=900)
    def requirement(self):
        ssh = ScaleTools.Ssh(host=self.getConfig(self.configCondorServer),
                             username=self.getConfig(self.configCondorUser),
                             key=self.getConfig(self.configCondorKey))

        # get running and idling jobs and the number of requested CPUs
        # job status ids: https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
        # this is not done with -constraints since "Requirements" can not be used for
        # selecting specific jobs.
        # grep is the solution here

        # TODO: htcondor python bindings? || condor_q -global -constraint "REMOTE_JOB==True"

        cmd = ("condor_q -global -constraint 'JobStatus == 1 || JobStatus == 2' -format %s | grep -i '%s' | awk -F%s" %
               (self._query_format_string, self.getConfig(self.configCondorRequirement), self._query_processing))

        result = ssh.handleSshCall(call=cmd, quiet=True)

        # get number of idle jobs with requirements that allow them to run on
        # a specific site (using -slotads)
        # cmd_idle = "condor_q -constraint 'JobStatus == 1' -slotads slotads_bwforcluster " \
        #            "-analyze:summary,reverse | tail -n1 | awk -F ' ' " \
        #            "'{print $3 "\n" $4}'| sort -n | head -n1"

        if result[0] == 0:
            condor_jobs = str(result[1]).strip()
            condor_jobs = re.split(",|\n", condor_jobs)
            condor_jobs = [condor_jobs[i:i + 2] for i in range(0, len(condor_jobs), 2)]
            n_slots = 0
            n_jobs_idle = 0
            n_jobs_running = 0
            if any(condor_jobs[0]):
                for job in condor_jobs:
                    n_slots += int(job[1])
                    if int(job[0]) == 1:  # 1: idle
                        n_jobs_idle += 1
                    elif int(job[0]) == 2:  # 2: running
                        n_jobs_running += 1

            self.logger.debug("HTCondor queue (%d+%d). [Status, CPUs]:\n%s"
                              % (n_jobs_idle, n_jobs_running, condor_jobs))

            # this requires the machines variable to be listed twice in the config file
            n_cores = - int(self.getConfig(self.configMachines)[self.getNeededMachineType()]["cores"])

            # calculate the number of machines needed
            self._curRequirement = - (n_slots // n_cores)

            with Logging.JsonLog() as json_log:
                json_log.addItem(self.getNeededMachineType(), 'jobs_idle', n_jobs_idle)
                json_log.addItem(self.getNeededMachineType(), 'jobs_running', n_jobs_running)

            return self._curRequirement
        else:
            self.logger.warning("Could not get HTCondor queue status! %d: %s" % (result[0], result[2]))
            return None

    def getNeededMachineType(self):
        # TODO: Handle multiple machine types!
        machineType = list(self.getConfig(self.configMachines).keys())[0]
        if machineType:
            return machineType
        else:
            self.logger.error("No machine type defined for requirement.")
