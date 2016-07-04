# ===============================================================================
#
# Copyright (c) 2010, 2011 by Thomas Hauth and Stephan Riedel
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

import logging
import math
import os

from Core import Config
from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import ScaleTools


class GridEngineRequirementAdapter(RequirementAdapterBase):
    ConfigQueueName = "queuename"

    def __init__(self):
        super(GridEngineRequirementAdapter, self).__init__()

        self.addCompulsoryConfigKeys(self.ConfigQueueName, Config.ConfigTypeString)

        # deprecated
        # self.geIp = None
        # self.geHostName = None
        # self.torqKey = None
        # self.geQName = "cloud.q"  # que name

        self.qsizeOffset = 2  # number of lines from qstat list header
        self.qsizeDivider = 1  # scaling factor for machine/job ratio

        self.addCompulsoryConfigKeys(self.ConfigQueueName, Config.ConfigTypeString)

        self.curReq = None

        self.shell_env = os.environ.copy()  # read current shell vars and add SGE vars
        self.shell_env["SGE_ROOT"] = "/opt/sge6.2u5"
        self.shell_env["PATH"] += ":/opt/sge6.2u5/bin/lx24-amd64"

    def init(self):
        self.exportMethod(self.setCurrentRequirement, "GridEngine_setCurrentRequirement")

    def countQ(self, cmd):
        """subprocess routine"""

        (res, count, stderr) = ScaleTools.Shell.executeCommand(cmd, self.shell_env)

        return res, int(count)

    @property
    def requirement(self):
        """get the number of jobs currently queued/running in grid engine"""
        """
            for this method the ge bin files have to be locally available and a ssh tunnel to the
            qmaster server has to be established first:
            ssh -f -L 6444:localhost:6444 -L 6445:localhost:6445 root@<sge_master_host> -N
            shell variables have to be set via self.shell_env
            return value is the number of jobs in the list - independent of running status
            broker decides whether to start or shut down machines depending on already running machines
            devider can be used to scale machine number to job number --> e.g. four jobs, one machine

            TODO:
                - better use ssh connection than port forwarding --> binaries don't have to be installed locally
        """

        """
            this one was a bit tricky since qsub -q cloud.q does not allow any submissions as long as no
            hosts are on the hostlist. since the initial state will have no cloud hosts i added localhost
            as a host. this is a bit dirty but solved my problem for now.
        """

        (res, count) = self.countQ("qstat -q %s -u \"*\" | wc -l"
                                   % self.getConfig(self.ConfigQueueName))

        if res == 0:  # if shell command was successful
            if count == 0:
                self._curRequirement = 0
            else:
                self._curRequirement = count - self.qsizeOffset

            self._curRequirement = int(math.ceil(float(self._curRequirement) /
                                                 float(self.qsizeDivider)))

            logging.info("grid engine needs %d nodes. qsizeoffset is %d." % (self._curRequirement, self.qsizeOffset))
        else:
            self._curRequirement = 0

        return self._curRequirement

    def getNeededMachineType(self):
        return "one-default"

    @property
    def description(self):
        return "GridEngineRequirementAdapter"
