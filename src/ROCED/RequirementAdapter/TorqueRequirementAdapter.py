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


import logging
import subprocess

from RequirementAdapter.Requirement import RequirementAdapterBase
from Util import ScaleTools


class TorqueRequirementAdapter(RequirementAdapterBase):
    def __init__(self):
        super(TorqueRequirementAdapter, self).__init__()

        self.torqIp = None
        self.torqHostName = None
        self.torqKey = None
        self.torqQName = "batch"
        # this number is subtracted from the actual q size to be able
        # to only start machines at a certain size
        self.qsizeOffset = 0
        self.qsizeDivider = 1

        self.curReq = None

    def init(self):
        self.exportMethod(self.setCurrentRequirement, "Torq_setCurrentRequirement")

    # qstat | egrep "Q batch|R batch" | wc -l
    def countLocalQ(self, cmd):
        p1 = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        return 0, p1.communicate()[0]

    @property
    def requirement(self):
        cmd = "qstat | egrep \"Q %s|R %s\" | wc -l" % (self.torqQName, self.torqQName)

        if self.torqKey is None:
            (res1, count1) = self.countLocalQ(cmd)
        else:
            ssh = ScaleTools.Ssh(self.torqIp, "root", self.torqKey, None, 1)
            (res1, count1) = ssh.handleSshCall(cmd)

        # dangerous, if not all instances are run by us...

        if res1 == 0:
            self._curRequirement = int(count1) - self.qsizeOffset

            if self._curRequirement < 0:
                self._curRequirement = 0

            # apply divider
            self._curRequirement //= self.qsizeDivider

            logging.info("torq needs " + str(self._curRequirement) +
                         " nodes. qsizeoffset is " + str(self.qsizeOffset))
            return self._curRequirement
        else:
            return None

    def getNeededMachineType(self):
        return "euca-default"

    @property
    def description(self):
        return "TorqueRequirementAdapter"
