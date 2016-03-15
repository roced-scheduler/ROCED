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
import math
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

        self.qsizeOffset = 0
        self.qsizeDivider = 1

        self.curReq = None

    # this number is substracted from the actual q size to be able
    # to only start machines at a certain size
    def qsizeOffset():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._qsizeOffset

        def fset(self, value):
            self._qsizeOffset = value

        def fdel(self):
            del self._qsizeOffset

        return locals()

    qsizeOffset = property(**qsizeOffset())

    def qsizeDivider():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._qsizeDivider

        def fset(self, value):
            self._qsizeDivider = value

        def fdel(self):
            del self._qsizeDivider

        return locals()

    qsizeDivider = property(**qsizeDivider())

    def torqHostName():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._torqHostName

        def fset(self, value):
            self._torqHostName = value

        def fdel(self):
            del self._torqHostName

        return locals()

    torqHostName = property(**torqHostName())

    def torqIp():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._torqIp

        def fset(self, value):
            self._torqIp = value

        def fdel(self):
            del self._torqIp

        return locals()

    torqIp = property(**torqIp())

    def torqKey():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._torqKey

        def fset(self, value):
            self._torqKey = value

        def fdel(self):
            del self._torqKey

        return locals()

    torqKey = property(**torqKey())

    def torqQName():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._torqQName

        def fset(self, value):
            self._torqQName = value

        def fdel(self):
            del self._torqQName

        return locals()

    torqQName = property(**torqQName())

    def init(self):
        self.exportMethod(self.setCurrentRequirement, "Torq_setCurrentRequirement")

    # qstat | egrep "Q batch|R batch" | wc -l
    def countLocalQ(self, cmd):
        p1 = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        return (0, p1.communicate()[0])

    def getCurrentRequirement(self):
        if not self.curReq == None:
            return self.curReq

        cmd = "qstat | egrep \"Q %s|R %s\" | wc -l" % (self.torqQName, self.torqQName)

        if self.torqKey == None:
            (res1, count1) = self.countLocalQ(cmd)
        else:
            ssh = ScaleTools.Ssh(self.torqIp, "root", self.torqKey, None, 1)
            (res1, count1) = ssh.handleSshCall(cmd)

        # dangerous, if not all instances are run by us...

        if res1 == 0:
            overall = int(count1) - self.qsizeOffset

            if overall < 0:
                overall = 0

            # apply divider
            overall = int(math.floor(float(overall) / float(self.qsizeDivider)))

            logging.info("torq needs " + str(overall) + " nodes. qsizeoffset is " + str(self.qsizeOffset))
            return overall
        else:
            return None  # todo: None means: no information available

    def setCurrentRequirement(self, c):
        if (c < 0):
            self.curReq = None
        else:
            self.curReq = c
        # to avoid the None problem with XML RPC
        return 23

    def getNeededMachineType(self):
        return "euca-default"

    def getDescription(self):
        return "TorqueRequirementAdapter"
