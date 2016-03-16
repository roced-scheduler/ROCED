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

from Core import MachineRegistry
from IntegrationAdapter import Integration
import IntegrationAdapter.TorqueIntegrationAdapter
from Util import ScaleTools
from Core import ScaleTest


class FakeSsh(object):
    # static
    ranCommands = []
    predefCommands = dict()

    def __init__(self, host, username, key, password=None, timeout=3):
        pass

    @staticmethod
    def clear():
        FakeSsh.predefCommands = dict()
        FakeSsh.ranfCommands = []

    @staticmethod
    def getSshOnMachine(machine):
        return FakeSsh()

    def canConnect(self):
        return True

    def handleSshCall(self, call):
        return self.executeRemoteCommand(call)

    @staticmethod
    def executeRemoteCommand(command):
        logging.debug("ssh running %s" % command)
        FakeSsh.ranCommands.append(command)

        if command in FakeSsh.predefCommands:
            return FakeSsh.predefCommands[command]
        else:
            return (0, "")


class TorqueIntegrationAdapterTest(ScaleTest.ScaleTestBase):
    def setUp(self):
        self.mr = MachineRegistry.MachineRegistry()
        self.mr.clear()
        FakeSsh.clear()

    def test_disintegrate(self):
        int = IntegrationAdapter.TorqueIntegrationAdapter.TorqueIntegrationAdapter()

        mid = self.mr.newMachine()
        self.mr.machines[mid][self.mr.regSite] = "cloud-site"
        self.mr.updateMachineStatus(mid, self.mr.statusWorking)
        self.mr.machines[mid][int.reg_torque_node_name] = "cloud-001"

        mid_notorque = self.mr.newMachine()
        self.mr.machines[mid_notorque][self.mr.regSite] = "cloud_site"
        self.mr.updateMachineStatus(mid_notorque, self.mr.statusWorking)

        # todo: fix and re-enable
        ScaleTools.Ssh = FakeSsh
        # FakeSsh.predefCommands[ "pbsnodes -x cloud-001"] = (0, "<Data><Node><name>cloud-001</name><state>offline, job-exclusive</state><np>1</np><ntype>cluster</ntype><status>opsys=linux,uname=Linux localhost.localdomain 2.6.31-14-server #48-Ubuntu SMP Fri Oct 16 15:07:34 UTC 2009 x86_64,sessions=? 15201,nsessions=? 15201,nusers=0,idletime=4945,totmem=2056456kb,availmem=1950652kb,physmem=2056456kb,ncpus=1,loadave=0.02,netload=2353631,state=free,jobs=,varattr=,rectime=1270214759</status></Node><Node><name>ekp-cloud-pbs</name><state>down</state><np>1</np><ntype>cluster</ntype></Node></Data>" )

        # registers the event listener on MachineRegistry
        # int.init()
        # self.mr.updateMachineStatus(mid, self.mr.StatusPendingDisintegration)

        # self.assertTrue( "pbsnodes -o cloud-001" in FakeSsh.ranCommands )
        # self.assertEqual( self.mr.machines[mid][self.mr.reg_status], self.mr.StatusDisintegrating )

        # immediate shutdown
        # self.mr.updateMachineStatus(mid_notorque, self.mr.StatusPendingDisintegration)
        # self.assertEqual( self.mr.machines[mid_notorque][self.mr.reg_status], self.mr.StatusDisintegrated )

        # no shutdown allowed yet
        # int.manage()
        # self.assertFalse( "python torqconf.py del_node cloud-001" in FakeSsh.ranCommands )
        # self.assertEqual( self.mr.machines[mid][self.mr.reg_status], self.mr.StatusDisintegrating )

        # FakeSsh.predefCommands[ "pbsnodes -x cloud-001"] = (0, "<Data><Node><name>cloud-001</name><state>offline</state><np>1</np><ntype>cluster</ntype><status>opsys=linux,uname=Linux localhost.localdomain 2.6.31-14-server #48-Ubuntu SMP Fri Oct 16 15:07:34 UTC 2009 x86_64,sessions=? 15201,nsessions=? 15201,nusers=0,idletime=4945,totmem=2056456kb,availmem=1950652kb,physmem=2056456kb,ncpus=1,loadave=0.02,netload=2353631,state=free,jobs=,varattr=,rectime=1270214759</status></Node><Node><name>ekp-cloud-pbs</name><state>down</state><np>1</np><ntype>cluster</ntype></Node></Data>" )

        # shutdownyet
        # int.manage()
        # self.assertTrue( "python torqconf.py del_node cloud-001" in FakeSsh.ranCommands )
        # self.assertEqual( self.mr.machines[mid][self.mr.reg_status], self.mr.StatusDisintegrated )
