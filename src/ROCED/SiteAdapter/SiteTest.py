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

from Core import ScaleTest


# import EucaUtil
# from SiteAdapter.Ec2SiteAdapter import EucaSiteAdapter

# TODO: disabled until properly fixed
class EucaSiteAdapterTest:  # (ScaleTest.ScaleTestBase):

    def __init__(self):
        pass

    class MockEucaConnection(object):
        def run_instances(self, image_id,
                          min_count,
                          max_count,
                          key_name,
                          security_groups,
                          user_data,
                          addressing_type,
                          instance_type,
                          placement,
                          kernel_id,
                          ramdisk_id):
            pass

        def get_all_instances(self):
            return []

        def get_all_images(self):
            return []

    def getDefaultMachine(self):
        m = EucaSiteAdapter.Ec2MachineConfig()
        m.imageName = "imgName"
        m.instanceKey = "key"
        m.instanceType = "fat.lady"
        return m

    def getAltDefaultMachine(self):
        m = EucaSiteAdapter.Ec2MachineConfig()
        m.imageName = "imgAltName"
        m.instanceKey = "key"
        m.instanceType = "fat.lady"
        return m

    def getDefaultMachines(self):
        return dict({"euca-default": self.getDefaultMachine(),
                     "euca-alt-default": self.getAltDefaultMachine(),})

    def test_getMachineAvailable(self):
        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())

        self.assertTrue(espawn.isMachineTypeSupported("euca-default"))
        self.assertFalse(espawn.isMachineTypeSupported("not-here"))

    def test_getSiteInfo(self):
        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())

        siteInfo = espawn.siteInformation
        self.assertEqual(len(siteInfo.supportedMachineTypes), 2)
        self.assertTrue("euca-default" in siteInfo.supportedMachineTypes)
        self.assertTrue("euca-alt-default" in siteInfo.supportedMachineTypes)

    def getDefaultEucaConnection(self):
        self.MockEucaConnection.utest = self

        class LazyObject(object):
            def __init__(self):
                pass

        inst1 = LazyObject()
        inst1.instances = [LazyObject(), LazyObject(), LazyObject(), LazyObject(), LazyObject()]
        inst1.instances[0].image_id = "img233445"
        inst1.instances[0].state = "running"
        inst1.instances[1].image_id = "img233445"
        inst1.instances[1].state = "running"
        inst1.instances[2].image_id = "img233445"
        inst1.instances[2].state = "terminated"
        inst1.instances[3].image_id = "img123"
        inst1.instances[3].state = "running"
        inst1.instances[4].image_id = "img123-unknown"
        inst1.instances[4].state = "running"

        img1 = LazyObject()
        img1.id = "img233445"
        img1.location = "imgName/manifest.xml"
        img2 = LazyObject()
        img2.id = "img123"
        img2.location = "imgAltName/manifest.xml"
        img3 = LazyObject()
        img3.id = "img123-unknown"
        img3.location = "imgUn/manifest.xml"

        self.MockEucaConnection.get_all_instances = lambda xself: [inst1]
        self.MockEucaConnection.get_all_images = lambda xself: [img1, img2, img3]

        return self.MockEucaConnection()

    def test_onEventTerminate(self):

        self.wasRun = False

        def termcheck(self, terminateids):
            self.utest.assertEqual(len(terminateids), 1)
            self.utest.assertEqual(terminateids[0], "i-123456")
            self.utest.wasRun = True
            return terminateids

        self.MockEucaConnection.terminate_instances = termcheck
        EucaUtil.openConnection = lambda xself: self.getDefaultEucaConnection()

        espawn = EucaSiteAdapter()
        espawn.init()
        id1 = self.mr.newMachine()
        self.mr.machines[id1][self.mr.regSite] = "not my site"
        self.mr.machines[id1][self.mr.regMachineType] = "machine1"
        self.mr.updateMachineStatus(id1, self.mr.statusDisintegrated)

        id2 = self.mr.newMachine()
        self.mr.machines[id2][self.mr.regSite] = "default-site"
        self.mr.machines[id2][self.mr.regMachineType] = "machine1"
        self.mr.machines[id2][espawn.reg_site_euca_instance_id] = "i-123456"
        self.mr.updateMachineStatus(id2, self.mr.statusDisintegrated)

        # id3 = mr.newMachine()
        # mr.machines[id3][ mr.reg_site ] = "default-site"
        # mr.machines[id3][ mr.reg_machine_type ] = "machine2"
        # mr.updateMachineStatus(id3, mr.StatusDisintegrated )

        self.assertTrue(self.wasRun)
        self.assertEqual(self.mr.machines[id1][self.mr.regStatus], self.mr.statusDisintegrated)
        self.assertEqual(self.mr.machines[id2][self.mr.regStatus], self.mr.statusDown)

    def test_getTerminate(self):
        espawn = EucaSiteAdapter()
        id1 = self.mr.newMachine()
        self.mr.machines[id1][self.mr.regSite] = "not my site"
        self.mr.machines[id1][self.mr.regMachineType] = "machine1"
        self.mr.updateMachineStatus(id1, self.mr.statusWorking)

        id2 = self.mr.newMachine()
        self.mr.machines[id2][self.mr.regSite] = "default-site"
        self.mr.machines[id2][self.mr.regMachineType] = "machine1"
        self.mr.updateMachineStatus(id2, self.mr.statusBooting)

        id3 = mr.newMachine()
        self.mr.machines[id3][self.mr.regSite] = "default-site"
        self.mr.machines[id3][self.mr.regMachineType] = "machine1"
        self.mr.updateMachineStatus(id3, self.mr.statusWorking)

        id4 = self.mr.newMachine()
        self.mr.machines[id4][self.mr.regSite] = "default-site"
        self.mr.machines[id4][self.mr.regMachineType] = "machine2"
        self.mr.updateMachineStatus(id4, self.mr.statusWorking)

        # terminate the booting machine first
        espawn.terminateMachines("machine1", 1)

        self.assertEqual(self.mr.machines[id1][self.mr.regStatus], self.mr.statusWorking)
        self.assertEqual(self.mr.machines[id2][self.mr.regStatus], self.mr.statusPendingDisintegration)
        self.assertEqual(self.mr.machines[id3][self.mr.regStatus], self.mr.statusWorking)
        self.assertEqual(self.mr.machines[id4][self.mr.regStatus], self.mr.statusWorking)

        # terminate the booting machine first
        espawn.terminateMachines("machine1", 1)
        self.assertEqual(self.mr.machines[id1][self.mr.regStatus], self.mr.statusWorking)
        self.assertEqual(self.mr.machines[id2][self.mr.regStatus], self.mr.statusPendingDisintegration)
        self.assertEqual(self.mr.machines[id3][self.mr.regStatus], self.mr.statusPendingDisintegration)
        self.assertEqual(self.mr.machines[id4][self.mr.regStatus], self.mr.statusWorking)

    def test_getRunningMachines(self):
        # TODO: Fix
        return
        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())

        EucaUtil.openConnection = lambda xself: self.getDefaultEucaConnection()

        runningMachines = espawn.runningMachines()

        self.assertEqual(len(runningMachines), 2)
        self.assertEqual(len(runningMachines["euca-default"]), 2)
        self.assertEqual(len(runningMachines["euca-alt-default"]), 2)

    def test_applyDes(self):
        # TODO: Fix
        return

        class LazyObject(object):
            def __init__(self):
                pass

        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())

        self.MockEucaConnection.utest = self
        self.wasRun = False

        def runcheck(self, image_id,
                     min_count,
                     max_count,
                     key_name,
                     security_groups,
                     user_data,
                     addressing_type,
                     instance_type,
                     placement,
                     kernel_id,
                     ramdisk_id):
            self.utest.assertEqual(image_id, "i-2718281")
            self.utest.assertEqual(max_count, 3)
            self.utest.wasRun = True

            reserve = LazyObject()
            reserve.instances = []
            for i in range(0, 10):
                reserve.instances.append(LazyObject())
                reserve.instances[i].id = "i-123456"

            return reserve

        self.MockEucaConnection.run_instances = runcheck

        EucaUtil.openConnection = lambda xself: self.getDefaultEucaConnection()
        EucaUtil.getImageIdByImageName = lambda xself, con, iname: "i-2718281"

        des = dict({"euca-default": 5})
        espawn.applyMachineDecision(des)
        self.assertTrue(self.wasRun)

    def test_spawnMachineCloudFull(self):

        class LazyObject(object):
            def __init__(self):
                pass

        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())
        espawn.setConfig(EucaSiteAdapter.ConfigMaxMachines, 4)

        self.mr.clear()
        # insert 3 machines
        mid = self.mr.newMachine()
        self.mr.machines[mid][self.mr.regSite] = espawn.getConfig(EucaSiteAdapter.ConfigSiteName)
        self.mr.machines[mid][self.mr.regMachineType] = "euca-default"
        mid = self.mr.newMachine()
        self.mr.machines[mid][self.mr.regSite] = espawn.getConfig(EucaSiteAdapter.ConfigSiteName)
        self.mr.machines[mid][self.mr.regMachineType] = "euca-default"
        mid = self.mr.newMachine()
        self.mr.machines[mid][self.mr.regSite] = espawn.getConfig(EucaSiteAdapter.ConfigSiteName)
        self.mr.machines[mid][self.mr.regMachineType] = "euca-default"

        self.assertEqual(len(self.mr.machines), 3)

        self.MockEucaConnection.utest = self

        def runcheck(self, image_id,
                     min_count,
                     max_count,
                     key_name,
                     security_groups,
                     user_data,
                     addressing_type,
                     instance_type,
                     placement,
                     kernel_id,
                     ramdisk_id):
            self.utest.assertEqual(image_id, "i-2718281")

            reserve = LazyObject()
            reserve.instances = []
            # only 1 instance is stared
            for i in range(0, 1):
                reserve.instances.append(LazyObject())
                reserve.instances[i].id = "i-123456"
                reserve.instances[i].state = "pending"

            return reserve

        self.MockEucaConnection.run_instances = runcheck

        EucaUtil.openConnection = lambda xself: self.MockEucaConnection()
        EucaUtil.getImageIdByImageName = lambda xself, con, iname: "i-2718281"

        self.assertEqual(espawn.spawnMachines("euca-default", 10), 1)
        self.assertEqual(len(self.mr.machines), 4)

    def test_spawnMachine(self):

        class LazyObject(object):
            def __init__(self):
                pass

        espawn = EucaSiteAdapter()
        espawn.setConfig(EucaSiteAdapter.ConfigMachines, self.getDefaultMachines())

        self.mr.clear()
        self.assertEqual(len(self.mr.machines), 0)

        self.MockEucaConnection.utest = self

        def runcheck(self, image_id,
                     min_count,
                     max_count,
                     key_name,
                     security_groups,
                     user_data,
                     addressing_type,
                     instance_type,
                     placement,
                     kernel_id,
                     ramdisk_id):
            self.utest.assertEqual(image_id, "i-2718281")

            reserve = LazyObject()
            reserve.instances = []
            for i in range(0, 10):
                reserve.instances.append(LazyObject())
                reserve.instances[i].id = "i-123456"
                reserve.instances[i].state = "pending"

            return reserve

        self.MockEucaConnection.run_instances = runcheck

        EucaUtil.openConnection = lambda xself: self.MockEucaConnection()
        EucaUtil.getImageIdByImageName = lambda xself, con, iname: "i-2718281"

        self.assertEqual(espawn.spawnMachines("euca-default", 10), 10)
        self.assertEqual(len(self.mr.machines), 10)

        for (mid, machine) in self.mr.machines.items():
            self.assertTrue(espawn.reg_site_euca_instance_id in machine)
            self.assertTrue(self.mr.regSite in machine)
            self.assertTrue(self.mr.regStatus in machine)
            self.assertEqual(machine[self.mr.regMachineType], "euca-default")


class ONESiteAdapterTest(ScaleTest.ScaleTestBase):
    pass
