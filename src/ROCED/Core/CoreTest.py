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


import ConfigParser

from Core import MachineStatus, ScaleCore, ScaleCoreFactory
from Core import Config
from Broker import StupidBroker, SiteBrokerBase
from SiteAdapter.Site import SiteAdapterBase, SiteInformation
from RequirementAdapter.Requirement import RequirementAdapterBase
import ScaleTest


class ScaleCoreTestBase(ScaleTest.ScaleTestBase):
    def getDefaultSiteInfo(self):
        sinfo = [SiteInformation(), SiteInformation()]

        sinfo[0].siteName = "site2"
        sinfo[0].baselineMachines = 2
        sinfo[0].maxMachines = 5
        sinfo[0].supportedMachineTypes = ["machine1", "machine2", "machine3"]
        sinfo[0].cost = 3

        sinfo[1].siteName = "site1"
        sinfo[1].baselineMachines = 2
        sinfo[1].maxMachines = 5
        sinfo[1].supportedMachineTypes = ["machine1", "machine3"]
        sinfo[1].cost = 0

        return sinfo


class ScaleCoreTest(ScaleCoreTestBase):
    def test_factory(self):
        config = ConfigParser.RawConfigParser()

        # general
        config.add_section(Config.GeneralSection)
        config.set(Config.GeneralSection, Config.GeneralBroker, 'default_broker')

        config.set(Config.GeneralSection, Config.GeneralSiteAdapters, 'fake_site1 fake_site2')
        config.set(Config.GeneralSection, Config.GeneralIntAdapters, 'fake_req')
        config.set(Config.GeneralSection, Config.GeneralReqAdapters, 'fake_int')


        # Broker
        config.add_section("default_broker")
        config.set("default_broker", Config.ConfigObjectType, 'Broker.StupidBroker')

        # Site
        config.add_section("fake_site1")
        config.set("fake_site1", Config.ConfigObjectType, 'FakeSiteAdapter')
        config.set("fake_site1", SiteAdapterBase.ConfigSiteName, 'fake_site1')
        config.set("fake_site1", SiteAdapterBase.ConfigSiteDescription, 'my test description')

        config.add_section("fake_site2")
        config.set("fake_site2", Config.ConfigObjectType, 'FakeSiteAdapter')
        config.set("fake_site2", SiteAdapterBase.ConfigSiteName, 'fake_site2')
        config.set("fake_site2", SiteAdapterBase.ConfigSiteDescription, 'my test description 2')

        # Integration
        config.add_section("fake_int")
        config.set("fake_int", Config.ConfigObjectType, 'FakeIntegrationAdapter')

        # Requirement
        config.add_section("fake_req")
        config.set("fake_req", Config.ConfigObjectType, 'FakeRequirementAdapter')

        config.write(open("mconfig.cfg", "w"))

        fact = ScaleCoreFactory()

        core = fact.getCore(config)
        self.assertFalse(core == None)
        self.assertFalse(core.broker == None)

        self.assertEqual(len(core.siteBox.get_adapterList()), 2)

    def test_manage(self):
        broker = SiteBrokerBase()
        broker.decide = lambda machineTypes, siteInfo: dict({"site1": dict({"machine1": 1})})

        req = RequirementAdapterBase()
        req.getCurrentRequirement = lambda: 1
        # req.getNeededMachineType = lambda xself: "machine1"

        site1 = SiteAdapterBase()
        site1.getSiteName = lambda: "site1"
        site1.getSiteInformation = lambda: self.getDefaultSiteInfo()
        site1.getRunningMachines = lambda: dict({"machine1": [None], "machine2": [None, None]})

        site2 = SiteAdapterBase()
        site2.getSiteName = lambda: "site2"
        site2.getSiteInformation = lambda: self.getDefaultSiteInfo()
        site2.getRunningMachines = lambda: dict({"machine1": [None]})

        sc = ScaleCore(broker, None, [req, req], [site1, site2], [], False)


class StupidBrokerTest(ScaleCoreTestBase):
    def test_decide(self):
        broker = StupidBroker(20, 0)
        broker.shutdownDelay = 0
        mtypes = dict({"machine1": MachineStatus(),
                       "machine2": MachineStatus(),
                       "machine3": MachineStatus()})

        mtypes["machine1"].required = 2
        mtypes["machine1"].actual = 0
        mtypes["machine2"].required = 2
        mtypes["machine2"].actual = 4
        mtypes["machine3"].required = 4
        mtypes["machine3"].actual = 4

        sinfo = self.getDefaultSiteInfo()
        orders = broker.decide(mtypes, sinfo)

        self.assertEqual(orders["site1"]["machine1"], 2)
        self.assertTrue(not "machine1" in orders["site2"])
        self.assertEqual(orders["site2"]["machine2"], -2)
        self.assertTrue(not "machine2" in orders["site1"])
        self.assertTrue(not "machine3" in orders["site1"])
        self.assertTrue(not "machine3" in orders["site2"])
