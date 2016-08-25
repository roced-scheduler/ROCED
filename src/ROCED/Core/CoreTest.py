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

import configparser

from RequirementAdapter.RequirementTest import RequirementAdapterTest
from SiteAdapter.Site import SiteAdapterBase, SiteInformation
from . import Config
from . import ScaleTest
from .Broker import StupidBroker, SiteBrokerBase
from .Core import MachineStatus, ScaleCore, ScaleCoreFactory


class SiteBrokerTest(SiteBrokerBase):
    def decide(self, machineTypes, siteInfo):
        pass


class SiteAdapterTest(SiteAdapterBase):
    def __init__(self):
        """
        Empty Unittest implementation of SiteAdapterBase
        """
        super(SiteAdapterTest, self).__init__()

    def spawnMachines(self, machineType, count):
        return

    def manage(self, cleanup=False):
        pass

    @property
    def siteName(self):
        return super(SiteAdapterTest, self).siteName

    @siteName.setter
    def siteName(self, value_):
        self.setConfig(self.ConfigSiteName, value_)


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
        config = configparser.RawConfigParser()

        # general
        config.add_section(Config.GeneralSection)
        config.set(Config.GeneralSection, Config.GeneralBroker, "default_broker")

        config.set(Config.GeneralSection, Config.GeneralSiteAdapters, "fake_site1 fake_site2")
        config.set(Config.GeneralSection, Config.GeneralIntAdapters, "fake_int1 fake_int2")
        config.set(Config.GeneralSection, Config.GeneralReqAdapters, "fake_req1 fake_req2")

        # Broker
        config.add_section("default_broker")
        config.set("default_broker", Config.ConfigObjectType, "Broker.StupidBroker")

        # Site
        config.add_section("fake_site1")
        config.set("fake_site1", Config.ConfigObjectType, "FakeSiteAdapter")
        config.set("fake_site1", SiteAdapterBase.ConfigSiteName, "fake_site1")
        config.set("fake_site1", SiteAdapterBase.ConfigSiteDescription, "my test description")

        config.add_section("fake_site2")
        config.set("fake_site2", Config.ConfigObjectType, "FakeSiteAdapter")
        config.set("fake_site2", SiteAdapterBase.ConfigSiteName, "fake_site2")
        config.set("fake_site2", SiteAdapterBase.ConfigSiteDescription, "my test description 2")

        # Integration
        config.add_section("fake_int1")
        config.set("fake_int1", Config.ConfigObjectType, "FakeIntegrationAdapter")
        config.set("fake_int1", SiteAdapterBase.ConfigSiteName, "fake_site1")
        config.add_section("fake_int2")
        config.set("fake_int2", Config.ConfigObjectType, "FakeIntegrationAdapter")
        config.set("fake_int2", SiteAdapterBase.ConfigSiteName, "fake_site2")

        # Requirement
        config.add_section("fake_req1")
        config.set("fake_req1", Config.ConfigObjectType, "FakeRequirementAdapter")
        config.add_section("fake_req2")
        config.set("fake_req2", Config.ConfigObjectType, "FakeRequirementAdapter")

        logging.debug("=======Testing Core=======")

        core = ScaleCoreFactory.getCore(config)
        self.assertFalse(core is None)
        self.assertFalse(core.broker is None)

        self.assertEqual(len(core.siteBox.adapterList), 2)

    def test_manage(self):
        logging.debug("=======Testing Management=======")
        broker = SiteBrokerTest()
        broker.decide = lambda machineTypes, siteInfo: dict({"site1": dict({"machine1": 1})})

        req = RequirementAdapterTest()
        req.requirement = 1

        site1 = SiteAdapterTest()
        site1.siteName = "site1"
        site1.getSiteInformation = lambda: self.getDefaultSiteInfo()
        site1.getRunningMachines = lambda: dict({"machine1": [None], "machine2": [None, None]})

        site2 = SiteAdapterTest()
        site2.siteName = "site2"
        site2.getSiteInformation = lambda: self.getDefaultSiteInfo()
        site2.getRunningMachines = lambda: dict({"machine1": [None]})

        sc = ScaleCore(broker, None, [req, req], [site1, site2], [], False)


class StupidBrokerTest(ScaleCoreTestBase):
    def test_decide(self):
        logging.debug("=======Testing Broker=======")
        broker = StupidBroker(20, 0)
        broker.shutdownDelay = 0
        mtypes = {"machine1": MachineStatus(),
                  "machine2": MachineStatus(),
                  "machine3": MachineStatus()}

        mtypes["machine1"].required = 2
        mtypes["machine1"].actual = 0
        mtypes["machine2"].required = 2
        mtypes["machine2"].actual = 4
        mtypes["machine3"].required = 4
        mtypes["machine3"].actual = 4

        sinfo = self.getDefaultSiteInfo()
        orders = broker.decide(mtypes, sinfo)

        self.assertEqual(orders["site1"]["machine1"], 2)
        self.assertTrue("machine1" not in orders["site2"])
        self.assertEqual(orders["site2"]["machine2"], -2)
        self.assertTrue("machine2" not in orders["site1"])
        self.assertTrue("machine3" not in orders["site1"])
        self.assertTrue("machine3" not in orders["site2"])
