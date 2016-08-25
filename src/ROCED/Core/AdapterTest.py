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

from IntegrationAdapter import Integration
from SiteAdapter.Site import SiteAdapterBase
from . import Config
from . import ScaleTest
from .Adapter import AdapterBase, AdapterBoxBase


class AdapterBaseTestClass(AdapterBase):
    @property
    def description(self):
        return super(AdapterBaseTestClass, self).description


class IntegrationAdapterTest(Integration.IntegrationAdapterBase):
    def init(self):
        super(IntegrationAdapterTest, self).init()

    def manage(self, cleanup):
        pass

    @property
    def description(self):
        return super(IntegrationAdapterTest, self).description


class AdapterBoxTest(ScaleTest.ScaleTestBase):
    def test_getBoxContent(self):
        logging.debug("=======Testing AdapterBox=======")
        logging.basicConfig(level=logging.DEBUG)

        box = AdapterBoxBase()
        box.addAdapter(IntegrationAdapterTest())
        box.addAdapter(IntegrationAdapterTest())

        con = box.adapterList
        self.assertEqual(len(con), 2)


class AdapterBaseTest(ScaleTest.ScaleTestBase):
    def test_addOptionalConfigKeys(self):
        logging.debug("=======Testing AdapterBase=======")
        config = configparser.RawConfigParser()

        # general
        config.add_section(Config.GeneralSection)
        config.set(Config.GeneralSection, Config.GeneralSiteAdapters, "fake_site")

        # Site
        config.add_section("fake_site")
        config.set("fake_site", SiteAdapterBase.ConfigMachineBootTimeout, 20)

        adapter = AdapterBaseTestClass()

        # config keys not in "config file"
        config1_key = "key"
        config1_def_val = "def_value"
        config1_type = Config.ConfigTypeString
        config1_description = "config value not in config file"

        # config keys in "config file"
        config2_def_val = 10
        config2_def_desc = "config value in config file"

        adapter.addOptionalConfigKeys(config1_key, config1_type, description=config1_description,
                                      default=config1_def_val)
        adapter.addOptionalConfigKeys(SiteAdapterBase.ConfigMachineBootTimeout,
                                      Config.ConfigTypeInt,
                                      description=config2_def_desc, default=config2_def_val)

        adapter.loadConfigValue(adapter.optionalConfigKeys, config, True, "fake_site", adapter)

        self.assertTrue(len(adapter.optionalConfigKeys) == 2)
        self.assertEqual(adapter.getConfig(SiteAdapterBase.ConfigMachineBootTimeout), 20)
        self.assertEqual(adapter.getConfig(config1_key), config1_def_val)
