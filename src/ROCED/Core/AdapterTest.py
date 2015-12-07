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

import ConfigParser

import ScaleTest
from Adapter import AdapterBase, AdapterBoxBase
from SiteAdapter.Site import SiteAdapterBase, SiteInformation
from IntegrationAdapter import Integration
import Config


class AdapterBoxTest(ScaleTest.ScaleTestBase):
    _adapterList = []

    def test_getBoxContent(self):
        logging.basicConfig(level=logging.DEBUG)

        box = AdapterBoxBase()
        box._adapterList.append(Integration.IntegrationAdapterBase())
        box._adapterList.append(Integration.IntegrationAdapterBase())

        con = box.get_adapterList()
        self.assertEqual(len(con), 2)


class AdapterBaseTest(ScaleTest.ScaleTestBase):
    _adapterList = []

    def test_addOptionalConfigKeys(self):
        config = ConfigParser.RawConfigParser()

        # general
        config.add_section(Config.GeneralSection)
        config.set(Config.GeneralSection, Config.GeneralSiteAdapters, 'fake_site')

        # Site
        config.add_section("fake_site")
        config.set("fake_site", SiteAdapterBase.ConfigMachineBootTimeout, 20)


        adapter = AdapterBase()

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
        adapter.addOptionalConfigKeys(SiteAdapterBase.ConfigMachineBootTimeout, Config.ConfigTypeInt,
                                      description=config2_def_desc ,default=config2_def_val)

        adapter.loadConfigValue(adapter.getOptionalConfigKeys(), config, True, "fake_site", adapter)

        self.assertTrue(len(adapter.getOptionalConfigKeys()) == 2)
        self.assertEqual(adapter.getConfig(SiteAdapterBase.ConfigMachineBootTimeout), 20)
        self.assertEqual(adapter.getConfig(config1_key), config1_def_val)
