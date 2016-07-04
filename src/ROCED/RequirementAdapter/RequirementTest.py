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

from Core import ScaleTest
from RequirementAdapter import Requirement


class RequirementAdapterTest(Requirement.RequirementAdapterBase):
    def __init__(self, machineType="default"):
        super(RequirementAdapterTest, self).__init__(machineType)
        logging.debug("New requirement adapter for machine \"%s\"" % machineType)

    @property
    def description(self):
        return "Test requirement adapter for unit-test."

    @property
    def requirement(self):
        return super(RequirementAdapterTest, self).requirement

    @requirement.setter
    def requirement(self, requirement_):
        Requirement.RequirementAdapterBase.requirement.__set__(self, requirement_)


class RequirementBoxTest(ScaleTest.ScaleTestBase):
    def test_getReq(self):
        logging.debug("=======Testing Requirement Adapters=======")
        box = Requirement.RequirementBox()

        box.addAdapter(RequirementAdapterTest("type1"))
        box.addAdapter(RequirementAdapterTest("type2"))
        box.addAdapter(RequirementAdapterTest("type3"))
        box.addAdapter(RequirementAdapterTest("type2"))

        self.assertEqual(len(box.getMachineTypeRequirement()), 3)
        self.assertEqual(box.getMachineTypeRequirement()["type2"], 0)
        logging.info(str(box.getMachineTypeRequirement()))

        box.adapterList[1].requirement = 3
        logging.debug("Second adapter requirement increased by 3.")
        box.adapterList[3].requirement = 2
        logging.debug("Fourth adapter requirement increased by 2.")

        self.assertEqual(len(box.getMachineTypeRequirement()), 3)
        self.assertEqual(box.getMachineTypeRequirement()["type2"], 5)
        logging.info(str(box.getMachineTypeRequirement()))
