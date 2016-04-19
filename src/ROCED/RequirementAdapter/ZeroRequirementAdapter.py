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


from RequirementAdapter.Requirement import RequirementAdapterBase


class ZeroRequirementAdapter(RequirementAdapterBase):
    def __init__(self, machineType="default"):
        super(ZeroRequirementAdapter, self).__init__(machineType)

    @property
    def description(self):
        return "ZeroRequirementAdapter"

    @property
    def requirement(self):
        return 0

    def getNeededMachineType(self):
        return "euca-default"
