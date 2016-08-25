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

import abc

from Core import Adapter, MachineRegistry


class IntegrationAdapterBase(Adapter.AdapterBase):
    """
    IntegrationAdapters are responsible for monitoring changes in the scheduling infrastructure (batch servers!).
    They may also be responsible for adding/removing cloud machines to the scheduling infrastructure, if the machines
    can't do it automatically.
    """
    __metaclass__ = abc.ABCMeta

    mr = MachineRegistry.MachineRegistry()

    @property
    @abc.abstractmethod
    def description(self):
        return "IntegrationAdapterBase"

    @abc.abstractmethod
    def init(self):
        pass

    @abc.abstractmethod
    def manage(self, cleanup=False):
        """Periodically called manage function responsible for initiating status changes.

        This method has to handle the following (machine registry) machine status transitions:
            Up                      -> Integrating
            Integrating             -> Working
            Working                 <-> Pending Disintegration
            Pending Disintegration  -> Disintegrating
            Disintegrating          -> Disintegrated
        The method "OnEvent" can also handle some of these. If the class is registered with the machine registry.

        1. Connect with the batch system site to retrieve a list of all machines including their status.
        2. Iterate the Machine Registry (Method getSiteMachines) and change machine status accordingly.

        As described earlier, it also can be used to handle different tasks regarding the scheduling infrastructure.

        :return:
        """
        pass


class IntegrationBox(Adapter.AdapterBoxBase):
    pass
