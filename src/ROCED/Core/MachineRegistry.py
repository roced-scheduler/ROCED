# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015 by Georg Fleig, Thomas Hauth and Stephan Riedel
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
import logging
import uuid
from datetime import datetime

from Util.Logging import CsvStats
from Util.PythonTools import Singleton
from . import Event


class MachineRegistry(Event.EventPublisher, Singleton):
    statusBooting = "booting"
    statusUp = "up"
    statusIntegrating = "integrating"
    statusWorking = "working"
    statusPendingDisintegration = "pending-disintegration"
    statusDisintegrating = "disintegrating"
    statusDisintegrated = "disintegrated"
    # statusShutdown = "down"  # not in PBS, but still running and needing cloud resources
    statusDown = "down"

    # all states in consecutive order
    list_status = (statusBooting, statusUp, statusIntegrating, statusWorking, statusPendingDisintegration,
                   statusDisintegrating, statusDisintegrated, statusDown)

    statusChangeHistory = "state_change_history"

    regStatus = "status"
    regStatusLastUpdate = "status_last_update"
    regHostname = "hostname"
    regInternalIp = "internal_ip"
    regUsesGateway = "uses_gateway"
    regGatewayIp = "gateway_ip"
    regGatewayKey = "gateway_key"
    regGatewayUser = "gateway_user"
    regSshKey = "ssh_key"
    regSite = "site"
    regSiteType = "site_type"
    regMachineType = "machine_type"
    regMachineId = "machine_id"
    regMachineCores = "machine_cores"
    regMachineLoad = "machine_load"
    regVpnIp = "vpn_ip"
    regVpnCert = "vpn_cert"
    regVpnCertIsValid = "vpn_cert_is_valid"

    def init(self):
        self.logger = logging.getLogger("MachReg")
        self.machines = dict()
        super(MachineRegistry, self).init()

    def getMachines(self, site=None, status=None, machineType=None):
        """Return MachineRegistry dictionary, filtered by variables.

        :return {machine_id: {a:b, c:d, e:f}, ... }
        """
        return {mid: machine for mid, machine in self.machines.items() if
                (site is None or machine.get(self.regSite) == site) and
                (status is None or machine.get(self.regStatus) == status) and
                (machineType is None or machine.get(self.regMachineType) == machineType)}

    def updateMachineStatus(self, mid, newStatus):
        """Change Machine status"""
        newTime = datetime.now()
        if self.regStatusLastUpdate in self.machines[mid]:
            oldTime = self.machines[mid][self.regStatusLastUpdate]
        else:
            oldTime = newTime
        diffTime = newTime - oldTime

        oldStatus = self.machines[mid].get(self.regStatus, None)
        self.machines[mid][self.regStatus] = newStatus
        self.machines[mid][self.regStatusLastUpdate] = newTime
        self.machines[mid][self.statusChangeHistory].append({"old_status": oldStatus, "new_status": newStatus,
                                                             "timestamp": str(newTime), "time_diff": str(diffTime)})

        if mid in self.machines and len(self.machines[mid][self.statusChangeHistory]) > 0:
            with CsvStats() as csv_stats:
                csv_stats.add_item(site=self.machines[mid][self.regSite], mid=mid,
                                   old_status=self.machines[mid][self.statusChangeHistory][-1]["old_status"],
                                   new_status=self.machines[mid][self.statusChangeHistory][-1]["new_status"],
                                   timestamp=self.machines[mid][self.statusChangeHistory][-1]["timestamp"],
                                   time_diff=self.machines[mid][self.statusChangeHistory][-1]["time_diff"])
                csv_stats.write_stats()

        self.logger.info("Updating status of %s: %s -> %s" % (mid, oldStatus, newStatus))
        self.publishEvent(StatusChangedEvent(mid, oldStatus, newStatus))

    def calcLastStateChange(self, mid):
        # type: (str) -> int
        """Calculate time passed since last machine state change (in seconds)

        :param mid:
        :return: seconds
        """
        diff = datetime.now() - self.machines[mid].get(self.regStatusLastUpdate, datetime.now())
        return diff.total_seconds()

    def getMachineOverview(self):
        # type: () -> str
        """Create comma-separated list of number of machines in each state."""
        info = "MachineState: %s" % ",".join((str(len(self.getMachines(status=status_)))
                                              for status_ in self.list_status))
        return info

    def newMachine(self, mid=None):
        # type: uuid.uuid4 -> uuid.uuid4
        """Create a new machine entry and publish "NewMachineEvent" event to all listeners."""
        if mid is None:
            mid = str(uuid.uuid4())
        self.logger.debug("Adding machine with id %s." % mid)
        self.machines[mid] = dict()
        self.machines[mid][self.regSite] = self.regSite
        self.machines[mid][self.statusChangeHistory] = []
        self.publishEvent(NewMachineEvent(mid))
        return mid

    def removeMachine(self, mid):
        # type: str -> None
        """Remove a machine entry and publish "MachineRemovedEvent" event to all listeners."""
        self.logger.debug("Removing machine with id %s." % mid)
        # Also publish machine information for possible cleanups, since it's already removed when the event occurs.
        machine = self.machines[mid]
        self.machines.pop(mid)
        event = MachineRemovedEvent(mid, machine)
        self.publishEvent(event)

    def clear(self):
        """ Clear machine registry (without raising any events). Should only be used in unit tests."""
        self.machines = dict()
        self.clearListeners()


class MachineEvent(Event.EventBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, mid):
        super(MachineEvent, self).__init__()
        self.id = mid


class NewMachineEvent(MachineEvent):
    def __init__(self, mid):
        super(NewMachineEvent, self).__init__(mid)


class MachineRemovedEvent(MachineEvent):
    def __init__(self, mid, machine):
        # type: (str, dict) -> None
        """Event "Machine was removed from MachineRegistry", published to every registered listener."""
        super(MachineRemovedEvent, self).__init__(mid)
        self.machine = machine


class StatusChangedEvent(MachineEvent):
    def __init__(self, mid, oldStatus, newStatus):
        super(StatusChangedEvent, self).__init__(mid)
        self.newStatus = newStatus
        self.oldStatus = oldStatus
