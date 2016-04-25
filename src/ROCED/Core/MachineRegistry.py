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
from __future__ import unicode_literals

import abc
import logging
import uuid
from datetime import datetime

from Util.Logging import CsvStats
from . import Event


class MachineEvent(Event.EventBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, mid):
        super(MachineEvent, self).__init__()
        self.id = mid


class NewMachineEvent(MachineEvent):
    def __init__(self, mid):
        super(NewMachineEvent, self).__init__(mid)


class MachineRemovedEvent(MachineEvent):
    def __init__(self, mid):
        super(MachineRemovedEvent, self).__init__(mid)


class StatusChangedEvent(MachineEvent):
    def __init__(self, mid, oldStatus, newStatus):
        super(StatusChangedEvent, self).__init__(mid)
        self.newStatus = newStatus
        self.oldStatus = oldStatus


# Implemented singleton
class MachineRegistry(Event.EventPublisher):
    statusBooting = "booting"
    statusUp = "up"
    statusIntegrating = "integrating"
    statusWorking = "working"
    statusPendingDisintegration = "pending-disintegration"
    statusDisintegrating = "disintegrating"
    statusDisintegrated = "disintegrated"
    statusShutdown = "down"  # not in PBS, but still running and needing cloud resources
    statusDown = "down"

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

    def __new__(cls, *args):
        if '_the_instance' not in cls.__dict__:
            cls._the_instance = object.__new__(cls)
        return cls._the_instance

    def __init__(self):
        self.logger = logging.getLogger('MachReg')
        if '_ready' not in dir(self):
            self._ready = True
            self.machines = dict()
            super(MachineRegistry, self).__init__()

    def getMachines(self, site=None, status=None, machineType=None):
        """Return MachineRegistry dictionary, filtered by variables.

        :return {machine_id: {a:b, c:d, e:f}, ... }
        """
        newd = dict()

        for (k, v) in self.machines.items():
            if (site is None or v.get(self.regSite) == site) and \
                    (status is None or v.get(self.regStatus) == status) and \
                    (machineType is None or v.get(self.regMachineType) == machineType):
                newd[k] = v

        return newd

    def updateMachineStatus(self, mid, newStatus):
        newTime = datetime.now()
        if self.regStatusLastUpdate in self.machines[mid]:
            oldTime = self.machines[mid][self.regStatusLastUpdate]
        else:
            oldTime = newTime
        diffTime = newTime - oldTime

        oldStatus = self.machines[mid].get("status", None)
        self.machines[mid][self.regStatus] = newStatus
        self.machines[mid][self.regStatusLastUpdate] = newTime
        self.machines[mid][self.statusChangeHistory].append(
            {
                "old_status": oldStatus,
                "new_status": newStatus,
                "timestamp": str(newTime),
                "time_diff": str(diffTime)
            }
        )

        if (mid in self.machines) and (len(self.machines[mid][self.statusChangeHistory]) > 0):
            with CsvStats() as csv_stats:
                if str("site") not in self.machines[mid]:
                    self.machines[mid]["site"] = "site"
                csv_stats.add_item(site=self.machines[mid]["site"], mid=mid,
                                   old_status=self.machines[mid][self.statusChangeHistory][-1][
                                       "old_status"],
                                   new_status=self.machines[mid][self.statusChangeHistory][-1][
                                       "new_status"],
                                   timestamp=self.machines[mid][self.statusChangeHistory][-1][
                                       "timestamp"],
                                   time_diff=self.machines[mid][self.statusChangeHistory][-1][
                                       "time_diff"])
                csv_stats.write_stats()

        self.logger.info(
            "updating status of " + str(mid) + ": " + str(oldStatus) + " -> " + newStatus)
        self.publishEvent(StatusChangedEvent(mid, oldStatus, newStatus))

    def calcLastStateChange(self, mid):
        """Calculate time passed since last machine state change (in seconds)

        :param mid:

        :return: seconds
        :type: int
        """
        diff = datetime.now() - self.machines[mid].get(self.regStatusLastUpdate, datetime.now())
        return diff.seconds

    def getMachineOverview(self):
        """Create comma-separated list of machines in different statuses."""
        info = "MachineState: "
        l = list(self.getMachines(status=self.statusBooting))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusUp))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusIntegrating))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusWorking))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusPendingDisintegration))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusDisintegrating))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusDisintegrated))
        info += str(len(l)) + ","
        l = list(self.getMachines(status=self.statusDown))
        info += str(len(l))
        return info

    def newMachine(self, mid=None):
        if mid is None:
            mid = str(uuid.uuid4())
        self.logger.debug("adding machine with id " + mid)
        self.machines[mid] = dict()
        self.machines[mid][self.statusChangeHistory] = []
        self.publishEvent(NewMachineEvent(mid))
        return mid

    def removeMachine(self, mid):
        self.logger.debug("removing machine with id " + str(mid))
        self.machines.pop(mid)
        self.publishEvent(MachineRemovedEvent(mid))

    def clear(self):
        """ Clear machine registry. Should only be used in unit tests."""
        self.machines = dict()
        self.clearListeners()
