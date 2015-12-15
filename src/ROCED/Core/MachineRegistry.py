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

import logging
import uuid
import time, datetime

import Event

from Util.Logging import JsonStats

class MachineEvent(Event.EventBase):
    def id():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._id

        def fset(self, value):
            self._id = value

        def fdel(self):
            del self._id

        return locals()

    id = property(**id())


class NewMachineEvent(MachineEvent):
    def __init__(self, id):
        self.id = id


class MachineRemovedEvent(MachineEvent):
    def __init__(self, id):
        self.id = id


class StatusChangedEvent(MachineEvent):
    def oldStatus():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._oldStatus

        def fset(self, value):
            self._oldStatus = value

        def fdel(self):
            del self._oldStatus

        return locals()

    oldStatus = property(**oldStatus())

    def newStatus():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._newStatus

        def fset(self, value):
            self._newStatus = value

        def fdel(self):
            del self._newStatus

        return locals()

    newStatus = property(**newStatus())

    def __init__(self, id, oldStatus, newStatus):
        self.newStatus = newStatus
        self.oldStatus = oldStatus
        self.id = id


"""
Implemented singleton
"""


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

    def __new__(self, *args):
        if not '_the_instance' in self.__dict__:
            self._the_instance = object.__new__(self)
        return self._the_instance

    def __init__(self):
        self.logger = logging.getLogger('MachReg')
        if not '_ready' in dir(self):
            self._ready = True
            self.machines = dict()
            self.listener = []

    def machines():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._machines

        def fset(self, value):
            self._machines = value

        def fdel(self):
            del self._machines

        return locals()

    machines = property(**machines())

    def getMachines(self, site=None, status=None, machineType=None):
        newd = dict()

        for (k, v) in self.machines.iteritems():
            if (site == None or v.get(self.regSite) == site) and \
                    (status == None or v.get(self.regStatus) == status) and \
                    (machineType == None or v.get(self.regMachineType) == machineType):
                newd[k] = v

        return newd

    def updateMachineStatus(self, id, newStatus):
        newTime = datetime.datetime.now()
        if self.regStatusLastUpdate in self.machines[id]:
            oldTime = self.machines[id][self.regStatusLastUpdate]
        else:
            oldTime = newTime
        diffTime = newTime - oldTime

        oldStatus = self.machines[id].get("status", None)
        self.machines[id]["status"] = newStatus
        self.machines[id][self.regStatus] = newStatus
        self.machines[id][self.regStatusLastUpdate] = newTime #datetime.datetime.now()
        self.machines[id][self.statusChangeHistory].append(
            {
                "old_status": oldStatus,
                "new_status": newStatus,
                "timestamp": str(newTime),
                "time_diff": str(diffTime)
            }
        )

        if (id in self.machines) and (len(self.machines[id][self.statusChangeHistory]) > 0):
            json_stats = JsonStats()
            if str("site") not in self.machines[id].keys():
                self.machines[id]["site"] = "site"
            json_stats.add_item(self.machines[id]["site"], id, self.machines[id][self.statusChangeHistory][-1])
            json_stats.write_stats()
            del json_stats

        #self.machines[id][self.statusChangeHistory].append([oldStatus, newStatus, str(newTime), str(diffTime)])

        self.logger.info("updating status of " + str(id) + ": " + str(oldStatus) + " -> " + newStatus)
        self.publishEvent(StatusChangedEvent(id, oldStatus, newStatus))

    # in secs
    def calcLastStateChange(self, mid):
        return (
        datetime.datetime.now() - self.machines[mid].get(self.regStatusLastUpdate, datetime.datetime.now())).seconds

    def getMachineOverview(self):
        info = "MachineState: "
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusBooting, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusUp, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusIntegrating, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusWorking, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusPendingDisintegration, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusDisintegrating, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusDisintegrated, self.machines.iteritems())
        info += str(len(l)) + ","
        l = filter(lambda (k, v): v.get(self.regStatus) == self.statusDown, self.machines.iteritems())
        info += str(len(l))
        return info

    def newMachine(self, id=None):
        if id == None:
            id = str(uuid.uuid4())
        self.logger.debug("adding machine with id " + id)
        self.machines[id] = dict()
        self.machines[id][self.statusChangeHistory] = []
        self.publishEvent(NewMachineEvent(id))
        return id

    def removeMachine(self, id):
        self.logger.debug("removing machine with id " + str(id))
        self.machines.pop(id)
        self.publishEvent(MachineRemovedEvent(id))

    # only used for testing
    def clear(self):
        self.machines = dict()
        self.clearListeners()
