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

import datetime
import logging

try:
    # TODO: These seem to be here for self.getApiUtil...
    # from EucaUtil import EucaUtil
    # from EucaUtil import Ec2Util
    # see http://docs.pythonboto.org/
    import boto.exception
except ImportError:
    pass

from .Site import SiteAdapterBase
from Core import MachineRegistry
from Util import ScaleTools


class NovaSiteAdapter(SiteAdapterBase):
    class Ec2MachineConfig(object):
        def __init__(self):
            self.imageName = None
            self.userData = ""
            self.instanceType = None
            self.instanceKey = None

            self.gatewayIp = None
            self.gatewayUser = None
            self.gatewayKey = None

            self.usesGateway = False
            self.securityGroup = None
            self.addressingType = "public"

            self.kernelId = None
            self.ramdiskId = None

    reg_site_euca_instance_id = "site_euca_instance_id"
    reg_site_euca_first_dead_check = "site_euca_first_dead_check"

    def __init__(self):
        super(NovaSiteAdapter, self).__init__()

    def init(self):
        # todo: see whats running as we start up
        self.mr.registerListener(self)

    def onEvent(self, evt):
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            if self.mr.machines[evt.id].get(self.mr.regSite) == self.siteName:
                # check correct site etc...
                if evt.newStatus == self.mr.statusDisintegrated:
                    # ha, machine to kill
                    self.eucaTerminateMachines(
                        [self.mr.machines[evt.id].get(self.reg_site_euca_instance_id)])
                    # TODO maybe use shutdown in between ?
                    self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def getConfigAsDict(self, onlyPublic=False):
        new = super(NovaSiteAdapter, self).getConfigAsDict(True)
        new.pop(self.ConfigMachines)

        return new

    def getMachineByEucaId(self, machineList, euca_id):
        m = [machineList[mid] for (mid, machine) in machineList.items()
             if machine.get(self.reg_site_euca_instance_id) == euca_id]
        if len(m) == 0:
            # raise LookupError("Machine with euca id " + str(euca_id) + " not found in scale machine repository")
            return None
        else:
            return m[0]

    def checkForDeadMachine(self, mid):
        logging.info("Machine %s is running but no ssh connect yet." % mid)
        firstCheck = self.mr.machines[mid].get(self.reg_site_euca_first_dead_check, None)

        if firstCheck is None:
            self.mr.machines[mid][self.reg_site_euca_first_dead_check] = datetime.datetime.now()
        else:
            if (datetime.datetime.now() - firstCheck).total_seconds() > self.getConfig(
                    self.ConfigMachineBootTimeout):
                logging.warning("Machine %s did not boot in time. Shutting down." % mid)
                self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

    def manage(self, cleanup=False):
        # check for machine status
        ut = self.getApiUtil()
        logging.info("Querying Nova Server for running instances...")

        try:
            euca_conn = ut.openConnection()
            reservations = euca_conn.get_all_instances()
        except boto.exception.EC2ResponseError:
            logging.error("cannot connect to eucalyptus, no manage cycle")
            return 0

        myMachines = self.getSiteMachines()

        for r in reservations:
            for i in r.instances:
                mach = self.getMachineByEucaId(myMachines, i.id)
                if mach is not None:

                    if i.state == "terminated" and not mach[1].get(
                            self.mr.regStatus) == self.mr.statusDown:
                        self.mr.updateMachineStatus(mach[0], self.mr.statusDown)
                    if i.state == "running" and mach[1].get(
                            self.mr.regStatus) == self.mr.statusBooting:
                        self.transferInstanceData(i, mach[0])
                        if self.checkIfMachineIsUp(mach[0]):
                            self.mr.updateMachineStatus(mach[0], self.mr.statusUp)
                        else:
                            self.checkForDeadMachine(mach[0])

                    if i.state == "shutting-down" and mach[1].get(
                            self.mr.regStatus) == self.mr.statusBooting:
                        self.mr.updateMachineStatus(mach[0], self.mr.statusShutdown)
                else:
                    # is ok, this machine is not managed by us... integrate
                    self.integrateMachine(euca_conn, i)

    def transferInstanceData(self, euca_inst, machine_id):

        self.mr.machines[machine_id][self.mr.regHostname] = euca_inst.public_dns_name
        self.mr.machines[machine_id][self.mr.regInternalIp] = euca_inst.private_dns_name
        self.mr.machines[machine_id][self.mr.regSshKey] = euca_inst.key_name

    def checkIfMachineIsUp(self, mid):
        ssh = ScaleTools.Ssh.getSshOnMachine(self.mr.machines[mid])
        return ssh.canConnect()

    def integrateMachine(self, euca_conn, euca_inst, mtype=None):
        ut = self.getApiUtil()

        try:
            if mtype is None:
                imageName = ut.getImageNameByImageId(euca_conn, euca_inst.image_id)
                machineType = self.getMachineTypeByImageName(imageName)
            else:
                machineType = mtype
        except LookupError:
            logging.info("Cant add euca machine since its image is not configured")
            return

        mid = self.mr.newMachine()

        mconf = self.getConfig(self.ConfigMachines)[machineType]

        self.mr.machines[mid][self.mr.regSite] = self.siteName
        self.mr.machines[mid][self.mr.regSiteType] = self.siteType
        self.mr.machines[mid][self.mr.regMachineType] = machineType
        self.mr.machines[mid][self.reg_site_euca_instance_id] = euca_inst.id

        if mconf.usesGateway:
            self.mr.machines[mid][self.mr.regUsesGateway] = True
            self.mr.machines[mid][self.mr.regGatewayIp] = mconf.gatewayIp
            self.mr.machines[mid][self.mr.regGatewayKey] = mconf.gatewayKey
            self.mr.machines[mid][self.mr.regGatewayUser] = mconf.gatewayUser

        if euca_inst.state == "running":
            # get instance data
            self.transferInstanceData(euca_inst, mid)
            if self.checkIfMachineIsUp(mid):
                self.mr.updateMachineStatus(mid, self.mr.statusWorking)
            else:
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
        if euca_inst.state == "pending":
            self.mr.updateMachineStatus(mid, self.mr.statusBooting)
        if euca_inst.state == "terminated":
            self.mr.updateMachineStatus(mid, self.mr.statusDown)
        if euca_inst.state == "shutting-down":
            self.mr.updateMachineStatus(mid, self.mr.statusShutdown)

    def getMachineTypeByImageName(self, imageName):
        for (mtype, machine) in self.getConfig(self.ConfigMachines).items():
            if machine.imageName == imageName:
                return mtype

        raise LookupError("Machine Image %s is not configured to be used by scale." % imageName)

    def eucaTerminateMachines(self, euca_ids):

        try:
            ut = self.getApiUtil()
            euca_conn = ut.openConnection()
            euca_conn.terminate_instances(euca_ids)
        except boto.exception.EC2ResponseError:
            logging.error("cannot connect to eucalyptus, no machines terminated")
            return 0

    def terminateMachines(self, machineType, count):
        booting = list(self.getSiteMachines(machineType=machineType, status=self.mr.statusBooting))
        working = list(self.getSiteMachines(machineType=machineType, status=self.mr.statusWorking))

        toRemove = booting + working
        toRemove = toRemove[0:count]

        [self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration) for mid in toRemove]

        return len(toRemove)

    def spawnMachines(self, machineType, count):
        if not self.isMachineTypeSupported(machineType):
            raise LookupError("Machine Image %s not supported by this Adapter." % machineType)

        # ensure we dont overstep the site quota
        if not self.getConfig(self.ConfigMaxMachines) is None:
            # returns a dict of machine types
            machineCount = self.cloudOccupyingMachinesCount
            slotsLeft = self.getConfig(self.ConfigMaxMachines) - machineCount

            if slotsLeft < count:
                logging.warning(
                    "Site %s reached MaxMachines, truncating to %s new machines." % (self.siteName, slotsLeft))
                count = max(0, slotsLeft)

        if count == 0:
            return 0
        try:
            ut = self.getApiUtil()
            euca_conn = ut.openConnection()
            machineConf = self.getConfig(self.ConfigMachines)[machineType]
            imgId = ut.getImageIdByImageName(euca_conn, machineConf.imageName)

            if machineConf.securityGroup is None:
                secGroup = []
            else:
                secGroup = [machineConf.securityGroup]

            logging.info("EucaSpawnAdapter: running %s instances of image %s." % (count, machineConf.imageName))
            reservation = euca_conn.run_instances(image_id=imgId,
                                                  min_count=count,
                                                  max_count=count,
                                                  key_name=machineConf.instanceKey,
                                                  security_groups=secGroup,
                                                  user_data=machineConf.userData,
                                                  addressing_type=machineConf.addressingType,
                                                  instance_type=machineConf.instanceType,
                                                  placement=None,
                                                  kernel_id=machineConf.kernelId,
                                                  ramdisk_id=machineConf.ramdiskId)

            for instance in reservation.instances:
                self.integrateMachine(euca_conn, instance, machineType)
        except boto.exception.EC2ResponseError:
            logging.error("cannot connect to eucalyptus, no machines spawned")
            return 0

        # logging.debug( reservation.instances )
        return count

    def getNova(self):
        return

    @property
    def description(self):
        return "EucaSpawnAdapter runs machines inside an eucalyptus cloud"
