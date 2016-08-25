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
import hashlib
import logging
import socket
from xml.dom import minidom

import xmlrpc.client

from Core import MachineRegistry, Config
from SiteAdapter.Site import SiteAdapterBase
from Util import ScaleTools


class OneSiteAdapter(SiteAdapterBase):
    """Site Adapter for OpenNebula. OSA is responsible for spawning new machines and setting
    status from booting to up and from disintegrated to done"""

    ConfigServerProxy = "oneServerProxy"
    ConfigUser = "oneUser"
    ConfigPass = "onePass"
    ConfigUID = "oneUID"

    reg_site_one_vmid = "one_vmid"
    reg_gridengine_node_name = "gridengine_node_name"

    def __init__(self):
        super(OneSiteAdapter, self).__init__()

        #        self.setConfig( self.ConfigUser, open("one_auth","r").read()[0:7] ) #reads in the one user name stored locally in one_auth
        #        self.setConfig( self.ConfigPass, open("one_auth","r").read()[8:18] ) # reads in the one password stored locally in one_auth
        #        self.setConfig( self.ConfigUID,"17" ) # one user ID
        #        self.setConfig( self.ConfigServerProxy, "http://localhost:3000" ) # xmpl-rpc server address. port points via ssh tunnel to http://scchpblade09b.fzk.de:2633/RPC2

        self.addCompulsoryConfigKeys(self.ConfigUser, Config.ConfigTypeString)
        self.addCompulsoryConfigKeys(self.ConfigPass, Config.ConfigTypeString)
        self.addCompulsoryConfigKeys(self.ConfigUID, Config.ConfigTypeString)
        self.addCompulsoryConfigKeys(self.ConfigServerProxy, Config.ConfigTypeString)

        self.hostname_prefix = "cloud-"

        self.reg_site_one_first_dead_check = "site_one_first_dead_check"

    def init(self):
        self.mr.registerListener(self)

    def getProxy(self):
        """helper method which returns xmlrpc proxy instance """

        try:
            return self.proxy
        except AttributeError:
            self.proxy = xmlrpc.client.ServerProxy(self.getConfig(self.ConfigServerProxy))
            return self.proxy

    def VMAllocate(self, template_name, hostname):
        """allocates a new vm in one. corresponds to "onevm create <templatefile>" in shell"""

        template = open(template_name, "r").read().replace("$NAME", hostname)

        try:
            return self.getProxy().one.vm.allocate(self.getOneSessionString(), template)
        except socket.error:
            logging.debug("Failed to connect to ONE RPC server %s!" % self.getConfig(self.ConfigServerProxy))
            return [False]
            # returns [True,VM_ID] or [False]

    def VMAction(self, action, vm_id):
        """implements the action method of the one xmlrpc server.
        actions only work for already running machines!
        see http://opennebula.org/documentation:rel2.0:api for more infos"""

        actions = ["shutdown", "hold", "release", "stop", "cancel",
                   "suspend", "resume", "restart", "finalize"]

        # ToDo: First check existing machines for IDs and state

        vm_action = [False]

        if action in actions:
            info = self.VMInfo(vm_id)
            if info[0] is True:
                if info[1]["UID"] == self.getConfig(self.ConfigUID):  # machine is my machine
                    try:
                        vm_action = self.getProxy().one.vm.action(self.getOneSessionString(),
                                                                  action, vm_id)
                        logging.debug(vm_action)
                        if vm_action[0] is True:
                            logging.info("Action %s for VM ID %s successful!" % (action, vm_id))
                    except socket.error:
                        logging.debug("Failed to connect to ONE RPC server %s!" %
                                      self.getConfig(self.ConfigServerProxy))
                        vm_action[0] = False

        logging.debug(vm_action)
        return vm_action

    def ParseVmInfo(self, response):

        info_elements = ["ID", "UID", "NAME", "STATE", "LCM_STATE", "IP_PUBLIC", "MAC"]
        info = dict()
        xmldoc = minidom.parseString(response[1])

        for element in info_elements:
            data = xmldoc.getElementsByTagName(element)[0].firstChild.data
            info[element] = str(data)

        return info

    def VMInfo(self, vm_id):
        """implements the info method of the one xmlrpc server and returns already parsed
        info as a dictionary
        see http://opennebula.org/documentation:rel2.0:api for more infos"""

        try:
            vm_info = self.getProxy().one.vm.info(self.getOneSessionString(), vm_id)
        except socket.error:
            logging.debug("Failed to connect to ONE RPC server %s!" % self.getConfig(self.ConfigServerProxy))
            vm_info[0] = False

        if vm_info[0] is True:
            info = [True, self.ParseVmInfo(vm_info)]
        else:
            info = [False]

        return info

    def ParseVMPoolInfo(self, vm_pool_info):

        xmldoc = minidom.parseString(vm_pool_info)
        number_machines = len(xmldoc.getElementsByTagName("VM"))

        info_elements = ["ID", "UID", "NAME", "STATE", "LCM_STATE"]
        parsed_pool_info = []

        for machine in range(0, number_machines):
            info = dict()
            for element in info_elements:
                data = xmldoc.getElementsByTagName(element)[machine].firstChild.data
                info[element] = str(data)

            # vm.pool.info doesn't contain "IP_PUBLIC" and "MAC" in its response and have to be obtained via the vm.info method
            vm_info = self.VMInfo(int(info["ID"]))

            if vm_info[0] is True:
                info["IP_PUBLIC"] = vm_info[1]["IP_PUBLIC"]
                info["MAC"] = vm_info[1]["MAC"]

            parsed_pool_info.append(info)

        # print parsed_pool_info
        # [{'NAME': 'ubuntu_server', 'MAC': '02:00:8d:34:d0:a6', 'STATE': '3', 'IP_PUBLIC': '141.52.208.166', 'LCM_STATE': '3', 'ID': '863', 'UID': '14'}, {'NAME': 'ubuntu_server', 'MAC': '02:00:8d:34:d0:bd', 'STATE': '3', 'IP_PUBLIC': '141.52.208.189', 'LCM_STATE': '3', 'ID': '1129', 'UID': '14'}]
        return parsed_pool_info

    def VMPoolInfo(self, filter_flag=-1, extended_info=True, state=-1):
        """implements the vmpool info method of the one xmlrpc server
        see http://opennebula.org/documentation:rel2.0:api for more infos"""

        try:
            vm_pool_info = self.getProxy().one.vmpool.info(self.getOneSessionString(), filter_flag,
                                                           extended_info,
                                                           state)
        except socket.error:
            logging.debug("Failed to connect to ONE RPC server %s!" % self.getConfig(self.ConfigServerProxy))

        if vm_pool_info[0] is True:
            info = [True, self.ParseVMPoolInfo(vm_pool_info[1])]
        else:
            info = [False]

        return info

    def checkIfMachineIsUp(self, mid):
        ssh = ScaleTools.Ssh.getSshOnMachine(self.mr.machines[mid])
        return ssh.canConnect()

    def getOneSessionString(self):
        """one session string used for authentication by the xmlrpc server"""

        hash_ = hashlib.sha1()
        hash_.update(self.getConfig(self.ConfigPass))

        return self.getConfig(self.ConfigUser) + ":" + hash_.hexdigest()

    def checkForDeadMachine(self, mid):
        logging.info("Machine %s is running but no ssh connect yet." % mid)
        firstCheck = self.mr.machines[mid].get(self.reg_site_one_first_dead_check, None)

        if firstCheck is None:
            self.mr.machines[mid][self.reg_site_one_first_dead_check] = datetime.datetime.now()
        else:
            if (datetime.datetime.now() - firstCheck).total_seconds() > self.getConfig(
                    self.ConfigMachineBootTimeout):
                logging.warning("Machine %s did not boot in time. Shutting down." % mid)
                self.mr.updateMachineStatus(mid, self.mr.statusDisintegrated)

    def manage(self, cleanup=False):
        """manage method is called every manage cycle.
        this is the right place to survey the booting status and set it to StatusUp if machine is up"""

        """
        logging.info("Querying OpenNebula Server for running instances...")
        
        vm_pool = self.VMPoolInfo()
        print vm_pool
        
        running_instances = filter(lambda x: self.hostname_prefix in x[1]["NAME"], vm_pool[1:])
        print running_instances
        
        running_instances2 = (filter(lambda x: "ubuntu" in x[1]["NAME"], vm_pool[1:]))[0]
        print running_instances2
        """

        myMachines = self.getSiteMachines()

        # print myMachines
        # {'8e661aac-fc4e-450f-9cbb-e57ec6e4adb2': {'status': 'working', 'site_type': 'one', 'hostname': '141.52.208.174', 'ssh_key': 'one_host_key', 'one_vmid': 910, 'status_last_update': datetime.datetime(2011, 1, 13, 15, 48, 39, 328084), 'machine_type': 'euca-default', 'site': 'one_site_scc'}}

        for mid in myMachines:
            if myMachines[mid]["status"] == "booting":
                vm_info = self.VMInfo(myMachines[mid]["one_vmid"])
                logging.debug(myMachines[mid]["vpn_ip"])
                logging.debug(myMachines[mid]["vpn_cert_is_valid"])
                logging.debug(myMachines[mid]["vpn_cert"])
                if vm_info[0] is True:

                    if vm_info[1]["STATE"] == "3" and vm_info[1]["LCM_STATE"] == "3":
                        if self.checkIfMachineIsUp(mid):
                            vpn = ScaleTools.Vpn()

                            if myMachines[mid]["vpn_cert_is_valid"] is None:
                                if vpn.makeCertificate(myMachines[mid]["vpn_cert"]) == 0:
                                    myMachines[mid]["vpn_cert_is_valid"] = True

                            if myMachines[mid]["vpn_cert_is_valid"] is True and \
                                            myMachines[mid]["vpn_ip"] is None:
                                if (vpn.copyCertificate(myMachines[mid]["vpn_cert"],
                                                        myMachines[mid]) == 0):
                                    if (vpn.connectVPN(myMachines[mid]["vpn_cert"],
                                                       myMachines[mid]) == 0):
                                        (res, ip) = vpn.getIP(myMachines[mid])
                                        logging.debug(res)
                                        logging.debug(ip)
                                        if res == 0 and ip != "":
                                            myMachines[mid]["vpn_ip"] = ip
                                        else:
                                            logging.debug("getting VPN IP failed!!")

                            if (myMachines[mid]["vpn_cert_is_valid"] is True and myMachines[mid][
                                "vpn_ip"] is not None):
                                # if( vpn.revokeCertificate(myMachines[k]["vpn_cert"]) == 0):
                                #    myMachines[k]["vpn_cert_is_valid"] = False
                                self.mr.updateMachineStatus(mid, self.mr.statusUp)

                            logging.debug(myMachines[mid]["vpn_ip"])
                            logging.debug(myMachines[mid]["vpn_cert_is_valid"])
                            logging.debug(myMachines[mid]["vpn_cert"])

                        else:
                            self.checkForDeadMachine(mid)

    """
        Possible VM States @ ONE:
        "STATE":
            INIT = 0, PENDING = 1, HOLD = 2, ACTIVE = 3, STOPPED = 4, SUSPENDED = 5, DONE = 6, FAILED = 7
        "LCM_STATE":
            LCM_INIT = 0, PROLOG = 1, BOOT = 2, RUNNING = 3, MIGRATE = 4, SAVE_STOP = 5, SAVE_SUSPEND = 6,
            SAVE_MIGRATE = 7, PROLOG_MIGRATE = 8, PROLOG_RESUME = 9, EPILOG_STOP = 10, EPILOG = 11,
            SHUTDOWN = 12, CANCEL = 13, FAILURE = 14, DELETE = 15, UNKNOWN = 16
    """

    def spawnMachines(self, machineType, count):
        """spawns machines by calling VMAllocate and registering new VMs in machine registry"""

        for k in range(0, count):
            mid = self.mr.newMachine()
            node_name = self.hostname_prefix + mid

            machineConfigs = self.getConfig(self.ConfigMachines)
            info = self.VMAllocate(machineConfigs[machineType], node_name)

            if info[0] is True:
                # mid = self.mr.newMachine()
                self.mr.machines[mid][self.mr.regSite] = self.siteName
                self.mr.machines[mid][self.mr.regSiteType] = self.siteType
                self.mr.machines[mid][self.mr.regMachineType] = machineType
                self.mr.machines[mid][self.reg_site_one_vmid] = info[1]  # ONE VM ID
                self.mr.machines[mid][self.reg_gridengine_node_name] = node_name
                # hostname = ip - if you use scale tools you better not mix up the definitions otherwise ssh can't connect for instance
                self.mr.machines[mid][self.mr.regHostname] = self.VMInfo(info[1])[1]["IP_PUBLIC"]
                self.mr.machines[mid][self.mr.regSshKey] = "one_host_key"
                self.mr.updateMachineStatus(mid, self.mr.statusBooting)
                self.mr.machines[mid][self.mr.regVpnCert] = node_name
                self.mr.machines[mid][self.mr.regVpnCertIsValid] = None
                self.mr.machines[mid][self.mr.regVpnIp] = None
            else:
                self.mr.removeMachine(mid)

        return count

    def terminateMachines(self, machineType, count):
        """kill <count> machines of type <machineType>"""
        toRemove = [mid for (mid, machine)
                    in self.mr.getMachines(site=self.siteName, machineType=machineType)
                    if machine[self.mr.regStatus] in [self.mr.statusWorking, self.mr.statusBooting]]

        toRemove = toRemove[0:count]

        [self.mr.updateMachineStatus(mid, self.mr.statusPendingDisintegration)
         for mid in toRemove]

        return len(toRemove)

    def onEvent(self, evt):
        """triggered when event appears, independent of manage cycle"""

        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            if self.mr.machines[evt.id].get(self.mr.regSite) == self.siteName:
                # check correct site etc...
                if evt.newStatus == self.mr.statusDisintegrated:
                    # print int(self.mr.machines[evt.id].get(self.reg_site_one_vmid))
                    vm_action = self.VMAction("cancel", int(
                        self.mr.machines[evt.id].get(self.reg_site_one_vmid)))
                    if vm_action[0] is True:
                        self.mr.updateMachineStatus(evt.id, self.mr.statusDown)

    def isMachineTypeSupported(self, machineType):
        return True

    @property
    def description(self):
        return "OneSiteAdapter"
