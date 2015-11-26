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

"""
This file contains the ScaleCore class which contains
all module objects and runs the SiteBroker to handle
Cloud utilization.
"""

import json
import logging
from threading import Timer
import shutil
from datetime import datetime
import simplejson

from IntegrationAdapter.Integration import IntegrationBox
from IntegrationAdapter.FakeIntegrationAdapter import FakeIntegrationAdapter
from IntegrationAdapter.HTCondorIntegrationAdapter import HTCondorIntegrationAdapter
from RequirementAdapter.Requirement import RequirementBox
from RequirementAdapter.FakeRequirementAdapter import FakeRequirementAdapter
from RequirementAdapter.HTCondorRequirementAdapter import HTCondorRequirementAdapter
from SiteAdapter.Site import SiteBox  # , EucaSiteAdapter, Ec2SiteAdapter,
from SiteAdapter.OpenStackSiteAdapter import OpenStackSiteAdapter
from SiteAdapter.FreiburgSiteAdapter import FreiburgSiteAdapter
from SiteAdapter.FakeSiteAdapter import FakeSiteAdapter
import Broker
import Config
import MachineRegistry
from Util.ScaleTools import JsonLog

logger = logging.getLogger('Core')


class MachineStatus(object):
    def __init__(self, required=0, actual=0):
        self.required = required
        self.actual = actual

    def required():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._required

        def fset(self, value):
            self._required = value

        def fdel(self):
            del self._required

        return locals()

    required = property(**required())

    def actual():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._actual

        def fset(self, value):
            self._actual = value

        def fdel(self):
            del self._actual

        return locals()

    actual = property(**actual())

    # baseline, max, priority


class ScaleCore(object):
    '''
    Contains all Adpater Boxes and the Broker object and
    calls SiteBrokerBase. Decides and issues the new site orders to
    the site adapters. 
    '''

    def broker():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._broker

        def fset(self, value):
            self._broker = value

        def fdel(self):
            del self._broker

        return locals()

    broker = property(**broker())

    def manageInterval():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._manageInterval

        def fset(self, value):
            self._manageInterval = value

        def fdel(self):
            del self._manageInterval

        return locals()

    manageInterval = property(**manageInterval())

    def reqBox():  # @NoSelf
        """Contains the RequirementBox which holds all available RequirementAdapter"""

        def fget(self):
            return self._reqBox

        def fset(self, value):
            self._reqBox = value

        def fdel(self):
            del self._reqBox

        return locals()

    reqBox = property(**reqBox())

    def autoRun():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._autoRun

        def fset(self, value):
            self._autoRun = value

        def fdel(self):
            del self._autoRun

        return locals()

    autoRun = property(**autoRun())

    def siteBox():  # @NoSelf
        """Contains the SiteBox which holds all available SiteAdapter"""

        def fget(self):
            return self._siteBox

        def fset(self, value):
            self._siteBox = value

        def fdel(self):
            del self._siteBox

        return locals()

    siteBox = property(**siteBox())

    def intBox():  # @NoSelf
        """Contains the IntegrationBox which holds all available IntegrationAdapter"""

        def fget(self):
            return self._intBox

        def fset(self, value):
            self._intBox = value

        def fdel(self):
            del self._intBox

        return locals()

    intBox = property(**intBox())

    _rpcServer = None

    def exportMethod(self, meth, name):
        if not self._rpcServer == None:
            self._rpcServer.register_function(meth, name)
        else:
            logger.warn("Can't register method " + name + " with rpc, self._rpcServer not set")

    def __init__(self,
                 broker,
                 rpcServer,
                 reqAdapterList,
                 siteAdapterList,
                 intAdapterList,
                 autoRun=True,
                 maximumManageIterations=None):

        self.broker = broker
        self.autoRun = autoRun
        self.manageInterval = 30
        # will count the number of iterations that have been executed
        self.manageIterations = 0
        self.maximumManageIterations = maximumManageIterations
        self.mr = MachineRegistry.MachineRegistry()
        '''
        Constructor
        '''
        self._rpcServer = rpcServer
        # self._rpcServer.register_function(self.getDescription,"ScaleCore_getDescription" )

        # REQ
        self.reqBox = RequirementBox()

        for a in reqAdapterList:
            a._rpcServer = self._rpcServer
            a.init()

        self.reqBox.addAdapterList(reqAdapterList)

        # SITE
        self.siteBox = SiteBox()

        for a in siteAdapterList:
            a._rpcServer = self._rpcServer
            a.init()

        self.siteBox.addAdapterList(siteAdapterList)

        # INTEGRATION
        self.intBox = IntegrationBox()

        for a in intAdapterList:
            a._rpcServer = self._rpcServer
            a.init()

        self.intBox.addAdapterList(intAdapterList)

    def toJson(self, python_object):
        """function to write not serializeable objects in a json file.
        set default=toJson in json.dump()
        must be adapted for other types!
        """

        if isinstance(python_object, datetime):
            return {"__class__": "datetime.datetime", \
                    "__value__": python_object.strftime("%Y-%m-%d %H:%M:%S:%f")}
            raise TypeError(repr(python_object) + ' is not JSON serializable')

    def fromJson(self, json_object):
        """function to read not serializeable objects from a json file.
        set object_hook=fromJson in json.load()
        must be adapted for other types!
        """

        if "__class__" in json_object:
            if json_object["__class__"] == "datetime.datetime":
                return datetime.strptime(json_object["__value__"], "%Y-%m-%d %H:%M:%S:%f")
        return json_object

    def dumpState(self):
        """dumps the current machine registry to a json file.
        called in Core.startManage()
        """

        try:
            shutil.move("log/machine_registry.json", "log/old_machine_registry.json")
        except IOError:
            logger.warning("Json file could not be moved!")

        # print self.mr.machines

        try:
            file = open("log/machine_registry.json", "w")
            simplejson.dump(self.mr.machines, file, default=self.toJson)
            file.close()
        except IOError:
            logger.error("json file could not be opened for dumping state!")

    def loadState(self):
        """loads the last state from the json file in the machine registry.
        called in Core.init()
        """

        try:
            file = open("log/machine_registry.json", "r")
            state = simplejson.load(file, object_hook=self.fromJson)
            file.close()

            self.mr.machines = state

        except IOError:
            logger.error("json file could not be opened for loading state!")

        logger.info("Previous state loaded!")

    def startManagementTimer(self):
        t = Timer(self.manageInterval, self.startManage)
        t.start()

    def init(self):
        # self.exportMethod(self.setMachineTypeMaxInstances,
        #                  "setMachineTypeMaxInstances")
        self.loadState()

    def startManage(self):
        logger.info("----------------------------------")
        logger.info("Management cycle triggered")
        logger.info("Time: " + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))

        # regular management

        self.reqBox.manage()
        self.siteBox.manage()
        self.intBox.manage()

        # scaling
        req = self.reqBox.getMachineTypeRequirement()
        logger.info("Current requirement: " + str(req))

        siteInfo = self.siteBox.getSiteInformation()
        runningBySite = self.siteBox.getRunningMachinesCount()

        def mergeDicts(x, y):
            for (k, v) in y.iteritems():
                if k in x:
                    x[k] += v
                else:
                    x[k] = v
            return x

        # contains a list of all machine types merged
        runningOverall = reduce(mergeDicts, runningBySite.values(), dict())

        machStat = dict()
        for (k, v) in runningOverall.iteritems():
            machStat[k] = MachineStatus(req.get(k, 0), v)

        for k in req:
            if not k in machStat:
                machStat[k] = MachineStatus(req.get(k, 0), 0)

        # logger.info("MachineStatus: " + str(machStat))

        decision = self.broker.decide(machStat, siteInfo.values())
        logger.info("Decision: " + str(decision))

        # make the machine counts absolute, as they come in relative from the broker
        for (ksite, vmach) in decision.iteritems():
            for kmach in vmach:
                decision[ksite][kmach] += runningBySite[ksite].get(kmach, [])

        logger.info("Absolute Decision: " + str(decision))
        self.siteBox.applyMachineDecision(decision)

        mr = MachineRegistry.MachineRegistry()
        logger.info(str(mr.getMachineOverview()))

        self.dumpState()

        log = JsonLog()
        log.writeLog()

        self.manageIterations += 1

        lastIteration = False
        if not self.maximumManageIterations is None:
            lastIteration = self.maximumManageIterations <= self.manageIterations

        if self.autoRun == True and not lastIteration:
            self.startManagementTimer()

    def getDescription(self):
        return "Scale Core 0.2"


class ObjectFactory(object):
    @staticmethod
    def getObject(classname):
        obj = globals()[classname]
        return obj()


class ScaleCoreFactory(object):
    def getCore(self, configuration, maximumInterval=None):

        interval = 60  # one minute is default

        if configuration.has_option(Config.GeneralSection, Config.GeneralManagementInterval):
            interval = configuration.getint(Config.GeneralSection, Config.GeneralManagementInterval)

        sc = ScaleCore(self.getBroker(configuration),
                       None,
                       self.getReqAdapterList(configuration),
                       self.getSiteAdapterList(configuration),
                       self.getIntAdapterList(configuration),
                       autoRun=True,
                       maximumManageIterations=maximumInterval)

        sc.manageInterval = interval

        return sc

    def getBroker(self, configuration):

        # get the broker name
        broker_name = configuration.get(Config.GeneralSection, Config.GeneralBroker)

        # get broker type 
        broker_type = configuration.get(broker_name, Config.ConfigObjectType)

        if broker_type == "Broker.StupidBroker":
            return Broker.StupidBroker()
        else:
            raise Exception("Broker type " + broker_type + " not supported")

    def getReqAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralReqAdapters, configuration)

    def getSiteAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralSiteAdapters, configuration)

    def getIntAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralIntAdapters, configuration)

    def getAdapterList(self, adapter_type, configuration):

        site_adapters = []

        for sa in configuration.get(Config.GeneralSection, adapter_type).split():

            site_type = configuration.get(sa, Config.ConfigObjectType)
            obj = ObjectFactory.getObject(site_type)

            if obj == None:
                raise Exception("SiteAdapter type " + site_type + " not found")

            # transfer compulsary config
            self.loadConfigValue(obj.getCompulsoryConfigKeys(), configuration, False, sa, obj)
            # transfer optional config
            self.loadConfigValue(obj.getOptionalConfigKeys(), configuration, True, sa, obj)

            site_adapters += [obj]

        return site_adapters

    def loadConfigValue(self, key_list, configuration, optional, section, new_obj):

        for (config_key, config_type) in key_list:
            if optional:
                if not configuration.has_option(section, config_key):
                    continue

            if config_type == Config.ConfigTypeString:
                val = configuration.get(section, config_key)
            elif config_type == Config.ConfigTypeInt:
                val = configuration.getint(section, config_key)
            elif config_type == Config.ConfigTypeFloat:
                val = configuration.getfloat(section, config_key)
            elif config_type == Config.ConfigTypeBoolean:
                val = configuration.getboolean(section, config_key)
            elif config_type == Config.ConfigTypeDictionary:
                val = json.loads(configuration.get(section, config_key))
            else:
                print "Config data type " + config_type + " not supported"
                exit(0)

            new_obj.setConfig(config_key, val)
