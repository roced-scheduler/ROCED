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
from __future__ import unicode_literals, absolute_import, print_function

"""
This file contains the ScaleCore class which contains
all module objects and runs the SiteBroker to handle
Cloud utilization.
"""

import importlib
import logging

from datetime import datetime
from threading import Timer

from . import Broker
from . import Config
from . import MachineRegistry
from IntegrationAdapter.Integration import IntegrationBox
from RequirementAdapter.Requirement import RequirementBox
from SiteAdapter.Site import SiteBox
from Util.Logging import JsonLog, MachineRegistryLogger
from Util.PythonTools import summarize_dicts

logger = logging.getLogger("Core")


class MachineStatus(object):
    def __init__(self, required=0, actual=0):
        self.required = required
        self.actual = actual


class ScaleCore(object):
    _rpcServer = None

    def exportMethod(self, meth, name):
        if self._rpcServer is not None:
            self._rpcServer.register_function(meth, name)
        else:
            logger.warning("Can't register method %s with RPCServer \"%s\"." % (name, self._rpcServer))

    def __init__(self,
                 broker,
                 rpcServer,
                 reqAdapterList,
                 siteAdapterList,
                 intAdapterList,
                 autoRun=True,
                 maximumManageIterations=None):
        """
        Main core object which knows adapters, brokers and calls SiteBroker.

        Contains all adapter boxes & broker objects.
        SiteBroker decides on and issues new orders to the site adapter(s).
        """
        self.broker = broker
        self.autoRun = autoRun
        self.manageInterval = 30
        # will count the number of iterations that have been executed
        self.manageIterations = 0
        self.maximumManageIterations = maximumManageIterations
        self.mr = MachineRegistry.MachineRegistry()
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

    def startManagementTimer(self):
        t = Timer(self.manageInterval, self.startManage)
        t.start()

    def init(self):
        # self.exportMethod(self.setMachineTypeMaxInstances, "setMachineTypeMaxInstances")
        self.mr.machines = MachineRegistryLogger.load()

    def startManage(self):
        logger.info("----------------------------------")
        logger.info("Management cycle triggered")
        logger.info("Time: %s" % datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

        # regular management
        self.reqBox.manage()
        self.siteBox.manage()
        self.intBox.manage()

        # scaling
        mReq = self.reqBox.getMachineTypeRequirement()
        logger.info("Current requirement: %s" % mReq)

        siteInfo = self.siteBox.siteInformation
        runningBySite = self.siteBox.runningMachinesCount

        # contains a list of all machine types merged
        runningOverall = summarize_dicts(list(runningBySite.values()))

        machStat = dict()
        for (key_, value_) in runningOverall.items():
            machStat[key_] = MachineStatus(mReq.get(key_, 0), value_)

        for key_ in mReq:
            if not key_ in machStat:
                machStat[key_] = MachineStatus(mReq.get(key_, 0), 0)

        decision = self.broker.decide(machStat, siteInfo.values())

        # Service machines may modify site decision(s).
        decision = self.siteBox.modServiceMachineDecision(decision)

        logger.info("Decision: %s" % decision)

        # make machine counts absolute, as they come in relative from the broker
        for (ksite, vmach) in decision.items():
            for kmach in vmach:
                decision[ksite][kmach] += runningBySite[ksite].get(kmach, [])
        logger.info("Absolute Decision: %s" % decision)

        self.siteBox.applyMachineDecision(decision)

        logger.info(self.mr.getMachineOverview())

        MachineRegistryLogger.dump(self.mr.machines)

        log = JsonLog()
        log.writeLog()

        self.manageIterations += 1

        lastIteration = False
        if self.maximumManageIterations is not None:
            lastIteration = self.maximumManageIterations <= self.manageIterations

        if self.autoRun is True and lastIteration is False:
            self.startManagementTimer()

    @property
    def description(self):
        return "Scale Core 0.2"


class ObjectFactory(object):
    __packages = {Config.GeneralReqAdapters: "RequirementAdapter",
                  Config.GeneralIntAdapters: "IntegrationAdapter",
                  Config.GeneralSiteAdapters: "SiteAdapter"}

    @classmethod
    def getObject(cls, className, adapterType=None):
        """
        Dynamically load module(s) and instantiate object(s).

        The config file contains all necessary information which adapters
        are _really_ required for the current execution.
        This method loads the module with the help of importlib and instantiates
        a single object which is returned to the caller.

        :param className:
        :param adapterType:
        :return:
        """
        importName = cls.__packages[adapterType].__str__() + "." + className.__str__()
        module_ = importlib.import_module(name=importName)

        try:
            class_ = getattr(module_, className)()
            return class_
        except AttributeError:
            logging.error("Class %s does not exist" % className)


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

        # TODO: Get rid of hard-coded StupidBroker
        if broker_type == "Broker.StupidBroker":
            return Broker.StupidBroker()
        else:
            raise Exception("Broker type %s not supported" % broker_type)

    def getReqAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralReqAdapters, configuration)

    def getSiteAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralSiteAdapters, configuration)

    def getIntAdapterList(self, configuration):
        return self.getAdapterList(Config.GeneralIntAdapters, configuration)

    def getAdapterList(self, adapter_type, configuration):

        adapters = []

        for adapter in configuration.get(Config.GeneralSection, adapter_type).split():
            if adapter == "None":
                break
            site_type = configuration.get(adapter, Config.ConfigObjectType)
            try:
                obj = ObjectFactory.getObject(className=site_type, adapterType=adapter_type)
            except ImportError:
                obj = None
            if obj is None:
                raise Exception("Adapter type %s not found" % site_type)

            # transfer compulsory config
            obj.loadConfigValue(obj.getCompulsoryConfigKeys(), configuration, False, adapter, obj)
            # transfer optional config
            obj.loadConfigValue(obj.getOptionalConfigKeys(), configuration, True, adapter, obj)

            adapters += [obj]

        return adapters
