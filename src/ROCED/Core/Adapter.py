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
from __future__ import print_function, unicode_literals

import abc
import json
import logging

import xmlrpc.server

from . import Config


class NoDefaultSet(object):
    def __init__(self):
        pass


class AdapterBase(object):
    """
    Contains a list of ConfigKeys which must not be published outside
    the application borders, for example the REST API
    """
    __metaclass__ = abc.ABCMeta

    def getOptionalConfigKeys(self):
        return self.configKeysToLoadOptional

    def getCompulsoryConfigKeys(self):
        return self.configKeysToLoad

    def addOptionalConfigKeys(self, key, datatype, description=NoDefaultSet(),
                              default=NoDefaultSet()):
        self.configKeysToLoadOptional += [(key, datatype, default)]

    def addCompulsoryConfigKeys(self, key, datatype, description=None):
        self.configKeysToLoad += [(key, datatype, NoDefaultSet())]

    @property
    def configDict(self):
        return self._configDict

    @configDict.setter
    def configDict(self, dict_):
        self._configDict = dict_

    def applyConfigDict(self, newConfig):
        self._configDict.update(newConfig)

    # returns the Configuration containing only dicts
    def getConfigAsDict(self, onlyPublic=False):
        strippedConf = {}

        for (k, v) in self._configDict.items():
            if k not in self.privateConfig:
                strippedConf[k] = v

        return strippedConf

    # Methods    
    def getConfig(self, key):
        return self._configDict.get(key, None)

    def setConfig(self, key, value):
        self._configDict[key] = value

    def __init__(self):
        self._configDict = dict()

        # config keys whose values MUST be set before starting
        # format ( keyname, type )
        self.configKeysToLoad = []
        # config keys whose values CAN be set before staring
        self.configKeysToLoadOptional = []

        self.privateConfig = []

    def init(self):
        pass

    def terminate(self):
        pass

    @property
    @abc.abstractmethod
    def description(self):
        return "AdapterBase"

    def manage(self):
        pass

    _rpcServer = None

    def exportMethod(self, meth, name):
        if self._rpcServer is not None:
            self._rpcServer.register_function(meth, name)
        else:
            logging.warning("Can't register method " + name + ". RPCServer not set.")

    def loadConfigValue(self, key_list, configuration, optional, section, new_obj):
        for (config_key, config_type, opt_val) in key_list:
            if not configuration.has_option(section, config_key) and optional:
                if isinstance(opt_val, NoDefaultSet):
                    logging.error(
                        "Config key " + config_key + " not defined and no default value set")
                    exit(0)
                else:
                    val = opt_val
            else:
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
                    print("Config data type " + config_type + " not supported")
                    exit(0)

            new_obj.setConfig(config_key, val)


class AdapterBoxBase(object):
    @property
    def rpcServer(self):
        return self._rpcServer

    @rpcServer.setter
    def rpcServer(self, server):
        self._rpcServer = xmlrpc.server.ServerProxy(server)

    @property
    def adapterList(self):
        return self._adapterList

    @property
    def content(self):
        con = ""
        for adapter in self._adapterList:
            con += adapter.description + "\n"
        return con

    def __init__(self):
        self._adapterList = []
        self._rpcServer = None
        # self.rpcServer = "https://localhost:8000"

    def addAdapter(self, a):
        self._adapterList.append(a)

    def addAdapterList(self, alist):
        self._adapterList += alist

    def manage(self):
        [adapter.manage() for adapter in self._adapterList]
