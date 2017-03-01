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
from __future__ import print_function, unicode_literals, absolute_import

import abc
import json
import logging

from .MachineRegistry import MachineRegistry

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

    # List of "responsible" machine states by adapter(s):
    integration_states = frozenset((MachineRegistry.statusUp, MachineRegistry.statusIntegrating,
                                    MachineRegistry.statusWorking, MachineRegistry.statusPendingDisintegration,
                                    MachineRegistry.statusDisintegrating))
    site_states = frozenset((MachineRegistry.statusBooting, MachineRegistry.statusDisintegrated,
                             MachineRegistry.statusDown))

    @property
    def optionalConfigKeys(self):
        return self.configKeysToLoadOptional

    @property
    def compulsoryConfigKeys(self):
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

    def getConfigAsDict(self, onlyPublic=False):
        # type: (bool) -> dict
        """Returns the (complete) configuration as a dictionary."""
        return {key: self._configDict[key] for key in self._configDict if key not in self.privateConfig}

    # Methods    
    def getConfig(self, key):
        """Get a single configuration value."""
        return self._configDict.get(key, None)

    def setConfig(self, key, value):
        """Set a single configuration value."""
        self._configDict[key] = value

    def __init__(self):
        """Abstract base adapter."""
        self._configDict = dict()

        # config keys whose values MUST be set before starting
        # format ( keyname, type )
        self.configKeysToLoad = []
        # config keys whose values CAN be set before staring
        self.configKeysToLoadOptional = []
        self.privateConfig = []

    def init(self):
        """Delayed __Init__(). Code which depends on configuration being imported."""
        pass

    def terminate(self):
        pass

    @property
    @abc.abstractmethod
    def description(self):
        return "AdapterBase"

    _rpcServer = None

    def exportMethod(self, meth, name):
        if self._rpcServer is not None:
            self._rpcServer.register_function(meth, name)
        else:
            logging.warning("Can't register method %s. RPCServer not set." % name)

    def loadConfigValue(self, key_list, configuration, optional, section, new_obj):
        for (config_key, config_type, opt_val) in key_list:
            if not configuration.has_option(section, config_key) and optional:
                if isinstance(opt_val, NoDefaultSet):
                    logging.error("Config key %s not defined and no default value set." % config_key)
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
                elif config_type == Config.ConfigTypeList:
                    val = json.loads(configuration.get(section, config_key))
                else:
                    print("Config data type %s not supported." % config_type)
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
        # type: (AdapterBase) -> None
        self._adapterList.append(a)

    def addAdapterList(self, alist):
        # type: (List[AdapterBase]) -> None
        self._adapterList += alist

    def manage(self, cleanup=False):
        # type: (bool) -> None
        """ Call contained adapters' (periodic) manage function.

        Every tenth call is considered a "big" cleanup management, appropriate for more time consuming operations.
        """
        [adapter.manage(cleanup) for adapter in self._adapterList]
