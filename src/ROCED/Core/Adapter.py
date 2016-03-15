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


import abc
import logging
import Config
import json


class NoDefaultSet:
    def __init__(self):
        pass


class AdapterBase(object):
    # Properties

    """
    Contains a list of ConfigKeys which must not be published outside
    the application borders, for example the REST API
    """
    __metaclass__ = abc.ABCMeta

    def privateConfig():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._privateConfig

        def fset(self, value):
            self._privateConfig = value

        def fdel(self):
            del self._privateConfig

        return locals()

    privateConfig = property(**privateConfig())

    def getOptionalConfigKeys(self):
        return self.configKeysToLoadOptional

    def getCompulsoryConfigKeys(self):
        return self.configKeysToLoad

    def addOptionalConfigKeys(self, key, datatype, description=NoDefaultSet(), default=NoDefaultSet()):
        self.configKeysToLoadOptional += [(key, datatype, default)]

    def addCompulsoryConfigKeys(self, key, datatype, description=None):
        self.configKeysToLoad += [(key, datatype, NoDefaultSet())]

    def get_configDict(self):
        return self._configDict

    def set_configDict(self, r):
        self._configDict = r

    configDict = property(get_configDict, set_configDict)

    def applyConfigDict(self, newConfig):
        self.configDict.update(newConfig)

    # returns the Configuration containing only dicts
    def getConfigAsDict(self, onlyPublic=False):
        strippedConf = {}

        for (k, v) in self.configDict.iteritems():
            if not k in self.privateConfig:
                strippedConf[k] = v

        return strippedConf

    # Methods    
    def getConfig(self, key):
        return self.configDict.get(key, None)

    def setConfig(self, key, value):
        self.configDict[key] = value

    def __init__(self):
        self.configDict = dict()

        # config keys whose values MUST be set before starting
        # format ( keyname, type )
        self.configKeysToLoad = []
        # config keys whose valuese CAN be set before staring
        self.configKeysToLoadOptional = []

        self.privateConfig = []

    def init(self):
        pass

    def terminate(self):
        pass

    @abc.abstractmethod
    def getDescription(self):
        return "AdapterBase"

    def manage(self):
        pass

    _rpcServer = None

    def exportMethod(self, meth, name):
        if not self._rpcServer == None:
            self._rpcServer.register_function(meth, name)
        else:
            logging.warn("Can't register method " + name + " with rpc, _rpcServer not set")

    def loadConfigValue(self, key_list, configuration, optional, section, new_obj):

        for (config_key, config_type, opt_val) in key_list:
            if not configuration.has_option(section, config_key) and optional:
                if isinstance(opt_val, NoDefaultSet):
                    logger.error("Config key " + config_key + " not defined and no default value set")
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
                    print "Config data type " + config_type + " not supported"
                    exit(0)

            new_obj.setConfig(config_key, val)

        """
        old code fragment - remove when new one works
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
        """


class AdapterBoxBase(object):
    ''' Properties '''

    def get_rpcServer(self):
        return self._rpcServer

    def set_rpcServer(self, r):
        self._rpcServer = r

    rpcServer = property(get_rpcServer, set_rpcServer)

    def get_adapterList(self):
        return self._adapterList

    def set_adapterList(self, r):
        self._adapterList = r

    adapterList = property(get_adapterList, set_adapterList)

    def __init__(self):
        self.adapterList = []

    def addAdapter(self, a):
        self.adapterList.append(a)

    def manage(self):
        map(lambda x: x.manage(), self.adapterList)

    def addAdapterList(self, alist):
        self.adapterList += alist

    def exportMethod(self, meth, name):
        if not self._rpcServer == None:
            self._rpcServer.register_function(meth, name)
        else:
            logging.warn("Can't register method " + name + " with rpc, self._rpcServer not set")

    def getBoxContent(self):
        con = ""
        for a in self.adapterList:
            con += a.getDescription() + "\n"
        return con
