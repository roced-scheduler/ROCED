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

import abc
import logging

from datetime import datetime


class SiteBrokerBase(object):
    """
    Abstract class for SiteBrokers. SiteBrokers (de-)allocate cloud resources.

    Implementations must inherit from this class.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def decide(self, machineTypes, siteInfo):
        """
        Concrete implementaions of a SiteBroker must implement this method to specify the
        behaviour of the class.
        
        machineTypes: a dictionary with the machine type (string) as key and a MachineStatus object
                      as value
        siteInfo: a dictionary with the site name (string) as key and a SiteInformation object
                  as value
                  
        return: contanis 2 nested dics: [siteName][machineName] = the Delta of machines on siteName
                [siteName][machineName] = 0  -> no change on this site
                [siteName][machineName] = -3  -> shutdown 3 machines on this site
        """


class StupidBroker(SiteBrokerBase):
    """
    This class implements a simple cloud allocation schema.
    The basic functionality is impelement:
    - if new machines are required they are booted on the cheapest available cloud site
    - if machines are not needed any more, the are shut down on the most expansive cloud sites first
    """

    # the maximum number (global) of cloud instances to run
    # can be used as a fallback while debugging Brokering code     
    def __init__(self, max_instances=1000, shutdown_delay=0):
        self.delayedShutdownTime = None
        self.shutdownDelay = shutdown_delay  # seconds
        self._maxInstances = max_instances
        self.logger = logging.getLogger('Broker')

    def modSiteOrders(self, di, siteName, machineName, mod):
        """
        Increases or decreases the machines which shoud be stopped or stared on a site
        """
        if mod == 0:
            return

        if not siteName in di:
            di[siteName] = dict({machineName: 0})

        if not machineName in di[siteName]:
            di[siteName][machineName] = 0

        di[siteName][machineName] += mod

    def decide(self, machineTypes, siteInfo):
        """
        Redistribute cloud usage        
        TODO:
        report if not all req can be met
        the data input to this method is not complete. the Broker has to know which machine is 
        TODO: running at which site. FIX    
        """
        machinesToSpawn = dict()

        for (mname, mreq) in machineTypes.iteritems():
            # dont request any new machines when requirement is None (due to failure)
            if not mreq.required == None:
                delta = mreq.required - mreq.actual
            else:
                delta = 0
            delta = min(self._maxInstances - mreq.actual, delta)

            self.logger.info("machine type " + mname + ": " + str(mreq.actual) + " running " + str(
                mreq.required) + " needed. spawning/removing " + str(delta))

            # if delta > 0:
            if machinesToSpawn.has_key(mname):
                machinesToSpawn[mname] += delta
            else:
                machinesToSpawn[mname] = delta

        # now machinesToSpawn contains the wishlist of machines, distribute this to the cloud

        # spawn, cheap sites first...
        cheapFirst = sorted(siteInfo, lambda x, y: x.cost - y.cost)
        # shutdown, expensive sites first...
        expensiveFirst = sorted(siteInfo, lambda x, y: y.cost - x.cost)

        siteOrders = dict()

        # spawn
        for (mname, tospawn) in machinesToSpawn.iteritems():
            for site in cheapFirst:
                if tospawn > 0:
                    if mname in site.supportedMachineTypes:
                        self.modSiteOrders(siteOrders, site.siteName, mname, tospawn)

                        # implement max quota here
                        tospawn = 0

        # shutdown
        for (mname, tospawn) in machinesToSpawn.iteritems():
            for site in expensiveFirst:
                if tospawn < 0:
                    if mname in site.supportedMachineTypes:
                        if not self.delayedShutdownTime == None:
                            if (datetime.now() - self.delayedShutdownTime).seconds > self.shutdownDelay:
                                self.modSiteOrders(siteOrders, site.siteName, mname, tospawn)
                                self.delayedShutdownTime = None
                        else:
                            if self.shutdownDelay == 0:
                                # remove without delay
                                self.modSiteOrders(siteOrders, site.siteName, mname, tospawn)
                            else:
                                self.delayedShutdownTime = datetime.now()

                        tospawn = 0

        return siteOrders
