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
from __future__ import unicode_literals

import abc
import logging


class EventBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self):
        """
        Abstract base event class
        """
        pass


class EventPublisher(object):
    __metaclass__ = abc.ABCMeta

    # old singleton implementation:
    # def __new__(self, *args):
    #   if not '_the_instance' in self.__dict__:
    #       self._the_instance = object.__new__(self)
    #   return self._the_instance

    def __init__(self):
        """
        Abstract base event manager
        """
        self.__listener = []

    def publishEvent(self, evt):
        [listener.onEvent(evt) for listener in self.__listener]

    def registerListener(self, new_listener):
        """Register a class as event listener. This class' method "onEvent" may be triggered."""
        if new_listener not in self.__listener:
            if not hasattr(new_listener, "onEvent"):
                logging.error("Can't register listener " + type(new_listener).__name__ +
                              ". Method \"onEvent\" is missing.")
            logging.info("Registering new event listener: " + type(new_listener).__name__)
            self.__listener.append(new_listener)

    def clearListeners(self):
        self.__listener = []
