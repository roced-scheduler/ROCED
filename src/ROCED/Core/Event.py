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
    '''
    def __new__(self, *args):
        if not '_the_instance' in self.__dict__:
            self._the_instance = object.__new__(self)
        return self._the_instance

    '''

    def __init__(self):
        """
        Abstract base event manager
        """
        self.listener = []

    def listener():  # @NoSelf
        doc = """Docstring"""  # @UnusedVariable

        def fget(self):
            return self._listener

        def fset(self, value):
            self._listener = value

        def fdel(self):
            del self._listener

        return locals()

    listener = property(**listener())

    def publishEvent(self, evt):
        map(lambda x: x.onEvent(evt), self.listener)

    def registerListener(self, new_listener):
        logging.info("Registering new event listener: " + str(new_listener))

        if not new_listener in self.listener:
            self.listener.append(new_listener)

    def clearListeners(self):
        self.listener = []
