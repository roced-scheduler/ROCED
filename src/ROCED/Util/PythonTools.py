# ===============================================================================
#
# Copyright (c) 2016 by Frank Fischer
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

import collections
import functools
import logging
import time


def merge_dicts(*dict_args):
    # type: (*dict) -> dict
    """Given any number of dicts, shallow copy and merge into a new dict.

    Precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def summarize_dicts(*dict_args):
    # type: (*dict) -> dict
    """ Add up dictionary contents."""
    # TODO: Make more robust, catching and handling different exceptions.
    result = dict_args[0][0].copy()
    for dictionary in dict_args[0][1:]:
        for key_, value_ in dictionary.items():
            if key_ in result:
                result[key_] += value_
            else:
                result[key_] = value_
    return result


class Singleton(object):
    """Each instance creation returns the same object.

    __init__ is called each time (by python).
    Init is only called once.
    """

    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        else:
            cls.__it__ = it = object.__new__(cls)
            it.init(*args, **kwds)
            return it

    def init(self, *args, **kwds):
        pass


class Caching(dict):
    def __init__(self, validityPeriod=-1, redundancyPeriod=0):
        # type: (int, int)
        """ Decorator class to cache a method's/function's return value. Handles maximum age and errors.

        As long as data is "valid", the currently stored data will be returned. If none exists so far, it will be
        queried and stored.

        During the redundancy period, the function module will try to catch errors [return value "None"] by returning
        the previous result, if present. After the redundancy period, it will directly return the function's
        values/error.

        validityPeriod=0:               Read once, return forever
        validityPeriod>0:               As long as timeout is not reached, return value from cache.
        validityPeriod=-1:              Always call the function (only useful with error caching).

        These values are important, if the function raises an exception or returns with result None:
        redundancyPeriod=0:             If a value is cached, use this result instead
        redundancyPeriod>0:             As long as timeout is not reached, return value from cache instead (> validity!)
        redundancyPeriod=-1:            Return "None"

        Possible combinations:
        Caching(0,-1)                   Read once w/o error caching [regular memoization]
        Caching(x,0)|(x,y[>x])|(x,-1)   Read (valid for x seconds); /w error caching (for y seconds) or w/o
        Caching(-1,0)|(-1,y)            Read always; /w error caching (for y seconds)
        """
        super(Caching, self).__init__()
        self.__lastQueryTime = 0

        if redundancyPeriod > 0:
            self.__redundancy = redundancyPeriod
        elif redundancyPeriod == 0:
            # Always cache errors.
            self.__redundancy = True
        else:
            # No error caching.
            self.__redundancy = None

        if 0 < redundancyPeriod <= validityPeriod:
            # Error caching only makes sense, if it's longer than a single validity cycle.
            raise ValueError("redundancyPeriod (%i) <= validityPeriod (%i)." % (redundancyPeriod, validityPeriod))
        elif validityPeriod > 0:
            self.__validity = validityPeriod
        elif validityPeriod == 0:
            if redundancyPeriod >= 0:
                raise ValueError("redundancyPeriod (%i) & validityPeriod (%i) -> error caching is useless in this case."
                                 % (redundancyPeriod, validityPeriod))
            # Always return from cache (if possible)
            self.__validity = True
        elif redundancyPeriod < 0:
            # @Caching(-1,-1) has no effect at all.
            raise ValueError("redundancyPeriod (%i) & validityPeriod (%i) -> \"Do nothing\"."
                             % (redundancyPeriod, validityPeriod))
        else:
            # Never return from cache.
            self.__validity = None

    def __call__(self, function):
        # type: Callable[..., ...] -> Callable[..., ...]
        """Since __init__ has import arguments, __call__ is only called once and receives the function!"""
        self.__function = function

        def wrapped_function(*args):
            """This is the wrapped function which "replaces" the original."""
            if not isinstance(args, collections.Hashable):
                # unhashable argument, e.g.: list
                return self.__function(*args)
            if (args in self and self.__validity is True or
                    (self.__validity is not None and time.time() < self.__lastQueryTime + self.__validity)):
                # Cache forever or timeout not yet reached.
                return self[args]
            else:
                # Intentionally querying new value(s).
                result = self.__missing__(args)
                logging.debug("Queried new values. Result: %s" % result)
                if result is not None:
                    self[args] = result
                    return self[args]
                else:
                    if self.__redundancy is None:
                        return result
                    elif self.__redundancy is True and args in self:
                        logging.warning("%s did't return values. Using cached values." % self.__function.__str__())
                        return self[args]
                    elif time.time() < self.__lastQueryTime + self.__redundancy and args in self:
                        logging.warning("%s did't return values. Using cached values." % self.__function.__str__())
                        return self[args]
                    else:
                        # This includes passing the timeout or not having a value stored at all
                        return result

        return wrapped_function

    def __missing__(self, key):
        """Method is called internally (if necessary), since we're inheriting from a dictionary."""
        try:
            ret = self.__function(*key)
            self.__lastQueryTime = time.time()
        except BaseException as e:
            logging.warning("%s raised exception '%s' when querying for new values." % (self.__function.__str__(), e))
            ret = None
        return ret

    def __repr__(self):
        """Return the function's docstring."""
        return self.__function.__doc__()

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)
