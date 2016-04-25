# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015, 2016
# by Frank Fischer, Georg Fleig, Thomas Hauth and Stephan Riedel
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


import csv
import json
import logging
import os
import time
from datetime import datetime

import sys

PY3 = sys.version_info > (3,)

"""
    JSON Log handling

    stores the following to a json file
    ::logs:    how much machines are needed/used/requested
    ::stats:   time statistics for each machine
"""


# TODO: Use config file "logfolder"

class JsonLog(object):
    # use class variables to share log among instances
    __jsonLog = {}
    __fileName = ""

    @classmethod
    def __init__(cls, dir_="log", prefix="monitoring", suffix=""):
        """
        Initialize log folder and log file
        """
        # Existence check for log folder [log file creation requires existing folder]
        if os.path.isdir(dir_.__str__()) is False:
            try:
                os.makedirs(dir_.__str__() + "/")
            except OSError:
                logging.error("Error when creating /log/ folder")
        # Build log file name
        if not cls.__fileName:
            cls.__fileName = (dir_.__str__() + "/" + prefix.__str__() + "_" +
                              str(datetime.today().strftime("%Y-%m-%d_%H-%M")) +
                              suffix.__str__() + ".json")

    @classmethod
    def __enter__(cls):
        return cls

    # noinspection PyUnusedLocal
    @classmethod
    def __exit__(cls, exc_type, exc_val, exc_tb):
        # Raise exception(s) that appear along the way
        return False

    @classmethod
    def addItem(cls, site, key, value):
        if site not in cls.__jsonLog:
            cls.__jsonLog[site] = {}
        cls.__jsonLog[site][key] = value

    @classmethod
    def writeLog(cls):
        """
        Write current log into JSON file.
        """
        oldLog = {}
        if os.path.isfile(cls.__fileName):
            try:
                with open(cls.__fileName, "r") as jsonFile:
                    try:
                        oldLog = json.load(jsonFile)
                        oldLog[int(time.time())] = cls.__jsonLog
                    except ValueError:
                        logging.error("Could not parse JSON log!")
                        oldLog = {int(time.time()): cls.__jsonLog}
            except IOError:
                logging.error("JSON file could not be opened for logging!")

        else:
            oldLog = {int(time.time()): cls.__jsonLog}
        try:
            with open(cls.__fileName, "w") as jsonFile:
                json.dump(oldLog, jsonFile, indent=2)
        except IOError:
            logging.error("JSON file could not be opened for logging!")

        # clear jsonLog for next cycle
        cls.__jsonLog = {}

    @classmethod
    def printLog(cls):
        """
        Print log to output device.

        Format: | Timestamp: Log Output
        """
        print(str(int(time.time())) + ": " + str(cls.__jsonLog))


class JsonStats(object):
    __jsonStats = {}
    __fileName = ""

    @classmethod
    def __init__(cls, dir_="log", prefix="stats", suffix=""):
        """
        Initialize log folder and log file
        """
        # Existence check for log folder [log file creation requires existing folder]
        if os.path.isdir(dir_.__str__()) is False:
            try:
                os.makedirs(dir_.__str__() + "/")
            except OSError:
                logging.error("Error when creating /log/ folder")
        # Build log file name
        if not cls.__fileName:
            cls.__fileName = (dir_.__str__() + "/" + prefix.__str__() + "_" +
                              str(datetime.today().strftime("%Y-%m-%d_%H-%M")) +
                              suffix.__str__() + ".json")

    @classmethod
    def add_item(cls, site, mid, value):
        if site not in cls.__jsonStats:
            cls.__jsonStats[site] = {}
        if mid not in cls.__jsonStats[site]:
            cls.__jsonStats[site][str(mid)] = {}
        cls.__jsonStats[site][str(mid)] = value

    @classmethod
    def write_stats(cls):
        oldStats = {}
        if os.path.isfile(cls.__fileName):
            try:
                with open(cls.__fileName, "r") as jsonFile:
                    try:
                        oldStats = json.load(jsonFile)
                        for site in cls.__jsonStats:
                            if site not in oldStats:
                                oldStats[site] = {}
                            for mid in cls.__jsonStats[site]:
                                if mid not in oldStats[site]:
                                    oldStats[site][mid] = []
                                if cls.__jsonStats[site][mid] not in oldStats[site][mid]:
                                    oldStats[site][mid].append(cls.__jsonStats[site][mid])
                    except ValueError:
                        logging.error("Could not parse JSON log!")
                        for site in cls.__jsonStats:
                            for mid in cls.__jsonStats[site]:
                                oldStats = {site: {mid: cls.__jsonStats[mid]}}
            except IOError:
                logging.error("JSON file could not be opened for logging!")
        else:
            oldStats = {
                cls.__jsonStats.keys()[-1]: {
                    cls.__jsonStats[cls.__jsonStats.keys()[-1]].keys()[-1]:
                        [cls.__jsonStats[cls.__jsonStats.keys()[-1]].values()[-1]]
                }
            }
        try:
            with open(cls.__fileName, "w") as jsonFile:
                json.dump(oldStats, jsonFile, sort_keys=True, indent=2)
        except IOError:
            logging.error("JSON file could not be opened for logging!")

    @classmethod
    def printStats(cls):
        for mid in cls.__jsonStats:
            print(str(mid) + ": " + str(cls.__jsonStats[mid]))


class UnicodeWriter(object):
    def __init__(self, filename, fieldnames, dialect=csv.excel,
                 encoding="utf-8", **kw):
        self.filename = filename
        self.fieldnames = fieldnames
        self.dialect = dialect
        self.encoding = encoding
        self.kw = kw

    def __enter__(self):
        if PY3:
            self.f = open(self.filename, 'at', encoding=self.encoding, newline='')
        else:
            self.f = open(self.filename, 'wb')
        self.writer = csv.DictWriter(self.f, fieldnames=self.fieldnames, dialect=self.dialect,
                                     **self.kw)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()

    def writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        if not PY3:
            header = {str(key).encode(self.encoding): str(value).encode(self.encoding)
                      for key, value in header.items()}
        self.writerow(header)

    def writerow(self, dictrow):
        if not PY3:
            dictrow = {str(key).encode(self.encoding): str(value).encode(self.encoding)
                       for key, value in dictrow.items()}
        self.writer.writerow(dictrow)


class CsvStats(object):
    __csvStats = []
    # [{"site":"site_name", "mid":"machine_id", "old_status":"old status",
    #   "new_status":"new status", "timestamp":"date.date.now()",
    #   "time_diff":"datetime.timediff()"},{},{},...]
    __fileName = ""
    __fieldnames = ["site", "mid", "old_status", "new_status", "timestamp", "time_diff"]

    @classmethod
    def __init__(cls, dir_="log", prefix="stats", suffix=""):
        # Existence check for log folder [log file creation requires existing folder]
        if os.path.isdir(dir_.__str__()) is False:
            try:
                os.makedirs(dir_.__str__() + "/")
            except OSError:
                logging.error("Error when creating /log/ folder")
        cls.__fileName = str(dir_) + '/' + prefix + '_' + str(
            datetime.today().strftime('%Y-%m-%d')) + str(
            suffix) + ".csv"

        # Existence check for log file
        if not os.path.isfile(cls.__fileName):
            # with open(cls.__fileName, "w", newline='') as stats_file:
            #     writer = UnicodeWriter(stats_file, fieldnames=cls.__fieldnames)
            with UnicodeWriter(cls.__fileName, fieldnames=cls.__fieldnames) as writer:
                writer.writeheader()

    @classmethod
    def __enter__(cls):
        return cls

    # noinspection PyUnusedLocal
    @classmethod
    def __exit__(cls, exc_type, exc_val, exc_tb):
        # Throw exception, if a problem occurred
        return False

    @classmethod
    def add_item(cls, site, mid, old_status, new_status, timestamp, time_diff):
        cls.__csvStats.append(
            {"site": site, "mid": mid, "old_status": old_status, "new_status": new_status,
             "timestamp": timestamp, "time_diff": time_diff})

    @classmethod
    def write_stats(cls):
        with UnicodeWriter(cls.__fileName, fieldnames=cls.__fieldnames) as writer:
            # with open(cls.__fileName, "a") as stats_file:
            #     writer = UnicodeWriter(stats_file, fieldnames=cls.__fieldnames)
            for stat in range(len(cls.__csvStats)):
                writer.writerow(cls.__csvStats.pop())

    @classmethod
    def printLog(cls):
        for stat in cls.__csvStats:
            print(stat)
