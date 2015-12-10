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


from datetime import datetime
import json
import logging
import os
import time


"""

    stores the following to a json file

    logs: how much machines are needed/used/requested

    stats: time statistics for each machine

"""


class JsonLog:
    # use class variables to share log among instances
    __jsonLog = {}
    __fileName = ""

    def __init__(self):
        if not JsonLog.__fileName:
            JsonLog.__fileName = "log/monitoring_" + str(datetime.today().strftime("%Y-%m-%d_%H-%M")) + ".json"

    def addItem(self, site, key, value):
        if site not in JsonLog.__jsonLog.keys():
            JsonLog.__jsonLog[site] = {}
        JsonLog.__jsonLog[site][key] = value

    def writeLog(self):
        oldLog = {}
        if os.path.isfile(JsonLog.__fileName):
            try:
                jsonFile = open(JsonLog.__fileName, "r")
                try:
                    oldLog = json.load(jsonFile)
                    oldLog[int(time.time())] = JsonLog.__jsonLog
                except ValueError:
                    logging.error("Could not parse JSON log!")
                    oldLog = {int(time.time()): JsonLog.__jsonLog}
                jsonFile.close()
            except IOError:
                logging.error("JSON file could not be opened for logging!")
        else:
            oldLog = {int(time.time()): JsonLog.__jsonLog}
        try:
            jsonFile = open(JsonLog.__fileName, "w")
            json.dump(oldLog, jsonFile, sort_keys=True, indent=2)
            jsonFile.close()
        except IOError:
            logging.error("JSON file could not be opened for logging!")

        # clear jsonLog for next cycle
        JsonLog.__jsonLog = {}

    def printLog(self):
        print str(int(time.time())) + ": " + str(JsonLog.__jsonLog)


class JsonStats:
    __jsonStats = {}
    __fileName = ""

    def __init__(self, dir="log", prefix="stats", suffix=""):
        #if not JsonStats.__fileName:
        JsonStats.__fileName = str(dir) + '/' +\
                               prefix + '_' + str(datetime.today().strftime('%Y-%m-%d')) + str(suffix) + ".json"


    def add_item(self, site, mid, value):
        if site not in JsonStats.__jsonStats.keys():
            JsonStats.__jsonStats[site] = {}
        if mid not in JsonStats.__jsonStats[site].keys():
            JsonStats.__jsonStats[site][str(mid)] = {}
        JsonStats.__jsonStats[site][str(mid)] = value

    def write_stats(self):
        oldStats = {}
        if os.path.isfile(JsonStats.__fileName):
            try:
                jsonFile = open(JsonStats.__fileName, "r")
                try:
                    oldStats = json.load(jsonFile)
                    for site in JsonStats.__jsonStats.keys():
                        if site not in oldStats.keys():
                            oldStats[site] = {}
                        for mid in JsonStats.__jsonStats[site].keys():
                            if mid not in oldStats[site].keys():
                                oldStats[site][mid] = []
                            if JsonStats.__jsonStats[site][mid] not in oldStats[site][mid]:
                                oldStats[site][mid].append(JsonStats.__jsonStats[site][mid])
                except ValueError:
                    logging.error("Could not parse JSON log!")
                    for site in JsonStats.__jsonStats.keys():
                        for mid in JsonStats.__jsonStats[site].keys():
                            oldStats = {site: {mid: JsonStats.__jsonStats[mid]}}
                jsonFile.close()
            except IOError:
                logging.error("JSON file could not be opened for logging!")
        else:
            oldStats = {
                JsonStats.__jsonStats.keys()[-1]: {
                    JsonStats.__jsonStats[JsonStats.__jsonStats.keys()[-1]].keys()[-1]:
                        [JsonStats.__jsonStats[JsonStats.__jsonStats.keys()[-1]].values()[-1]]
                }
            }
        try:
            jsonFile = open(JsonStats.__fileName, "w")
            json.dump(oldStats, jsonFile, sort_keys=True, indent=2)
            jsonFile.close()
        except IOError:
            logging.error("JSON file could not be opened for logging!")

    def printStats(self):
        for mid in JsonStats.__jsonStats.keys():
            print str(mid) + ": " + str(JsonStats.__jsonStats[mid])