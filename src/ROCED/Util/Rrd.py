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
from __future__ import print_function

import json
import logging
from datetime import datetime
from os import path

try:
    # Install rrdtool, librrd-dev and python(3)-rrdtool
    import rrdtool
except ImportError:
    rrdtool = None
    exit(1)


class Rrd(object):
    class _Data(object):
        """Constant keys used in JSON file."""
        condor_running = "jobs_running"
        condor_idle = "jobs_idle"
        vm_requested = "machines_requested"
        vm_running = "condor_nodes"
        vm_draining = "condor_nodes_draining"

    # Older version of rrdtool don't understand 1d, 2w, 1M, 1y syntax @ PDP step/rows
    # Use constants as workaround
    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    YEAR = 365 * DAY

    # TODO: Import DS & RRA definitions)
    def __init__(self, database, site_name, machine_type, check_existence=True):
        # type: (str) -> None
        """Round-robin database logging/plotting."""
        if database.rfind(".rrd") == -1:
            self.database = "%s.rrd" % database
        else:
            self.database = database

        if check_existence and not path.exists(self.database):
            raise ValueError("File %s does not exist." % self.database)

        self.site_name = site_name
        self.machine_type = machine_type

    def __enter__(self):
        """Context manager - currently only intended for usage with update."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Raise exception(s) that appear along the way
        return False

    def update(self, value_list, timestamp="N"):
        # type: (list, str) -> None
        """Update RRD database with a single new entry.

        If no explicit timestamp is supplied, entry is added with current time.
        Value list must be ordered similar to the RRD layout.
        """
        command = list((self.database, str(timestamp), ":".join([str(entry) for entry in value_list])))
        ret = rrdtool.update(command)
        if ret:
            raise RuntimeError(rrdtool.error())

    @staticmethod
    def create(database_name, step=30, start=20160804, max_machines=10000,
               site_name="freiburg_cloud", machine_type="fr-default"):
        """Create round-robin database and return corresponding python object.

        An object consists of Data-Sources and Round-Robin-Archives which collect data source values.
        - DataSource:Name:Type:Heartbeat:Min:Max
        - Archive:Function:xff:Steps:Rows
        [x-files factor: Which data fraction may be invalid for data to still be considered valid].

        More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html
        """
        logging.info("Creating round-robin database")

        data_sources = ["DS:jobs_running:GAUGE:900:0:%s" % max_machines,
                        "DS:jobs_idle:GAUGE:900:0:U",
                        "DS:machines_requested:GAUGE:900:0:%s" % max_machines,
                        "DS:nodes_running:GAUGE:900:0:%s" % max_machines,
                        "DS:nodes_draining:GAUGE:900:0:%s" % max_machines]

        rr_archives = ["RRA:AVERAGE:0.5:%s:%s" % (step, Rrd.DAY/step),
                       "RRA:AVERAGE:0.5:%s:%s" % ((5 * Rrd.MINUTE / step), Rrd.WEEK / (5 * Rrd.MINUTE)),
                       "RRA:AVERAGE:0.5:%s:%s" % ((15 * Rrd.MINUTE / step), (Rrd.MONTH/(15 * Rrd.MINUTE))),
                       "RRA:AVERAGE:0.5:%s:%s" % ((30 * Rrd.MINUTE / step), ((90 * Rrd.DAY)/(30 * Rrd.MINUTE))),
                       "RRA:AVERAGE:0.5:%s:%s" % ((Rrd.HOUR / step), ((2 * Rrd.MONTH) / Rrd.HOUR)),
                       "RRA:AVERAGE:0.5:%s:%s" % (((4 * Rrd.HOUR)/step), ((6 * Rrd.MONTH) / (4 * Rrd.HOUR))),
                       "RRA:AVERAGE:0.5:%s:%s" % ((8 * Rrd.HOUR / step), (Rrd.YEAR / (8 * Rrd.HOUR)))]

        result = Rrd(database_name, site_name, machine_type, check_existence=False)

        command = list((result.database, "-s", str(step), "-b", str(start)))
        command.extend(data_sources)
        command.extend(rr_archives)
        stderr = rrdtool.create(command)
        if stderr:
            raise RuntimeError(rrdtool.error())

        return result

    def update_from_json(self, file_list):
        """Update database by importing ROCED json logs.

        1. Store import strings in a list (format <<timestamp:value[:value:value...]>>
        2. Sort the list
        3. Update RRD

        More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html
        """
        result = list()
        processed_keys = set()
        logging.info("Updating RRD from %d files." % len(file_list))

        for input_file in file_list:
            if ".json" in input_file:
                with open(input_file, "r") as json_file:
                    result.extend(self._dict_to_update_string_list(json.load(json_file), processed_keys))
            else:
                raise NotImplementedError("%s can not be parsed. File type not yet implemented." % input_file)

        result.sort()
        result.insert(0, self.database)
        rrdtool.update(result)

    def update_from_dict(self, data_dict):
        """Update database from a ROCED log dictionary.

        More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html
        """
        result = self._dict_to_update_string_list(data_dict)
        result.insert(0, self.database)
        rrdtool.update(result)

    def _dict_to_update_string_list(self, data_dict, processed=None):
        """Convert a dictionary to a list of RRD update strings."""
        result = list()
        if processed is None:
            processed_keys = set()
        else:
            processed_keys = processed

        for timestamp in data_dict:
            if timestamp not in processed_keys:
                processed_keys.add(timestamp)
                string = self._item_to_update_string(data_dict[timestamp], timestamp)
                logging.debug(string)
                result.append(string)

        result.sort()
        return result

    def _item_to_update_string(self, item, timestamp):
        """Convert a single entry to a RRD update string."""
        temp1 = item.get(self.machine_type, {}).get(self._Data.condor_running, "U")
        temp2 = item.get(self.machine_type, {}).get(self._Data.condor_idle, "U")
        temp3 = item.get(self.site_name, {}).get(self._Data.vm_requested, "U")
        temp4 = item.get(self.site_name, {}).get(self._Data.vm_running, "U")
        temp5 = item.get(self.site_name, {}).get(self._Data.vm_draining, "U")
        result = "%s:%s:%s:%s:%s:%s" % (timestamp, temp1, temp2, temp3, temp4, temp5)
        return str(result)

    def export_to_xml(self, xml_name="output"):
        """Export RRD database to XML.

        More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrddump.en.html
        """
        # TODO: This should probably be implemented with rrdxport
        rrdtool.dump(self.database, "%s.xml" % xml_name)

    def fetch(self, commands, function="AVERAGE"):
        """Fetch values from an RRD database.

        More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrdfetch.en.html
        """
        command = [str(entry) for entry in commands]
        command.insert(0, self.database)
        command.insert(1, function)
        time_range, keys, data = rrdtool.fetch(command)

        timestamps = [datetime.fromtimestamp(unix_time) for unix_time
                      in range(time_range[0], time_range[1], time_range[2])]

        return timestamps, keys, data

        # def plot(self, plot_name="freiburg", cores_per_vm=4):
        #     """Basic plot creation template.
        #
        #     More documentation @ http://oss.oetiker.ch/rrdtool/doc/rrdgraph.en.html
        #     """
        #     dpi = 72
        #     width = 18 * dpi
        #     height = 8 * dpi
        #     rrdtool.graph("%s.png" % plot_name,
        #                   # Plot attributes
        #                   "--start", "20160804",
        #                   "--end", "20161003",
        #                   "--vertical-label=Slots",
        #                   "-w %d" % width,
        #                   "-h %d" % height,
        #                   "-u 12000", "-r",
        #                   # Data definitions
        #                   "DEF:jobs=%s:jobs_running:AVERAGE" % self.database,
        #                   "DEF:backlog=%s:jobs_idle:AVERAGE" % self.database,
        #                   "DEF:request_raw=%s:machines_requested:AVERAGE" % self.database,
        #                   "DEF:machines_raw=%s:nodes_running:AVERAGE" % self.database,
        #                   "DEF:draining_raw=%s:nodes_draining:AVERAGE" % self.database,
        #                   "CDEF:request=request_raw,%d,*" % cores_per_vm,
        #                   "CDEF:machines=machines_raw,%d,*" % cores_per_vm,
        #                   "CDEF:draining=draining_raw,%d,*" % cores_per_vm,
        #                   # Plot content
        #                   "AREA:jobs#b8c9ec:Jobs running",
        #                   "AREA:jobs#fdbe81:Jobs waiting:STACK:skipscale",
        #                   "LINE1:draining#7f69db:Slots draining",
        #                   "LINE3:machines#2c7bb6:Slots available:STACK",
        #                   "LINE1:request#FF3333:Slots requested:STACK")
