# ===============================================================================
#
# Copyright (c) 2016 # by Frank Fischer
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

import re


class Torque(object):
    """Utility functions/methods for Adaptive Computing's TORQUE resource manager."""
    # See http://docs.adaptivecomputing.com/torque/4-1-4/Content/topics/commands/qstat.htm
    job_completed = "C"
    job_exiting = "E"
    job_held = "H"
    job_queued = "Q"
    job_running = "R"
    job_moving = "T"
    job_waiting = "W"
    job_suspended = "S"

    all_job_idle = frozenset((job_queued, job_suspended, job_waiting))
    all_job_running = frozenset((job_exiting, job_running, job_moving))
    all_job_finished = frozenset((job_held, job_completed))

    __qstat_regex_short = re.compile(r"""
                         ^(\d+(\[\d+\])?)\S+\s+     # JobId + Array
                         (\S+)\s+                   # JobName
                         (\S+)\s+                   # User
                         ((?:\d{1,2}:?){3,4})\s+    # Time Used
                         ([CEHQRTWS]).+$            # Status
                                   """, flags=re.MULTILINE | re.VERBOSE)

    __qstat_regex_long = re.compile(r"""
                         ^(\d+(\[\d+\])?)\S+\s+   # JobId + Array
                         (\S+)\s+                 # Username
                         \S+\s+                   # Queue
                         (\S+)\s+                 # Job Name
                         \d+\s+                   # Session ID (trash)
                         (\d+)\s+                 # Number of nodes
                         (\d+)\s+                 # Number of CPU cores
                         (\d+)\wb\s+              # Memory
                         ((?:\d{1,2}:?){3,4})\s+  # Wall Time; 3-4 groups of 00:
                         ([CEHQRTWS]).+$          # Job status (single char) // trash, line end
                                  """, flags=re.MULTILINE | re.VERBOSE)

    @classmethod
    def parse_qstat(cls, std_out, long_output=True):
        # type: (str) -> dict
        """Parse qstat output.

        "Short" is the default qstat output, long is the output generated with -alt or -rlt flags.
        """
        if long_output:
            output = {jobid: {"status": status, "job_name": jobname, "cores": int(cores), "memory": int(memory),
                              "wall_time": wall_time}
                      for jobid, user, jobname, nodes, cores, memory, wall_time, status
                      in cls.__qstat_regex_long.findall(std_out)}
        else:
            output = {jobid: {"status": status, "job_name": jobname, "wall_time": time}
                      for jobid, jobname, user, time, status in cls.__qstat_regex_short.findall(std_out)}

        return output

    @classmethod
    def qstat_filter(cls, dict_, status):
        # type: (dict, str) -> dict
        for key, val in dict_.items():
            if val.get("status") not in status:
                continue
            yield key, val


class Moab(object):
    """Utility functions/methods for Adaptive Computing's Moab Workload Manager."""
    # Some lines contain multiple key value pairs per line; split at repeated whitespaces NOT following ":"
    __checkjob_replace_keys = ("SrcRM", "TasksPerNode")
    __checkjob_regex_replace = re.compile(r"(?<!:)\b(\s{2,})", flags=re.IGNORECASE)
    # Statements we explicitly ignore for now (Regex runaway argument)
    __checkjob_regex_ignore = re.compile(r"(?:available|rejected) for|Node Availability for Partition",
                                         flags=re.IGNORECASE)
    # Identify dictionary key and (key: value) pairs
    __checkjob_regex_key_identifier = re.compile(r"job (\S+).+", flags=re.IGNORECASE)
    __checkjob_regex_value_map = re.compile(r"^((?:[a-z0-9]+\s?)+):\s+(.+)(?!:)", flags=re.IGNORECASE)
    __checkjob_separator = "============================"

    # See http://docs.adaptivecomputing.com/maui/commands/checkjob.php
    # TODO: Implement parser which follows documentation details

    @classmethod
    def parse_checkjob(cls, iterable):
        """Convert "checkjob -v all" output to python dictionary entries.

        Future versions of MOAB should have a prettier output by using -A or --xml with Beautiful Soup + lxml.

        Format: (job_id, {dictionary of key:value pairs})
        """
        # Generator expressions for filtering/regexing:
        # 1. Ignore empty lines
        # 2. Ignore "regex_ignore"
        # 3. Apply line splits for problematic lines
        content = (line.strip() for line in iterable
                   if len(line) > 0 and cls.__checkjob_regex_ignore.search(line) is None)
        splitted = (cls.__checkjob_regex_replace.sub("\n", line)
                    if line.startswith(cls.__checkjob_replace_keys) else line
                    for line in content)

        key = ""
        value_dict = {}
        # Here comes the real dict assignment
        for line in splitted:
            if line.startswith("job"):
                try:
                    key = cls.__checkjob_regex_key_identifier.search(line).groups()[0]
                except IndexError:
                    print("error with key for %s" % line)
            elif line == cls.__checkjob_separator:
                output = (int(key), value_dict)
                key = ""
                value_dict = {}
                yield output
            else:
                value_dict.update(((key, value) for key, value in cls.__checkjob_regex_value_map.findall(line)))

        yield (int(key), value_dict)
