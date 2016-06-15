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

# htcondor module has problem with unicode literals!

import htcondor
import re
from collections import defaultdict


class HTCondorPy(object):
    jobStatusIdle = 1
    jobStatusRunning = 2
    jobStatusRemoved = 3
    jobStatusCompleted = 4
    jobStatusHeld = 5
    jobStatusTransferOutput = 6
    jobStatusSuspended = 7

    def __init__(self, collector=None):
        if collector is None:
            self.collector = htcondor.Collector()
        else:
            self.collector = htcondor.Collector(collector)
        """Central collector."""
        self.schedds = [htcondor.Schedd(classAd) for classAd in self.collector.query(htcondor.AdTypes.Schedd)]
        """List of schedd objects, retrieved from collector."""

    def status(self, constraint=True):
        """Similar to condor_status."""
        condor_machines = defaultdict(list)
        result = self.collector.query(ad_type=htcondor.AdTypes.Startd,
                                      projection=["Name", "State", "Activity"],
                                      constraint=constraint)

        regex = re.compile("slot\d+@([\w-]+).+")

        for slot in result:
            condor_machines[regex.search(slot["Name"]).group(1)].append([slot["State"], slot["Activity"]])

        return condor_machines

    def q(self, constraint=True):
        """Similar to condor_q."""
        condor_q = defaultdict(list)
        queries = [schedd.xquery(requirements="JobStatus == 1 || JobStatus == 2 && %s" % constraint,
                                 projection=["ClusterId", "ProcId", "RequestCpus", "JobStatus"])
                   for schedd in self.schedds]

        for query in htcondor.poll(queries):
            for ads in query.nextAdsNonBlocking():
                key = "%s.%s" % (ads.get("ClusterId"), ads.get("ProcId"))
                condor_q[key].append(int(ads.get("RequestCpus")))
                condor_q[key].append(int(ads.get("JobStatus")))

        return condor_q
