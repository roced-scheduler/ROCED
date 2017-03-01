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

try:
    import htcondor
except ImportError:
    # This packet is optional and only available on python 2.7
    pass
import logging
import re
import time
import types
from collections import defaultdict

from Core import ScaleTest


class HTCondorPy(object):
    jobStatusIdle = 1
    jobStatusRunning = 2
    jobStatusRemoved = 3
    jobStatusCompleted = 4
    jobStatusHeld = 5
    jobStatusTransferOutput = 6
    jobStatusSuspended = 7

    __q_requirement_string = "RoutedToJobId =?= undefined && ( JobStatus == %d || JobStatus == %d )" % (
                              jobStatusIdle, jobStatusRunning)

    def __init__(self, collector=None):
        """Helper class to query HTCondor via python bindings."""
        if collector is None:
            self.collector = htcondor.Collector()
        else:
            self.collector = htcondor.Collector(collector)
        """Central collector."""
        self.schedds = [htcondor.Schedd(classAd) for classAd in self.collector.query(htcondor.AdTypes.Schedd)]
        """List of schedd objects, retrieved from collector."""

    def status(self, constraint=True):
        # type: Union[bool, str] -> defaultdict
        """Return condor machine status (CLI condor_status)."""
        condor_machines = defaultdict(list)
        result = self.collector.query(ad_type=htcondor.AdTypes.Startd,
                                      projection=["Name", "State", "Activity"],
                                      constraint=constraint)

        regex = re.compile("slot[\d_]+@([\w-]+).+")

        for slot in result:
            condor_machines[regex.search(slot["Name"]).group(1)].append([slot["State"], slot["Activity"]])

        return condor_machines

    def q(self, constraint=True):
        # type: Union[bool, str] -> defaultdict
        """Return list of running and idle condor jobs (CLI condor_q)."""
        condor_q = defaultdict(list)
        queries = [schedd.xquery(requirements="%s && %s" % (self.__q_requirement_string, constraint),
                                 projection=["ClusterId", "ProcId", "RequestCpus", "JobStatus"])
                   for schedd in self.schedds]

        for query in htcondor.poll(queries):
            for ads in query.nextAdsNonBlocking():
                key = "%s.%s" % (ads.get("ClusterId"), ads.get("ProcId"))
                condor_q[key].append(int(ads.get("RequestCpus")))
                condor_q[key].append(int(ads.get("JobStatus")))

        return condor_q


class CondorPyTest(ScaleTest.ScaleTestBase):
    def setUp(self):
        try:
            isinstance(htcondor, types.ModuleType)
        except NameError:
            self.skipTest("htcondor module missing")
        self.condor = HTCondorPy()

    def test_CondorStatus(self):
        logging.debug("===Condor Status (Python)===")
        startTime = time.time()
        self.condor.status()
        logging.info("Runtime:\t%fs" % (time.time() - startTime))

    def test_CondorQ(self):
        logging.debug("====Condor Q (Python)===")
        startTime = time.time()
        self.condor.q()
        logging.info("Runtime:\t%fs" % (time.time() - startTime))
