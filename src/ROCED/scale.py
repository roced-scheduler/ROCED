#!/usr/bin/env python
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
from __future__ import unicode_literals, absolute_import

"""
ROCED main runtime file
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import unittest
import sys
import argparse
import configparser
import os

from Core.Core import ScaleCoreFactory
from Core import Config
from Util.Daemon import DaemonBase

###
# Unit tests:
###
from Core import CoreTest, EventTest, AdapterTest
from SiteAdapter import SiteTest
from RequirementAdapter import RequirementTest
from IntegrationAdapter import IntegrationTest
from Util import ScaleTools

# Optional modules with unit-tests
try:
    from Util import HTCondor
except ImportWarning:
    pass


class ScaleMain(object):
    def __init__(self):
        # initialize root logger with basic config
        logging.basicConfig(format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
        # initialize a class logger which inherits from root logger
        self.logger = logging.getLogger("Scale")

    def test(self):
        logging.getLogger().setLevel(logging.DEBUG)

        ts = unittest.TestSuite()
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(AdapterTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(SiteTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(CoreTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(EventTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(IntegrationTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(RequirementTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(ScaleTools))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(HTCondor))

        self.logger.info("Running %d tests." % ts.countTestCases())
        result = unittest.TestResult()

        ts.run(result)

        for entry in result.skipped:
            self.logger.warning("Skipped %s (%s)." % (entry[0], entry[1]))

        logging.debug("=======Testing Finished=======")
        if result.wasSuccessful():
            self.logger.info("Test SUCCESS")
        else:
            # this will print the test output with correct new lines
            self.logger.error("!!! Test FAIL")
            for v in result.failures:
                self.logger.error(v[1])
            self.logger.error("!!! Test ERROR")
            for v in result.errors:
                self.logger.error(v[1])

        if not result.wasSuccessful():
            sys.exit(1)
        else:
            sys.exit(0)

    @staticmethod
    def setupLogger(config, debug=False):
        # type: (RawConfigParser, bool) -> None
        """Set up logging object. Log rolls over at midnight."""

        logger = logging.getLogger()
        if debug is True:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if config.has_option(Config.GeneralSection, Config.GeneralLogFolder) is True:
            log_folder = config.get(Config.GeneralSection, Config.GeneralLogFolder)

            if os.path.isdir(log_folder.__str__()) is False:
                try:
                    os.makedirs(log_folder.__str__() + "/")
                except OSError:
                    logging.error("Error while creating folder %s." % log_folder.__str__())
            fname = log_folder + "/roced.log"
            logger.info("Writing to log file %s." % fname)
            file_handler = TimedRotatingFileHandler(fname, when="midnight")
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
            logger.addHandler(file_handler)

    def run(self, config_file_name, debug=False, iterations=None):

        self.logger.info("Loading config %s." % config_file_name)
        config = configparser.RawConfigParser()
        config.readfp(open(config_file_name))

        self.setupLogger(config=config, debug=debug)
        scaleCore = ScaleCoreFactory.getCore(config, maximumInterval=iterations)
        self.logger.info("----------------------------------")
        try:
            with open("roced_logo.txt", mode="r") as file_:
                [self.logger.info(line.rstrip()) for line in file_]
            self.logger.info("----------------------------------")
        except IOError:
            pass
        finally:
            self.logger.info("%s running" % scaleCore.description)
            self.logger.info("----------------------------------")

        scaleCore.init()
        # Run the server's main loop
        scaleCore.startManage()


class MyDaemon(DaemonBase):
    def run(self):
        scaleObject = ScaleMain()
        scaleObject.run(self.configfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ROCED scheduler")
    parser.add_argument("--config", nargs=1,
                        help="Run using a custom config file (default: %(default)s)",
                        default="/etc/roced/roced.conf")
    parser.add_argument("--iterations", type=int,
                        help="Number of control iterations to run (default: unlimited)",
                        default=None)
    parser.add_argument("--debug", action="store_true", help="Print debug information")

    subparsers = parser.add_subparsers(help="")

    parser_start = subparsers.add_parser("standalone", help="Run standalone (output to current session)")
    parser_start.set_defaults(cmd="standalone")

    parser_start = subparsers.add_parser("start", help="Start daemon")
    parser_start.set_defaults(cmd="start")

    parser_start = subparsers.add_parser("stop", help="Stop daemon")
    parser_start.set_defaults(cmd="stop")

    parser_start = subparsers.add_parser("status", help="Show daemon status")
    parser_start.set_defaults(cmd="status")

    parser_start = subparsers.add_parser("test", help="Run unit tests")
    parser_start.set_defaults(cmd="test")

    args = vars(parser.parse_args())

    command = args.get("cmd")
    if command is None:
        logging.error("Please supply a command type!")
        exit(1)
    elif command == "test":
        sm = ScaleMain()
        sm.test()
        exit(0)
    elif command == "standalone":
        sm = ScaleMain()
        sm.run(args["config"][0], args["debug"], args["iterations"])
        exit(0)

    daemon = MyDaemon("/tmp/daemon-scale.pid")
    daemon.configfile = args["config"][0]

    if command == "start":
        daemon.start()
    elif command == "status":
        daemon.status()
    elif command == "stop":
        daemon.stop()
    elif command == "restart":
        daemon.restart()
