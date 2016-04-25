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
from __future__ import unicode_literals

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

# test classes here
# import SiteTest

from Core import Config, EventTest, AdapterTest
from SiteAdapter import SiteTest
from RequirementAdapter import RequirementTest
from IntegrationAdapter import IntegrationTest
from Core import CoreTest
from Util.Daemon import DaemonBase


class ScaleMain(object):
    def __init__(self):
        # initialize root logger with basic config
        logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        # initialize a class logger which inherits from root logger
        self.logger = logging.getLogger('Scale')

    def test(self):
        logging.getLogger().setLevel(logging.DEBUG)

        ts = unittest.TestSuite()
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(AdapterTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(SiteTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(CoreTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(EventTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(IntegrationTest))
        ts.addTests(unittest.defaultTestLoader.loadTestsFromModule(RequirementTest))

        self.logger.info("Running " + str(ts.countTestCases()) + " tests")
        result = unittest.TestResult()
        ts.run(result)

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
                    logging.error("Error while creating /log/ folder")
            fname = log_folder + "/roced.log"
            logger.info("Writing to log file " + fname)
            file_handler = TimedRotatingFileHandler(fname, when='midnight')
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s',
                                  '%Y-%m-%d %H:%M:%S'))
            logger.addHandler(file_handler)

    def run(self, config_file_name, debug=False, iterations=None):

        self.logger.info("Loading config " + str(config_file_name))
        config = configparser.RawConfigParser()
        config.readfp(open(config_file_name))

        self.setupLogger(config=config, debug=debug)

        core_factory = ScaleCoreFactory()
        scaleCore = core_factory.getCore(config, maximumInterval=iterations)

        # scaleCore = Core.ScaleCore(server)
        scaleCore.init()
        scaleCore.startManage()

        # Run the server's main loop
        self.logger.info("----------------------------------")
        self.logger.info(scaleCore.description + " running")


class MyDaemon(DaemonBase):
    def run(self):
        scaleObject = ScaleMain()
        scaleObject.run(self.configfile)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run the ROCED scheduler')
    parser.add_argument('--config', nargs=1,
                        help="Run using a custom config file (default: /etc/roced/roced.conf)",
                        default="/etc/roced/roced.conf")

    parser.add_argument('--iterations', type=int,
                        help="Number of control iterations to run (default: unlimited)",
                        default=None)

    parser.add_argument('--debug', action='store_true', help="Print debug information")

    subparsers = parser.add_subparsers(help='')

    parser_start = subparsers.add_parser('standalone', help='Run standalone')
    parser_start.set_defaults(cmd="standalone")

    parser_start = subparsers.add_parser('stop', help='Stop daemon')
    parser_start.set_defaults(cmd="stop")

    parser_start = subparsers.add_parser('start', help='Start as daemon')
    parser_start.set_defaults(cmd="start")

    parser_start = subparsers.add_parser('status', help='Status of the daemon')
    parser_start.set_defaults(cmd="status")

    parser_start = subparsers.add_parser('test', help='Run unit tests')
    parser_start.set_defaults(cmd="test")

    args = parser.parse_args()

    if args.cmd == 'test':
        sm = ScaleMain()
        sm.test()
        exit(0)

    if args.cmd == 'standalone':
        sm = ScaleMain()
        sm.run(args.config[0], args.debug, args.iterations)
        exit(0)

    daemon = MyDaemon('/tmp/daemon-scale.pid')
    daemon.configfile = args.config[0]

    if args.cmd == 'start':
        daemon.start()
    if args.cmd == 'status':
        daemon.status()
    elif args.cmd == 'stop':
        daemon.stop()
    elif args.cmd == 'restart':
        daemon.restart()
