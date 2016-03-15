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

import Event
import ScaleTest


class EventPublisherTest(Event.EventPublisher):
    pass


class EventManagerTest(ScaleTest.ScaleTestBase):
    def test_publish(self):
        emgr = EventPublisherTest()

        self.wasCalled = False

        class Listener(object):
            def onEvent(self, evt):
                self.utest.wasCalled = True
                self.utest.assertEqual(evt, "eventstring")

        l = Listener()
        l.utest = self
        emgr.registerListener(l)
        emgr.publishEvent("eventstring")

        self.assertTrue(self.wasCalled)
