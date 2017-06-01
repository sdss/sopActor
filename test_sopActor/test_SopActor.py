#!/usr/bin/env python2
# encoding: utf-8

# Created by Brian Cherinka on 2016-06-09 16:21:28
# Licensed under a 3-clause BSD license.

# Revision History:
#     Initial Version: 2016-06-09 16:21:28 by Brian Cherinka
#     Last Modified On: 2016-06-09 16:21:28 by Brian


from __future__ import print_function, division, absolute_import
import unittest

from actorcore import Actor
from opscore.actor import Model, KeyVarDispatcher
from actorcore import TestHelper

import sopTester

from sopActor import SopActor

logDirBase = 'temp/'

''' unit tests for SopActor '''


class SopActorTester(unittest.TestCase):
    """Parent class for tests that actually need a proper sopActor instance."""
    @classmethod
    def setUpClass(cls):
        # can only configure the dispatcher once.
        if Model.dispatcher is None:
            Model.setDispatcher(KeyVarDispatcher())
        Actor.setupRootLogger = TestHelper.setupRootLogger

    def setUp(self):
        super(SopActorTester, self).setUp()
        self.addCleanup(self._close_port)

    def _close_port(self):
        """
        Close the connection: requires handling the deferred. Details here:
            https://jml.io/pages/how-to-disconnect-in-twisted-really.html
        """
        if getattr(self, 'sop', None) is not None:
            deferred = self.sop.commandSources.port.stopListening()
            deferred.callback(None)

    def tearDown(self):
        # have to clear any actors that were registered previously.
        Model._registeredActors = set()
        super(SopActorTester, self).tearDown()


class TestSopActor(SopActorTester):
    def test_init_fails(self):
        with self.assertRaises(KeyError):
            self.sop = SopActor.SopActor.newActor(location='nonsense', makeCmdrConnection=False)

    def test_init_apo(self):
        self.sop = SopActor.SopActor.newActor(location='apo', makeCmdrConnection=False)
        self.assertIsInstance(self.sop, SopActor.SopActorAPO)
        self.assertEqual(TestHelper.logBuffer.basedir, '/data/logs/actors/sop')
        logged = TestHelper.logBuffer.getvalue()
        self.assertIsInstance(self.sop.actorState, Actor.ActorState)
        self.assertIn('attaching command set SopCmd', logged)
        self.assertIn('attaching command set SopCmd_APO', logged)

    def test_init_lco(self):
        self.sop = SopActor.SopActor.newActor(location='lco', makeCmdrConnection=False)
        self.assertIsInstance(self.sop, SopActor.SopActorLCO)
        self.assertIsInstance(self.sop.actorState, Actor.ActorState)
        self.assertEqual(TestHelper.logBuffer.basedir, '/data/logs/actors/sop')
        logged = TestHelper.logBuffer.getvalue()
        self.assertIn('attaching command set SopCmd', logged)
        self.assertIn('attaching command set SopCmd_LCO', logged)

    def test_init_local(self):
        self.sop = SopActor.SopActor.newActor(location='local', makeCmdrConnection=False)
        self.assertIsInstance(self.sop, SopActor.SopActorLocal)
        self.assertIsInstance(self.sop.actorState, Actor.ActorState)
        self.assertEqual(TestHelper.logBuffer.basedir, '/data/logs/actors/sop')
        logged = TestHelper.logBuffer.getvalue()
        self.assertIn('attaching command set SopCmd', logged)
        self.assertIn('attaching command set SopCmd_LOCAL', logged)


if __name__ == '__main__':
    verbosity = 2

    unittest.main(verbosity=verbosity)
