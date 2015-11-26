#!/usr/bin/env python
# encoding: utf-8
"""

test_slewThread.py

Created by José Sánchez-Gallego on 25 Nov 2015.
Licensed under a 3-clause BSD license.

Revision history:
    25 Nov 2015 J. Sánchez-Gallego
      Initial version

Tests the commands included in the slew thread.

"""

from __future__ import division
from __future__ import print_function
import unittest

from actorcore import TestHelper
import sopTester

import sopActor
import sopActor.myGlobals as myGlobals
from sopActor.multiCommand import MultiCommand

from sopActor import slewThread
from sopActor import tccThread
from sopActor import ffsThread
from sopActor import apogeeThread

# False for less printing, True for more printing
verbose = True


class SlewThreadTester(sopTester.SopThreadTester, unittest.TestCase):
    """Tests the commands included in the slew thread."""

    def setUp(self):
        self.verbose = verbose
        self.useThreads = [
            ('tcc', sopActor.TCC, tccThread.main),
            ('ffs', sopActor.FFS, ffsThread.main),
            ('apogeeScript', sopActor.APOGEE_SCRIPT, apogeeThread.script_main),
            ('apogee', sopActor.APOGEE, apogeeThread.main)]
        super(SlewThreadTester, self).setUp()
        self.fail_on_no_cmd_calls = True  # we need cmd_calls for all of these.


class TestApogeeDomeFlat(SlewThreadTester):
    """Tests for apogee_dome_flat.

    NOTE: the cmd numbers here aren't right, because I haven't fully faked
    the apogee utrReadState thing, so the fflamps don't get called on+off.

    TBD: That would increase the nCalls in each of these by 2:
          one for ff.on and one for ff.off
    """

    def _apogee_dome_flat(self, nCall, nInfo, nWarn, nErr, multiCmd,
                          finish=False, didFail=False):
        """Helper function for testing apogee_dome_flat."""

        cmdState = self.actorState.doApogeeDomeFlat
        cmdState.reinitialize(self.cmd)
        result = slewThread.apogee_dome_flat(
            self.cmd, cmdState, myGlobals.actorState, multiCmd)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)
        self.assertEqual(result, not didFail)

    def test_apogee_dome_flat_gang_change(self):
        """Tests shutter open, FFS close, exposure +(ff on, ff off)."""
        name = 'apogeeDomeFlat'
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(4, 11, 0, 0, multiCmd)

    def test_apogee_dome_flat_enclosure(self):
        """Tests shutter open, exposure +(ff on, ff off)."""
        name = 'apogeeDomeFlat'
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_parked'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(3, 11, 0, 0, multiCmd)

    def test_apogee_dome_flat_enclosure_shutterOpen(self):
        """Tests exposure +(ff on, ff off)."""
        name = 'apogeeDomeFlat'
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_parked'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(2, 8, 0, 0, multiCmd)

    def test_apogee_dome_flat_ffs_fails(self):
        """Tests shutter open, ffs close -> fail."""
        name = 'apogeeDomeFlat'
        self.cmd.failOn = "mcp ffs.close"
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(2, 12, 1, 0, multiCmd,
                               finish=True, didFail=True)

    def test_apogee_dome_flat_gang_on_podium_fails(self):
        """Tests fail immediately."""
        name = 'apogeeDomeFlat'
        sopTester.updateModel('mcp', TestHelper.mcpState['all_off'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(0, 5, 0, 0, multiCmd, finish=True, didFail=True)

    def test_apogee_dome_flat_shuter_close_fails(self):
        """Tests shutter open, ffs close -> fail."""
        name = 'apogeeDomeFlat'
        self.cmd.failOn = "apogee shutter close"
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        multiCmd = MultiCommand(
            self.cmd, myGlobals.actorState.timeout + 50, name)
        self._apogee_dome_flat(4, 15, 0, 1, multiCmd,
                               finish=True, didFail=True)


class TestGotoGangChange(SlewThreadTester):
    """Test suite for goto_gang_change.

    NOTE: the cmd numbers here aren't right, because I haven't fully faked
          the apogee utrReadState thing, so the fflamps don't get called
          on+off.

    TBD: That would increase the nCalls in each of these by 2:
         One for ff.on and one for ff.off
    """

    def _goto_gang_change(self, nCall, nInfo, nWarn, nErr, finish=True,
                          didFail=False):
        """Helper functions for the gotoGangChange tests."""

        sopTester.updateModel('tcc', TestHelper.tccState['tracking'])
        cmdState = self.actorState.gotoGangChange
        cmdState.reinitialize(self.cmd)
        slewThread.goto_gang_change(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)

    def test_goto_gang_change_apogee_open(self):
        """Tests B open."""
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        self._goto_gang_change(7, 22, 0, 0)

    def test_goto_gang_change_apogee_closed(self):
        """Tests A closed."""
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['A_closed'])
        self._goto_gang_change(8, 22, 0, 0)

    def test_goto_gang_change_apogee_gang_podium(self):
        """Tests B open, all off."""
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel('mcp', TestHelper.mcpState['all_off'])
        self._goto_gang_change(4, 14, 0, 0)

    def test_goto_gang_change_boss(self):
        """Tests BOSS."""
        myGlobals.actorState.survey = sopActor.BOSS
        self._goto_gang_change(4, 14, 0, 0)

    def test_goto_gang_change_apogee_noSlew(self):
        """Tests B open, doSlew False."""
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        cmdState = self.actorState.gotoGangChange
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        slewThread.goto_gang_change(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(3, 16, 0, 0, True)

    def test_goto_gang_change_apogee_noDomeFlat(self):
        """Tests B open, no dome flat."""
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        cmdState = self.actorState.gotoGangChange
        cmdState.reinitialize(self.cmd)
        cmdState.doDomeFlat = False
        slewThread.goto_gang_change(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(5, 11, 0, 0, True)

    def test_goto_gang_change_apogee_bypass_gangToCart(self):
        """Tests bypass gangToCart.

        Testing for complaints from the observers about
        gang bypass and gang changes.
        """

        self._prep_bypass('gangToCart', clear=True)
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        self._goto_gang_change(7, 22, 3, 0)

    def test_goto_gang_change_apogee_bypass_gangToPodium(self):
        """Tests bypass gangToPodium.

        Testing for complaints from the observers about
        gang bypass and gang changes.
        """
        self._prep_bypass('gangToPodium', clear=True)
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        self._goto_gang_change(4, 14, 3, 0)

    def test_goto_gang_change_apogee_fails_domeflat(self):
        """Tests shutter open, FFS close, expose -> fail."""
        myGlobals.actorState.survey = sopActor.APOGEE
        self.cmd.failOn = "apogee expose time=50.0 object=DomeFlat"
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        self._goto_gang_change(2, 14, 0, 1, didFail=True)

    def test_goto_gang_change_apogee_fails_slew(self):
        """Tests failed slew."""
        self.cmd.failOn = "tcc axis init"
        myGlobals.actorState.survey = sopActor.APOGEE
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        self._goto_gang_change(5, 21, 0, 2, didFail=True)


class TestGotoPosition(SlewThreadTester):
    """Tests for GotoPosition."""

    def _goto_position(self, nCall, nInfo, nWarn, nErr, finish=True,
                       didFail=False, alt=30., az=121., rot=0.):
        """Helper method to launch GotoPosition tests."""

        cmdState = self.actorState.gotoPosition
        cmdState.reinitialize(self.cmd)
        cmdState.az = az
        cmdState.alt = alt
        cmdState.rot = rot
        slewThread.goto_position(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)

    def test_goto_60_60_60(self):
        """Goes to az=alt=rot=60 deg."""
        self._goto_position(4, 11, 0, 0, az=60, alt=60, rot=60)

    def test_goto_12_34_56(self):
        """Goes to "random" position, az=12, alt=34, rot=56 deg."""
        self._goto_position(4, 11, 0, 0, az=12, alt=34, rot=56)


if __name__ == '__main__':

    verbosity = 1

    if verbose:
        verbosity = 2

    suite = None
    # To test just one piece
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoGangChange)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestApogeeDomeFlat)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoPosition)

    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
