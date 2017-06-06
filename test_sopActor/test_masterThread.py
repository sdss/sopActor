"""
Test the various commands in SOP masterThread

NOTE: sometimes tests involving lamp warmup may fail by 1 info-level cmd. This
is probably because of small variations in threads completing, resulting in
certain lamp warmup messages sometimes being output, and sometimes not.
Particularly bad in this regard are the do_boss_calibs tests.
"""

import unittest
import threading
import multiprocessing as multi

from actorcore import TestHelper
import sopTester

import sopActor
import sopActor.myGlobals as myGlobals
import sopActor.CmdState as CmdState
from sopActor.multiCommand import MultiCommand

from sopActor import masterThread
from sopActor import bossThread
from sopActor import apogeeThread
from sopActor import guiderThread
from sopActor import ffsThread
from sopActor import lampThreads
from sopActor import tccThread

# False for less printing, True for more printing
verbose = True

class MasterThreadTester(sopTester.SopThreadTester,unittest.TestCase):
    """
    Tests for the various functions in sop masterThread, that were
    ripped out of the main loop during the bigCleanup...
    """
    def setUp(self):
        self.verbose = verbose
        self.useThreads = [("apogee",sopActor.APOGEE, apogeeThread.main),
                           ("apogeeScript",  sopActor.APOGEE_SCRIPT, apogeeThread.script_main),
                           ("boss",    sopActor.BOSS,     bossThread.main),
                           ("script",  sopActor.SCRIPT,   sopTester.FakeThread),
                           ("guider",  sopActor.GUIDER,   guiderThread.main),
                           ("gcamera", sopActor.GCAMERA,  sopTester.FakeThread),
                           ("ff",      sopActor.FF_LAMP,  lampThreads.ff_main),
                           ("hgcd",    sopActor.HGCD_LAMP,lampThreads.hgcd_main),
                           ("ne",      sopActor.NE_LAMP,  lampThreads.ne_main),
                           ("uv",      sopActor.UV_LAMP,  lampThreads.uv_main),
                           ("wht",     sopActor.WHT_LAMP, lampThreads.wht_main),
                           ("ffs",     sopActor.FFS,      ffsThread.main),
                           ("tcc",     sopActor.TCC,      tccThread.main),
                          ]
        super(MasterThreadTester,self).setUp()
        self.fail_on_no_cmd_calls = True # we need cmd_calls for all of these.


class TestGuider(MasterThreadTester):
    """guider_* tests"""
    def _guider_start(self,nCall,nInfo,nWarn,nErr,finish=False,didFail=False):
        """Helper for running guider start tests."""
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        result = masterThread.guider_start(self.cmd, cmdState, myGlobals.actorState, "gotoField")
        self.assertEqual(result,not didFail)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail=didFail)

    def test_guider_start_ffsClosed(self):
        """ffs open, 3x axis clear, guider on"""
        self._guider_start(5,18,0,0)
    def test_guider_start_ffsOpen(self):
        """3x axis clear, guider on"""
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._guider_start(4,15,0,0)
    def test_guider_start_arcsOn(self):
        """ffs open, he off, hgcd off, 3x axis clear, guider on"""
        sopTester.updateModel('mcp',TestHelper.mcpState['arcs'])
        self._guider_start(7,18,0,0)
    def test_guider_start_flatsOn(self):
        """ffs open, flat off, 3x axis clear, guider on"""
        sopTester.updateModel('mcp',TestHelper.mcpState['flats'])
        self._guider_start(6,18,0,0)
    def test_guider_start_fails(self):
        self.cmd.failOn = "guider on time=5"
        self._guider_start(5,13,0,1,finish=True,didFail=True)

    def _guider_flat(self,nCall,nInfo,nWarn,nErr,finish=False,didFail=False):
        """Helper for running guider flat tests."""
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        result = masterThread.guider_flat(self.cmd, cmdState, myGlobals.actorState, "guider")
        self.assertEqual(result,not didFail)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail=didFail)
    def test_guider_flat_ffsClosed(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self._guider_flat(2,18,0,0)
    def test_guider_flat_ffsOpen(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._guider_flat(3,18,0,0)
    def test_guider_flat_fails(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self.cmd.failOn = "guider flat time=0.5"
        self._guider_flat(2,13,0,1,finish=True,didFail=True)

    def _guider_flat_apogeeShutter(self,nCall,nInfo,nWarn,nErr,finish=False,didFail=False):
        """Helper for running guider flat tests that check the APOGEE shutter."""
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        result = masterThread.guider_flat(self.cmd, cmdState, myGlobals.actorState, "guider", apogeeShutter=True)
        self.assertEqual(result,not didFail)
        self._check_cmd(nCall,nInfo,nWarn,nErr, finish, didFail=didFail)
    def test_guider_flat_apogeeShutter_open(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._guider_flat_apogeeShutter(3,18,0,0)
    def test_guider_flat_apogeeShutter_closed(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        self._guider_flat_apogeeShutter(2,18,0,0)

    def _deactivate_guider_decenter(self,nCall,nInfo,nWarn,nErr, didFail=False):
        stageName = 'dither'
        cmdState = self.actorState.doMangaDither
        cmdState.reinitialize(self.cmd)
        result = masterThread.deactivate_guider_decenter(self.cmd, cmdState, myGlobals.actorState, stageName)
        self.assertEqual(result,not didFail)
        self._check_cmd(nCall, nInfo, nWarn, nErr, False, didFail=didFail)
    def test_deactivate_guider_decenter_on(self):
        """decenter off"""
        sopTester.updateModel('guider',TestHelper.guiderState['guiderOnDecenter'])
        self._deactivate_guider_decenter(1,6,0,0)
    def test_deactivate_guider_decenter_off(self):
        """(nothing: already off)"""
        self._deactivate_guider_decenter(0,4,0,0)
    def test_deactivate_guider_decenter_fails(self):
        """
        decenter off
        Will give cmd.error and stage=failed, but won't fail command.
        """
        sopTester.updateModel('guider',TestHelper.guiderState['guiderOnDecenter'])
        self.cmd.failOn="guider decenter off"
        self._deactivate_guider_decenter(1,9,0,2,didFail=True)


class TestGotoField(MasterThreadTester):
    """GotoField and slewing tests"""

    def setUp(self):
        super(TestGotoField, self).setUp()
        sopTester.updateModel('hartmann', TestHelper.hartmannState['default'])

    def test_start_slew(self):
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        result = masterThread.start_slew(self.cmd, cmdState, myGlobals.actorState, self.timeout)
        self.assertIsInstance(result,MultiCommand)
        self.assertEqual(result.timeout, self.timeout+myGlobals.actorState.timeout)
        self._check_cmd(0,3,0,0,False)

    def test_goto_field_unknown(self):

        mcpState = TestHelper.mcpState['all_off']
        mcpState.update({'instrumentNum': [15]})
        sopTester.updateModel('mcp', mcpState)

        cmdState = self.actorState.gotoField
        self.actorState.survey = sopActor.UNKNOWN
        masterThread.goto_field(self.cmd,cmdState,myGlobals.actorState)
        self._check_cmd(0, 3, 0, 0, True, True)

    def _goto_feld_apogee(self, nCall, nInfo, nWarn, nErr, cmdState):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        masterThread.goto_field_apogee(self.cmd,cmdState,myGlobals.actorState,self.timeout)
        self._check_cmd(nCall, nInfo, nWarn, nErr, False)
    def test_goto_field_apogee(self):
        """
        FF on, axis status, axis init, slew, FF on, guider flat, FF off, open FFS
        3xguider axes off, guider on
        """
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_feld_apogee(12,43,0,0,cmdState)
    def test_goto_field_apogee_no_guider(self):
        """axis status, axis init, slew"""
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doGuider = False
        self._goto_feld_apogee(3,11,0,0,cmdState)
    def test_goto_field_apogee_no_slew(self):
        """
        FF on, guider flat, FF off, open FFS
        3xguider axes off, guider on
        """
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        self._goto_feld_apogee(8,34,0,0,cmdState)
    def test_goto_field_apogee_no_slew_decenter_off(self):
        """
        FF on, guider flat, FF off, open FFS
        guider decenter off, 3xguider axes off, guider on
        """
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        sopTester.updateModel('guider',TestHelper.guiderState['guiderOnDecenter'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        self._goto_feld_apogee(9,35,0,0,cmdState)
    def test_goto_field_apogee_bypass_gangToCart(self):
        """Testing for a potential problem on SJD 56993,
        with gotoField not slewing when gang bypass had been set."""
        self._prep_bypass('gangToCart',clear=True)
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_feld_apogee(12,41,4,0,cmdState)


    def test_goto_field_apogee_no_slew_shutter_open(self):
        """
        shutter close, FF on, guider flat, FF off, open FFS
        3xguider axes off, guider on
        """
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        self._goto_feld_apogee(9,34,0,0,cmdState)

    def _goto_field_boss(self, nCall, nInfo, nWarn, nErr, cmdState, finish=False, didFail=False):
        masterThread.goto_field_boss(self.cmd,cmdState,myGlobals.actorState,self.timeout)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)
    def test_goto_field_boss_all(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_field_boss(25,99,0,0,cmdState)
    def test_goto_field_boss_slew(self):
        """
        axis status, axis init, slew
        """
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doGuider = False
        cmdState.doHartmann = False
        cmdState.doCalibs = False
        cmdState.arcTime = 0
        cmdState.flatTime = 0
        self._goto_field_boss(3,25,0,0,cmdState)
    def test_goto_field_boss_hartmann(self):
        """
        ne on, hgcd on, ff off, doHartmann, ne off, hgcd off
        """
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        cmdState.doCalibs = False
        cmdState.arcTime = 0
        cmdState.flatTime = 0
        cmdState.doGuider = False
        self._goto_field_boss(5,28,0,0,cmdState)
    def test_goto_field_boss_calibs(self):
        """
        see cmd_calls/TestGotoField.txt for command list.
        """
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        cmdState.doHartmann = False
        cmdState.doGuider = False
        self._goto_field_boss(10,55,0,0,cmdState)
    def test_goto_field_boss_guider(self):
        """
        Start with decentered guiding on, to check that we clear it.
        ff on, guider flat, ff off, ffs open 3xguider axes off, decenter off, guider on
        """
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        sopTester.updateModel('guider',TestHelper.guiderState['guiderOnDecenter'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        cmdState.doSlew = False
        cmdState.doHartmann = False
        cmdState.doCalibs = False
        cmdState.arcTime = 0
        cmdState.flatTime = 0
        self._goto_field_boss(9,35,0,0,cmdState)

    def test_goto_field_boss_flat_on_fails(self):
        """Fail on ff.on, but still readout the arc."""
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self.cmd.failOn = "mcp ff.on"
        self._goto_field_boss(16,68,0,1,cmdState,didFail=True,finish=True)
    def test_goto_field_boss_ne_on_fails(self):
        """Fail on ne.on."""
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self.cmd.failOn = "mcp ne.on"
        self._goto_field_boss(6,15,0,1,cmdState,didFail=True,finish=True)
    def test_goto_field_boss_hartmann_fails(self):
        """Fail on hartmann."""
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self.cmd.failOn = "hartmann collimate ignoreResiduals"
        # TBD: NOTE: Something wrong with this test!
        # Should produce 0 errors, but the failure usually (not always!)
        # cascades through to hgcd lampThread.
        # I'm pretty sure that's not correct.
        self._goto_field_boss(9,33,0,1,cmdState,didFail=True,finish=True)

    def test_goto_field_boss_hartmann_blue_fails(self):
        """Hartmann succeeds but the blue ring move is out of tolerance."""

        sopTester.updateModel('mcp', TestHelper.mcpState['all_off'])
        sopTester.updateModel('hartmann', TestHelper.hartmannState['blue_fails'])

        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)

        self._goto_field_boss(12, 36, 0, 0, cmdState, didFail=True, finish=True)

    def test_goto_field_boss_ffs_open_fails(self):
        """Fail on ffs.open, but still readout flat."""
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self.cmd.failOn = "mcp ffs.open"
        self._goto_field_boss(21,98,1,1,cmdState,didFail=True,finish=True)

    def _goto_field_apogeemanga(self, nCall, nInfo, nWarn, nErr, cmdState,
                                finish=False, didFail=False, surveyMode=None):
        myGlobals.actorState.survey = sopActor.APOGEEMANGA
        myGlobals.actorState.surveyMode = surveyMode or sopActor.MANGADITHER
        masterThread.goto_field_apogeemanga(self.cmd,cmdState,myGlobals.actorState,self.timeout)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)

    def test_goto_field_apogeemanga_all(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_field_apogeemanga(25,99,0,0,cmdState)

    def test_goto_field_apogeemanga_all_shutter_open(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_parked'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_field_apogeemanga(26,106,0,0,cmdState)

    def test_goto_field_apogeemanga_apogee_lead_hartmann_out_of_focus(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['all_off'])
        sopTester.updateModel('hartmann', TestHelper.hartmannState['blue_fails'])
        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)
        self._goto_field_apogeemanga(25, 99, 1, 0, cmdState, surveyMode=sopActor.APOGEELEAD)

    def test_goto_field_cartridge_mismatch(self):
        """Tests gotoField if there is a mismatch between MCP and guider."""

        sopTester.updateModel('guider', TestHelper.guiderState['bossLoaded'])

        mcpState = TestHelper.mcpState['boss_science']
        mcpState.update({'instrumentNum': [15]})
        sopTester.updateModel('mcp', mcpState)

        cmdState = self.actorState.gotoField
        cmdState.reinitialize(self.cmd)

        masterThread.goto_field(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(0, 13, 0, 0, finish=True, didFail=True)


class TestApogeeSkyFlats(MasterThreadTester):
    """do_apogee_sky_flats tests"""
    def _do_apogee_sky_flats(self,nCall,nInfo,nWarn,nErr, didFail=False):
        cmdState = self.actorState.doApogeeSkyFlats
        cmdState.reinitialize(self.cmd)
        masterThread.do_apogee_sky_flats(self.cmd,cmdState,myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,True,didFail)
    def test_do_apogee_sky_flats(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('guider',TestHelper.guiderState['guiderOn'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_apogee_sky_flats(8,46,0,0)
    def test_do_apogee_sky_flats_fails_tcc(self):
        self.cmd.failOn = 'tcc offset arc 0.01,0.0'
        self._do_apogee_sky_flats(1,6,0,0, didFail=True)
    def test_do_apogee_sky_flats_ok_tcc_bypassed(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        self.cmd.failOn = 'tcc offset arc 0.01,0.0'
        self._prep_bypass('axes',clear=True)
        self._do_apogee_sky_flats(8,44,2,0)



class TestHartmann(MasterThreadTester):
    """hartmann tests"""
    def _hartmann(self,nCall,nInfo,nWarn,nErr, finish=True, didFail=False):
        cmdState = self.actorState.hartmann
        cmdState.reinitialize(self.cmd)
        masterThread.hartmann(self.cmd,cmdState,myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,finish,didFail)
    def test_hartmann_open(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._hartmann(9,53,0,0)
    def test_hartmann_closed(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self._hartmann(7,52,0,0)
    def test_hartmann_fails(self):
        self.cmd.failOn="boss exposure arc itime=4 hartmann=left"
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self._hartmann(4,22,0,0,didFail=True)
    def test_hartmann_fails_cleanup(self):
        self.cmd.failOn="mcp ne.off"
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self._hartmann(7,52,0,1,didFail=True)


class TestCollimateBoss(MasterThreadTester):
    """hartmann tests"""
    def _collimate_boss(self,nCall,nInfo,nWarn,nErr, finish=True, didFail=False):
        cmdState = self.actorState.collimateBoss
        cmdState.reinitialize(self.cmd)
        masterThread.collimate_boss(self.cmd,cmdState,myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,finish,didFail)
    def test_collimate_boss_open(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._collimate_boss(10,41,0,0)
    def test_collimate_boss_closed(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        self._collimate_boss(8,40,0,0)
    def test_collimate_boss_fails(self):
        self.cmd.failOn="hartmann collimate ignoreResiduals noSubFrame"
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._collimate_boss(7,25,0,1,didFail=True)
    def test_collimate_boss_ffs_close_fails(self):
        self.cmd.failOn="mcp ffs.close"
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        self._collimate_boss(4,16,1,0,didFail=True)


class TestBossScience(MasterThreadTester):
    """do_boss_science tests"""
    def _do_boss_science(self, nCall, nInfo, nWarn, nErr, nExp=1):
        """Helper for boss science tests"""
        self._update_cart(11, 'BOSS')
        cmdState = self.actorState.doBossScience
        cmdState.reinitialize(self.cmd)
        cmdState.nExp = nExp
        masterThread.do_boss_science(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(2,nInfo,nWarn,nErr,True)

    def test_do_boss_science(self):
        """One call per requested exposure"""
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        nExp = 2
        self._do_boss_science(nExp,34,0,0,nExp=nExp)


class TestApogeeScience(MasterThreadTester):
    """do_apogee_science tests"""
    def _get_next_apogee_dither_pair(self, expect):
        dithers = masterThread.get_next_apogee_dither_pair(myGlobals.actorState)
        self.assertEqual(dithers,expect)
    def test_get_next_apogee_dither_pair_AB(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        self._get_next_apogee_dither_pair('AB')
    def test_get_next_apogee_dither_pair_BA(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._get_next_apogee_dither_pair('BA')

    def _do_apogee_science(self, nCall, nInfo, nWarn, nErr, ditherPairs=4):
        """Helper for apogee science tests"""
        self._update_cart(1, 'APOGEE')
        cmdState = self.actorState.doApogeeScience
        cmdState.reinitialize(self.cmd)
        cmdState.ditherPairs = ditherPairs
        masterThread.do_apogee_science(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,True)
    def test_do_apogee_science_4_pair_A_closed(self):
        """open shutter, one call per exposure/dither moves"""
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        self._do_apogee_science(13,71,0,0,ditherPairs=4)
    def test_do_apogee_science_1_pair_B_open(self):
        """one call per exposure/dither move"""
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_apogee_science(3,29,0,0,ditherPairs=1)

    def test_do_apogee_science_500s_after_1000s_cart7(self):
        """Tests if expTime is set to 500s after a double length exposure."""

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead1000sCart7'])
        self._update_cart(7, 'APOGEE')
        cmdState = self.actorState.doApogeeScience
        cmdState.reinitialize(self.cmd)
        self.assertEqual(cmdState.expTime, 1000)
        self.assertEqual(cmdState.keywords['expTime'], 1000)

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLeadCart7'])
        self._update_cart(7, 'APOGEE')
        cmdState = self.actorState.doApogeeScience
        cmdState.reinitialize(self.cmd)
        self.assertEqual(cmdState.expTime, 500)
        self.assertEqual(cmdState.keywords['expTime'], 500)

    def test_do_apogee_science_1000s_after_500s_cart7(self):
        """Tests if expTime is set to 1000s after a normal length exposure."""

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLeadCart7'])
        self._update_cart(7, 'APOGEE')
        cmdState = self.actorState.doApogeeScience
        cmdState.reinitialize(self.cmd)
        self.assertEqual(cmdState.expTime, 500)
        self.assertEqual(cmdState.keywords['expTime'], 500)

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead1000sCart7'])
        self._update_cart(7, 'APOGEE')
        cmdState = self.actorState.doApogeeScience
        cmdState.reinitialize(self.cmd)
        self.assertEqual(cmdState.expTime, 1000)
        self.assertEqual(cmdState.keywords['expTime'], 1000)


class TestMangaScience(MasterThreadTester):
    """do_manga_* tests"""

    def _do_one_manga_dither(self, nCall, nInfo, nWarn, nErr, dither='N', didFail=False):
        self._update_cart(1, 'MaNGA', 'MaNGA dither')
        cmdState = self.actorState.doMangaDither
        cmdState.reinitialize(self.cmd)
        cmdState.dither = dither
        success = masterThread.do_one_manga_dither(self.cmd, cmdState, myGlobals.actorState)
        self.assertEqual(success, not didFail)
        self._check_cmd(nCall, nInfo, nWarn, nErr, False, didFail=didFail)

    def test_do_one_manga_dither(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        dither = 'N'
        self._do_one_manga_dither(3, 19, 0, 0, dither=dither)

    def test_do_one_manga_dither_fails_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        self.cmd.failOn = 'boss exposure science itime=900'
        dither = 'N'
        self._do_one_manga_dither(3, 19, 0, 1, dither=dither, didFail=True)

    def _do_manga_dither(self, nCall, nInfo, nWarn, nErr, dither='N', didFail=False):
        cmdState = self.actorState.doMangaDither
        cmdState.reinitialize(self.cmd)
        cmdState.dither = dither
        masterThread.do_manga_dither(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall, nInfo, nWarn, nErr, True, didFail=didFail)

    def test_do_manga_dither(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        dither = 'N'
        self._do_manga_dither(4, 27, 0, 0, dither=dither)

    def test_do_manga_dither_after_sequence(self):
        """See ticket #2107 for the bug that this tickles."""
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        dither = 'N'
        cmdState = self.actorState.doMangaSequence
        cmdState.reinitialize(self.cmd)
        cmdState.count = 1
        cmdState.dithers = 'NSE'
        cmdState.reset_ditherSeq()
        self.cmd.verbose = False
        masterThread.do_apogeemanga_sequence(self.cmd, cmdState, myGlobals.actorState)
        self.cmd.reset()
        self.cmd.verbose = self.verbose
        self._do_manga_dither(4, 27, 0, 0, dither=dither)

    def test_do_manga_dither_fails_ffs(self):
        self.cmd.failOn = "mcp ffs.open"
        dither = 'S'
        self._do_manga_dither(4, 27, 1, 0, dither=dither, didFail=True)

    def test_do_manga_dither_fails_dither(self):
        self.cmd.failOn = "guider mangaDither ditherPos=S"
        dither = 'S'
        self._do_manga_dither(4, 27, 0, 1, dither=dither, didFail=True)

    def _do_manga_sequence(self, nCall, nInfo, nWarn, nErr, count, dithers='NSE', didFail=False, checkcmds=True):
        self._update_cart(1, 'MaNGA')
        cmdState = self.actorState.doMangaSequence
        cmdState.reinitialize(self.cmd)
        cmdState.count = count
        cmdState.dithers = dithers
        cmdState.reset_ditherSeq()
        masterThread.do_manga_sequence(self.cmd, cmdState, myGlobals.actorState)
        if checkcmds:
            self._check_cmd(nCall, nInfo, nWarn, nErr, True, didFail=didFail)
        self.assertTrue(self.cmd.finished)

    def test_do_manga_sequence(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        count = 3
        dithers = 'NSE'
        self._do_manga_sequence(29, 239, 0, 0, count, dithers)

    def test_do_manga_sequence_one_set(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        count = 1
        dithers = 'NSE'
        self._do_manga_sequence(11, 95, 0, 0, count, dithers)

    def test_do_manga_sequence_two_sets(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        count = 2
        dithers = 'NSE'
        self._do_manga_sequence(20, 167, 0, 0, count, dithers)

    # Spawn a new process
    def _start_thread(self, method, wait=2, mp=None, name='masterthread'):
        # threads or multiprocessing
        if mp:
            # multiprocess
            self.newproc = multi.Process(target=method, name=name)
        else:
            # threads
            self.newproc = threading.Thread(target=method, name=name)
        # start the process
        self.newproc.start()

    def test_do_manga_sequence_fails_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])
        self.cmd.failOn = 'boss exposure science itime=900 noreadout'
        count = 3
        dithers = 'NSE'
        self._do_manga_sequence(5, 44, 0, 1, count, dithers, didFail=True)

    def test_do_manga_sequence_modifycount_on_dither(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 2
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('guider mangaDither ditherPos=E', update_counts)
        self.cmd.runOnCount = 1

        count = 1
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(20, 167, 0, 0, count, dithers)

    def test_do_manga_sequence_modifycount_on_boss(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 2
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('boss exposure science itime=900 noreadout', update_counts)
        self.cmd.runOnCount = 2

        count = 1
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(20, 167, 0, 0, count, dithers)

    def test_do_manga_sequence_modifycount_on_readout(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 2
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('boss exposure   readout', update_counts)
        self.cmd.runOnCount = 3

        count = 1
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(20, 170, 0, 0, count, dithers)

    def test_do_manga_sequence_modifycount_2to1(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 1
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 1

        count = 2
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(11, 95, 0, 0, count, dithers)

    def test_do_manga_sequence_modifycount_3to1(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 1
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 1

        count = 3
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(11, 95, 0, 0, count, dithers)

    def test_do_manga_sequence_modifycount_2to3(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['boss_science'])

        def update_counts():
            cmdState.count = 3
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 2

        count = 2
        dithers = 'NSE'
        cmdState = self.actorState.doMangaSequence
        self._do_manga_sequence(29, 239, 0, 0, count, dithers)


class TestApogeeMangaScience(MasterThreadTester):
    def _do_one_apogeemanga_dither(self, nCall, nInfo, nWarn, nErr, mangaDither,
                                   apogee_long=False):
        self._update_cart(1, 'APOGEE&MaNGA')
        cmdState = self.actorState.doApogeeMangaDither
        cmdState.reinitialize(self.cmd)
        cmdState.mangaDither = mangaDither
        cmdState.apogee_long = apogee_long
        masterThread.do_one_apogeemanga_dither(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,False)
    def test_do_one_apogeemanga_dither_at_BC(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        mangaDither = 'N'
        self._do_one_apogeemanga_dither(6,19,0,0, mangaDither)
    def test_do_one_apogeemanga_dither_at_AC_shutter_closed(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        mangaDither = 'N'
        self._do_one_apogeemanga_dither(7,19,0,0, mangaDither)
    def test_do_one_apogeemanga_dither_apogee_lead(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        mangaDither = 'C'
        self._do_one_apogeemanga_dither(5,18,0,0, mangaDither)

    def _do_apogeemanga_dither(self, nCall, nInfo, nWarn, nErr, mangaDither,
                               didFail=False, surveyMode='MaNGA dither',
                               apogee_long=False):
        self._update_cart(1, 'APOGEE&MaNGA', surveyMode=surveyMode)
        cmdState = self.actorState.doApogeeMangaDither
        cmdState.reinitialize(self.cmd)
        cmdState.mangaDither = mangaDither
        cmdState.apogee_long = apogee_long
        if surveyMode == 'APOGEE lead' and apogee_long:
            cmdState.set_apogeeLead(apogeeExpTime=1000.)
        masterThread.do_apogeemanga_dither(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,finish=True,didFail=didFail)
    def test_do_apogeemanga_dither_at_BC(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        mangaDither = 'N'
        self._do_apogeemanga_dither(7,36,0,0, mangaDither)
    def test_do_apogeemanga_dither_gang_at_podium(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        mangaDither = 'N'
        self._do_apogeemanga_dither(0,6,0,0, mangaDither, didFail=True)
    def test_do_apogeemanga_dither_after_sequence(self):
        """See ticket #2107 for the bug that this tickles."""
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        self._update_cart(1, 'APOGEE&MaNGA', 'MaNGA dither')
        mangaDither = 'N'
        cmdState = self.actorState.doApogeeMangaSequence
        cmdState.reinitialize(self.cmd)
        cmdState.count = 1
        cmdState.mangaDithers = 'NSE'
        cmdState.reset_ditherSeq()
        self.cmd.verbose = False
        masterThread.do_apogeemanga_sequence(self.cmd, cmdState, myGlobals.actorState)
        self.cmd.reset()
        self.cmd.verbose = self.verbose
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_apogeemanga_dither(7,36,0,0, mangaDither)
    def test_do_apogeemanga_dither_apogee_lead_long_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        mangaDither = 'C'
        self._do_apogeemanga_dither(7, 36, 0, 0, mangaDither,
                                    surveyMode='APOGEE lead', apogee_long=True)

    def test_do_apogeemanga_dither_guider_dither_fails(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self.cmd.failOn = 'guider mangaDither ditherPos=N'
        mangaDither = 'N'
        self._do_apogeemanga_dither(3,21,0,1, mangaDither, didFail=True)

    def test_do_apogeemanga_dither_fails_long_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        self.cmd.failOn = 'boss exposure science itime=900'
        self.cmd.failOnCount = 2
        mangaDither = 'C'
        self._do_apogeemanga_dither(7, 28, 0, 1, mangaDither, didFail=True,
                                    apogee_long=True, surveyMode='APOGEE lead')


    def _do_apogeemanga_sequence(self,nCall,nInfo,nWarn,nErr, mangaDithers,
                                  count, didFail=False, surveyMode='MaNGA dither',
                                  apogee_long=False, checkCall=True):

        cmdState = self.actorState.doApogeeMangaSequence
        cmdState.reinitialize(self.cmd)
        self._update_cart(1, 'APOGEE&MaNGA', surveyMode)
        cmdState.count = count
        cmdState.mangaDithers = mangaDithers
        cmdState.reset_ditherSeq()
        masterThread.do_apogeemanga_sequence(self.cmd, cmdState, myGlobals.actorState)
        if checkCall:
            self._check_cmd(nCall,nInfo,nWarn,nErr,True,didFail=didFail)

    def test_do_apogeemanga_sequence_count_1(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        mangaDithers = 'NSE'
        count = 1
        self._do_apogeemanga_sequence(20,99,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_count_2(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        mangaDithers = 'NSE'
        count = 2
        self._do_apogeemanga_sequence(38,177,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_modifycount_on_dither(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])

        def update_counts():
            cmdState.count = 2
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()

        cmdState = self.actorState.doApogeeMangaSequence
        mangaDithers = 'NSE'
        count = 1
        self.cmd.runOn = ('guider mangaDither ditherPos=E', update_counts)
        self.cmd.runOnCount = 1
        self._do_apogeemanga_sequence(38,177,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_modifycount_on_readout(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])

        def update_counts():
            cmdState.count = 2
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()

        cmdState = self.actorState.doApogeeMangaSequence
        mangaDithers = 'NSE'
        count = 1
        self.cmd.runOn = ('boss exposure   readout', update_counts)
        self.cmd.runOnCount = 3
        self._do_apogeemanga_sequence(38,180,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_modifycount_2to1(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])

        def update_counts():
            cmdState.count = 1
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()

        cmdState = self.actorState.doApogeeMangaSequence
        mangaDithers = 'NSE'
        count = 2
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 1
        self._do_apogeemanga_sequence(20,99,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_modifycount_3to1(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])

        def update_counts():
            cmdState.count = 1
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()

        cmdState = self.actorState.doApogeeMangaSequence
        mangaDithers = 'NSE'
        count = 3
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 1
        self._do_apogeemanga_sequence(20,99,0,0, mangaDithers, count)

    def test_do_apogeemanga_sequence_modifycount_2to3(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])

        def update_counts():
            cmdState.count = 3
            cmdState.dithers = 'NSE'
            cmdState.reset_ditherSeq()

        cmdState = self.actorState.doApogeeMangaSequence
        mangaDithers = 'NSE'
        count = 2
        self.cmd.runOn = ('guider mangaDither ditherPos=S', update_counts)
        self.cmd.runOnCount = 2
        self._do_apogeemanga_sequence(56,255,0,0, mangaDithers, count)


    def test_do_apogeemanga_sequence_count_2_shutter_closed_at_A(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['A_closed'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        mangaDithers = 'NSE'
        count = 2
        self._do_apogeemanga_sequence(39,177,0,0, mangaDithers, count)
    def test_do_apogeemanga_sequence_gang_podium(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        mangaDithers = 'NSE'
        count = 1
        self._do_apogeemanga_sequence(0,4,0,0, mangaDithers, count, didFail=True)
    def test_do_apogeemanga_sequence_apogee_lead_count1_CC(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        mangaDithers = 'CC'
        count = 1
        self._do_apogeemanga_sequence(10,63,0,0, mangaDithers, count, surveyMode='APOGEE lead')

    def test_do_apogeemanga_sequence_apogee_lead_count1_CC_long_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apogeeLead1000s'])

        mangaDithers = 'CC'
        count = 1
        self._do_apogeemanga_sequence(7, 43, 0, 0, mangaDithers, count,
                                      surveyMode='APOGEE lead')

    def test_do_apogeemanga_sequence_apogee_lead_count2_CC_long_exposure(self):
        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apogeeLead1000s'])

        mangaDithers = 'CC'
        count = 2
        self._do_apogeemanga_sequence(12, 65, 0, 0, mangaDithers, count,
                                      surveyMode='APOGEE lead')

    def test_do_manga_led_sequence_after_apogee_long_lead_sequence(self):

        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apogeeLead1000s'])

        mangaDithers = 'CC'
        count = 1
        self._do_apogeemanga_sequence(0, 0, 0, 0, mangaDithers, count,
                                      surveyMode='APOGEE lead',
                                      checkCall=False)

        sopTester.updateModel('mcp', TestHelper.mcpState['apogee_science'])
        sopTester.updateModel('apogee', TestHelper.apogeeState['B_open'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apgoeemangaDither'])

        mangaDithers = 'NSE'
        count = 1
        self._do_apogeemanga_sequence(20, 99, 1, 0, mangaDithers, count,
                                      surveyMode='MaNGA dither',
                                      checkCall=True)


class TestBossCalibs(MasterThreadTester):
    """do_boss_calibs tests"""
    def _do_boss_calibs(self, nCall, nInfo, nWarn, nErr, cmdState, didFail=False):
        cmdState.setupCommand(self.cmd)
        masterThread.do_boss_calibs(self.cmd, cmdState, myGlobals.actorState)
        self._check_cmd(nCall,nInfo,nWarn,nErr,True,didFail=didFail)

    def test_do_boss_calibs_one_bias(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nBias = 1
        self._do_boss_calibs(4,24,0,0,cmdState)
    def test_do_boss_calibs_two_bias(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nBias = 2
        self._do_boss_calibs(5,38,0,0,cmdState)
    def test_do_boss_calibs_one_bias_ffs_open(self):
        """For #371, not closing FFS for bias/dark."""
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nBias = 1
        self._do_boss_calibs(4,24,0,0,cmdState)

    def test_do_boss_calibs_one_dark(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nDark = 1
        self._do_boss_calibs(4,24,0,0,cmdState)
    def test_do_boss_calibs_two_dark(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nDark = 2
        self._do_boss_calibs(5,38,0,0,cmdState)
    def test_do_boss_calibs_one_dark_ffs_open(self):
        """For #371, not closing FFS for bias/dark."""
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nDark = 1
        self._do_boss_calibs(4,24,0,0,cmdState)

    def test_do_boss_calibs_one_flat(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nFlat = 1
        self._do_boss_calibs(7,30,0,0,cmdState)
    def test_do_boss_calibs_two_flat(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nFlat = 2
        self._do_boss_calibs(13,50,0,0,cmdState)
    def test_do_boss_calibs_one_flat_coobserve(self):
        """coobserving carts should close the apogee shutter first."""
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nFlat = 1
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_parked'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_boss_calibs(8,37,0,0,cmdState)
    def test_do_boss_calibs_one_flat_coobserve_gangPodium(self):
        """
        Coobserving carts should not bother with the apogee shutter when the
        gang connector is not at the cart.
        """
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nFlat = 1
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('mcp',TestHelper.mcpState['all_off'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_boss_calibs(7,30,0,0,cmdState)
    def test_do_boss_calibs_one_flat_ffs_open(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nFlat = 1
        self._do_boss_calibs(9,30,0,0,cmdState)

    def test_do_boss_calibs_one_arc(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nArc = 1
        self._do_boss_calibs(7,31,0,0,cmdState)
    def test_do_boss_calibs_two_arc(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nArc = 2
        self._do_boss_calibs(12,49,0,0,cmdState)
    def test_do_boss_calibs_one_arc_coobserve(self):
        """coobserving carts should close the apogee shutter first."""
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nArc = 1
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('mcp',TestHelper.mcpState['apogee_parked'])
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_boss_calibs(8,38,0,0,cmdState)
    def test_do_boss_calibs_one_arc_ffs_open(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['boss_science'])
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nArc = 1
        self._do_boss_calibs(9,31,0,0,cmdState)

    def test_do_boss_calibs_one_of_each(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nBias = 1
        cmdState.nDark = 1
        cmdState.nFlat = 1
        cmdState.nArc = 1
        self._do_boss_calibs(16,82,0,0,cmdState)
    def test_do_boss_calibs_two_of_each(self):
        cmdState = CmdState.DoBossCalibsCmd()
        cmdState.nBias = 2
        cmdState.nDark = 2
        cmdState.nFlat = 2
        cmdState.nArc = 2
        self._do_boss_calibs(29,148,0,0,cmdState)

    def test_do_boss_calibs_flat_arc_fail_on_hgcd(self):
        cmdState = CmdState.DoBossCalibsCmd()
        self.cmd.failOn = "mcp hgcd.on"
        cmdState.nFlat = 1
        cmdState.nArc = 1
        self._do_boss_calibs(7,38,0,1,cmdState,didFail=True)
        self.assertFalse(cmdState.isSlewingDisabled())
    def test_do_boss_calibs_two_flat_fail_on_readout(self):
        cmdState = CmdState.DoBossCalibsCmd()
        self.cmd.failOn = "boss exposure   readout"
        cmdState.nFlat = 2
        self._do_boss_calibs(7,38,0,1,cmdState,didFail=True)
        self.assertFalse(cmdState.isSlewingDisabled())
    def test_do_boss_calibs_two_arc_fail_on_second_exposure(self):
        cmdState = CmdState.DoBossCalibsCmd()
        self.cmd.failOn = "boss exposure arc itime=4 noreadout"
        self.cmd.failOnCount = 2
        cmdState.nArc = 2
        self._do_boss_calibs(9,48,0,1,cmdState,didFail=True)
        self.assertFalse(cmdState.isSlewingDisabled())


if __name__ == '__main__':
    verbosity = 1
    if verbose:
        verbosity = 2

    suite = None
    # to test just one piece
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGuider)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoField)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestApogeeSkyFlats)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoGangChange)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestApogeeDomeFlat)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestApogeeScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestBossScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestHartmann)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestCollimateBoss)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMangaScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestApogeeMangaScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestBossCalibs)
    # suite = unittest.TestLoader().loadTestsFromName('test_masterThread.TestGotoGangChange.test_goto_gang_change_apogee_bypass_gangToCart')
    # suite = unittest.TestLoader().loadTestsFromName('test_masterThread.TestBossCalibs.test_do_boss_calibs_one_arc_ffs_open')
    # suite = unittest.TestLoader().loadTestsFromName('test_masterThread.TestGotoField.test_goto_field_boss_all')
    # suite = unittest.TestLoader().loadTestsFromName('test_masterThread.TestApogeeMangaScience.test_do_apogeemanga_sequence_apogee_lead_count1')
    # suite = unittest.TestLoader().loadTestsFromName('test_masterThread.TestMangaScience.test_do_manga_dither_after_sequence')
    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
