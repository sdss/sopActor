"""
Test creating, modifying, and destroying the many different CmdStates.
"""

import unittest
import abc

import sopActor

from actorcore import TestHelper
import sopTester

class TestKeywords(sopTester.SopTester,unittest.TestCase):
    """Test (re)setting defined keywords, initialization, in a generic cmdState."""
    def setUp(self):
        self.userKeys = False
        self.verbose = True
        self.ok_stage = ''
        self.ok_state = 'done'
        self.stages = ['1','2']
        self.keywords = {'a':1,'b':2}
        self.cmdState = sopActor.CmdState.CmdState('tester',self.stages,keywords=self.keywords)
        super(TestKeywords,self).setUp()

    def test_reset_keywords(self):
        self.cmdState.a = 100
        self.cmdState.b = 200
        self.cmdState.reset_keywords()
        for n in self.keywords:
            self.assertEquals(getattr(self.cmdState,n),self.keywords[n])

    def test_reinitialize(self):
        self.cmdState.setStageState('1','running')
        self.cmdState.setStageState('2','aborted')
        self.cmdState.aborted = True
        self.actorState.aborting = True
        self.cmdState.reinitialize()
        for n in self.stages:
            self.assertEquals(self.cmdState.stages[n],'idle')
        self.assertFalse(self.cmdState.aborted)
        self.assertFalse(self.actorState.aborting)

    def test_set_item_ok(self):
        x = 1000
        self.cmdState.set('a',x)
        self.assertEqual(self.cmdState.a,x)
    def test_set_item_invalid(self):
        with self.assertRaises(AssertionError):
            self.cmdState.set('not_valid',-1)
    def test_set_item_default(self):
        self.cmdState.set('a',None)
        self.assertEqual(self.cmdState.a,self.cmdState.keywords['a'])


class CmdStateTester(sopTester.SopTester):
    def setUp(self):
        if not hasattr(self, 'userKeys'):
            self.userKeys = False
        self.verbose = True
        self.ok_stage = ''
        self.ok_state = 'done'
        super(CmdStateTester,self).setUp()
        self.inform = 3 + (1 if self.userKeys else 0)

    def test_setStageState_ok(self):
        """Valid stage states output one msg: State"""
        stage = self.ok_stage
        state = self.ok_state
        self.cmdState.setStageState(stage,state)
        self.assertEqual(self.cmdState.stages[stage],state)
        self.assertEqual(self.cmd.levels.count('i'),1)

    def test_setStageState_badStage(self):
        with self.assertRaises(AssertionError):
            self.cmdState.setStageState('not_a_state!',self.ok_state)
    def test_setStageState_badState(self):
        with self.assertRaises(AssertionError):
            self.cmdState.setStageState(self.ok_stage,'bad')

    def test_setCommandState_running(self):
        """
        setCommandState outputs 3 or 4 inform level commands:
        Stages, State, StateKeys [UserKeys].
        """
        state = 'running'
        self.cmdState.setCommandState(state)
        self.assertEqual(self.cmdState.cmdState,state)
        self.assertEqual(self.cmd.levels.count('i'),self.inform)
        self.assertEqual(self.cmd.levels.count('w'),0)
    def test_setCommandState_failed(self):
        state = 'failed'
        stateText = 'I failed!'
        self.cmdState.setCommandState(state,stateText=stateText)
        self.assertEqual(self.cmdState.cmdState,state)
        self.assertEqual(self.cmdState.stateText,stateText)
        self.assertEqual(self.cmd.levels.count('i'),self.inform)
        self.assertEqual(self.cmd.levels.count('w'),0)

    def test_took_exposure(self):
        self.cmdState.index = 0
        self.cmdState.took_exposure()
        self.assertEqual(self.cmdState.index, 1)
        self._check_cmd(0,self.inform,0,0,False)

    def test_isSlewingDisabled_BOSS_integrating(self):
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        result,text = self.cmdState.isSlewingDisabled_BOSS()
        self.assertTrue(result)
        self.assertEqual(text,'; exposureState=INTEGRATING')
    def test_isSlewingDisabled_BOSS_no(self):
        sopTester.updateModel('boss',TestHelper.bossState['reading'])
        result,text = self.cmdState.isSlewingDisabled_BOSS()
        self.assertFalse(result)
        self.assertEqual(text,'; exposureState=READING')

    def _isSlewingDisabled_because_exposing(self, survey, nExp, state):
        self.cmdState.cmd = self.cmd
        result = self.cmdState.isSlewingDisabled()
        self.assertIsInstance(result,str)
        self.assertIn('slewing disallowed for %s'%survey,result)
        self.assertIn('with %d science exposures left; exposureState=%s'%(nExp,state),result)

    def _isSlewingDisabled_False(self):
        self.cmdState.cmd = self.cmd
        result = self.cmdState.isSlewingDisabled()
        self.assertFalse(result)

    def _isSlewingDisabled_no_cmd(self):
        """
        Enable this by making a test_*() function that calls this.
        (this command isn't valid for all cmdStates)
        """
        self.cmdState.cmd = None
        result = self.cmdState.isSlewingDisabled()
        self.assertFalse(result)
    def _isSlewingDisabled_cmd_finished(self):
        """
        Enable this by making a test_*() function that calls this.
        (this command isn't valid for all cmdStates)
        """
        self.cmdState.cmd = self.cmd
        self.cmdState.cmd.finished = True
        result = self.cmdState.isSlewingDisabled()
        self.assertFalse(result)

    @abc.abstractmethod
    def test_abort(self):
        """Override, but call via super: you always want to test more things."""
        self.cmdState.abort()
        self.assertTrue(self.cmdState.aborted)
        self.assertTrue(self.actorState.aborting)
        for stage in self.cmdState.activeStages:
            self.assertEqual(self.cmdState.stages[stage], 'aborted')
        # don't check the calls, since they'll vary between different cmdStates.
        self.assertEqual(self.cmd.levels.count('i'), 1)

    def test_stop_boss_exposure_integrating(self):
        self._fake_boss_exposing()
        self.cmdState.stop_boss_exposure()
        self.assertEqual(self.cmd.calls, ['boss exposure stop',])

    def test_stop_boss_exposure_reading(self):
        self._fake_boss_reading()
        self.cmdState.stop_boss_exposure()
        self._check_cmd(0,0,1,0,False) # nothing should happen except a warning.

    def test_stop_apogee_exposure(self):
        self.cmdState.stop_apogee_exposure()
        self.assertEqual(self.cmd.calls, ['apogee expose stop',])


class TestGotoGangChange(CmdStateTester,unittest.TestCase):
    def setUp(self):
        super(TestGotoGangChange,self).setUp()
        self.cmdState = sopActor.CmdState.GotoGangChangeCmd()
        self.ok_stage = 'slew'

    def test_abort(self):
        super(TestGotoGangChange,self).test_abort()
        self.assertEqual(self.cmd.calls, ['apogee expose stop', 'tcc axis stop',])
        self.assertFalse(self.cmdState.doDomeFlat)
        self.assertFalse(self.cmdState.doSlew)


class TestGotoField(CmdStateTester,unittest.TestCase):
    def setUp(self):
        super(TestGotoField,self).setUp()
        self.cmdState = sopActor.CmdState.GotoFieldCmd()
        self.ok_stage = 'slew'

    def test_abort(self):
        super(TestGotoField,self).test_abort()
        self.assertFalse(self.cmdState.doSlew)
        self.assertFalse(self.cmdState.doHartmann)
        self.assertFalse(self.cmdState.doCalibs)
        self.assertFalse(self.cmdState.doGuiderFlat)
        self.assertFalse(self.cmdState.doGuider)

    def test_abort_boss_calibs(self):
        self.cmdState.setStages(['slew','calibs'])
        self._fake_boss_exposing()
        super(TestGotoField,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop','tcc axis stop'])


class TestGotoPosition(CmdStateTester, unittest.TestCase):
    """Tests the GotoPosition command."""

    def setUp(self):
        super(TestGotoPosition, self).setUp()
        self.cmdState = sopActor.CmdState.GotoPositionCmd()
        self.ok_stage = 'slew'

    def test_abort(self):
        super(TestGotoPosition, self).test_abort()
        self.assertFalse(self.cmdState.doSlew)


class TestDoBossCalibs(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoBossCalibs,self).setUp()
        self.cmdState = sopActor.CmdState.DoBossCalibsCmd()
        self.ok_stage = 'flat'

    def test_abort(self):
        self._fake_boss_exposing()
        self.cmdState.nBias = 1
        self.cmdState.nDark = 1
        self.cmdState.nFlat = 1
        self.cmdState.nArc = 1
        super(TestDoBossCalibs,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop',])
        self.assertEqual(self.cmdState.nBias, self.cmdState.nBiasDone)
        self.assertEqual(self.cmdState.nDark, self.cmdState.nDarkDone)
        self.assertEqual(self.cmdState.nFlat, self.cmdState.nFlatDone)
        self.assertEqual(self.cmdState.nArc, self.cmdState.nArcDone)

    def test_exposures_remain_bias(self):
        self.cmdState.nBias = 1
        self.assertTrue(self.cmdState.exposures_remain())
    def test_exposures_remain_dark(self):
        self.cmdState.nDark = 1
        self.assertTrue(self.cmdState.exposures_remain())
    def test_exposures_remain_flat(self):
        self.cmdState.nFlat = 1
        self.assertTrue(self.cmdState.exposures_remain())
    def test_exposures_remain_arc(self):
        self.cmdState.nArc = 1
        self.assertTrue(self.cmdState.exposures_remain())
    def test_no_exposures_remain(self):
        self.assertFalse(self.cmdState.exposures_remain())
    def test_exposures_remain_abort(self):
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())


class TestDoApogeeScience(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoApogeeScience,self).setUp()
        self.cmdState = sopActor.CmdState.DoApogeeScienceCmd()
        self.ok_stage = 'expose'

    def test_reset_nonkeywords(self):
        self.cmdState.index = 100
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.index, 0)

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()
    def test_isSlewingDisabled_because_alive(self):
        self.cmdState.cmd = self.cmd
        result = self.cmdState.isSlewingDisabled()
        self.assertIsInstance(result,str)
        expect = 'slewing disallowed for APOGEE, blocked by active doApogeeScience sequence'
        self.assertEqual(result,expect)

    def test_abort(self):
        super(TestDoApogeeScience,self).test_abort()
        self.assertEqual(self.cmd.calls, ['apogee expose stop',])
        self.assertEqual(self.cmdState.index,self.cmdState.ditherPairs)

    def test_exposures_remain(self):
        self.assertTrue(self.cmdState.exposures_remain())

    def test_no_exposures_remain(self):
        self.cmdState.index = self.cmdState.ditherPairs
        self.assertFalse(self.cmdState.exposures_remain())

    def test_exposures_remain_aborted(self):
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())

    def test_set_apogee_expTime_None(self):
        self.cmdState.expTime = -9999
        self.cmdState.set_apogee_expTime(None)
        self.assertEqual(self.cmdState.expTime,500)
        self.assertEqual(self.cmdState.keywords['expTime'],500)

    def test_set_apogee_expTime_1000(self):
        self.cmdState.expTime = -9999
        self.cmdState.set_apogee_expTime(1000)
        self.assertEqual(self.cmdState.expTime,1000)
        self.assertEqual(self.cmdState.keywords['expTime'],1000)

    def test_set_apogee_expTime_None_after_other_value(self):
        self.cmdState.expTime = -9999
        self.cmdState.set_apogee_expTime(1000)
        self.cmdState.set_apogee_expTime(None)
        self.assertEqual(self.cmdState.expTime,500)
        self.assertEqual(self.cmdState.keywords['expTime'],500)


class TestDoApogeeSkyFlats(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoApogeeSkyFlats,self).setUp()
        self.cmdState = sopActor.CmdState.DoApogeeSkyFlatsCmd()
        self.ok_stage = 'expose'

    def test_reset_nonkeywords(self):
        self.cmdState.index = 100
        self.cmdState.expType = 'blah'
        self.cmdState.comment = 'blahdeblah'
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.index, 0)
        self.assertEqual(self.cmdState.expType, 'object')
        self.assertEqual(self.cmdState.comment, 'sky flat, offset 0.01 degree in RA')

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()
    def test_isSlewingDisabled_because_alive(self):
        self.cmdState.cmd = self.cmd
        result = self.cmdState.isSlewingDisabled()
        self.assertIsInstance(result,str)
        expect = 'slewing disallowed for APOGEE, blocked by active doApogeeSkyFlat sequence'
        self.assertEqual(result,expect)

    def test_abort(self):
        super(TestDoApogeeSkyFlats,self).test_abort()
        self.assertEqual(self.cmd.calls, ['apogee expose stop',])
        self.assertEqual(self.cmdState.ditherPairs,0)

    def test_exposures_remain(self):
        self.assertTrue(self.cmdState.exposures_remain())

    def test_no_exposures_remain(self):
        self.cmdState.index = self.cmdState.ditherPairs
        self.assertFalse(self.cmdState.exposures_remain())

    def test_exposures_remain_aborted(self):
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())


class TestDoApogeeDomeFlat(CmdStateTester,unittest.TestCase):
    def setUp(self):
        super(TestDoApogeeDomeFlat,self).setUp()
        self.cmdState = sopActor.CmdState.DoApogeeDomeFlatCmd()
        self.ok_stage = 'domeFlat'

    def test_abort(self):
        super(TestDoApogeeDomeFlat,self).test_abort()
        self.assertEqual(self.cmd.calls, ['apogee expose stop',])


class TestDoBossScience(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoBossScience,self).setUp()
        self.cmdState = sopActor.CmdState.DoBossScienceCmd()
        self.ok_stage = 'expose'

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()

    def test_isSlewingDisabled_because_expLeft(self):
        self.cmdState.nExp = 3
        self.cmdState.index = 1
        self._isSlewingDisabled_because_exposing('BOSS',2,'IDLE')
    def test_isSlewingDisabled_because_exposing(self):
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState.nExp = 3
        self.cmdState.index = 2
        self._isSlewingDisabled_because_exposing('BOSS',1,'INTEGRATING')
    def test_isSlewingDisabled_False_reading_last_exposure(self):
        self.cmdState.nExp = 3
        self.cmdState.index = 2
        sopTester.updateModel('boss',TestHelper.bossState['reading'])
        self._isSlewingDisabled_False()

    def test_abort(self):
        self._fake_boss_exposing()
        self.cmdState.nExp = 1
        self.cmdState.index = 0
        super(TestDoBossScience,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop',])
        self.assertEqual(self.cmdState.nExp, self.cmdState.index)

    def test_exposures_remain(self):
        self.cmdState.nExp = 1
        self.cmdState.index = 0
        self.assertTrue(self.cmdState.exposures_remain())

    def test_no_exposures_remain(self):
        self.cmdState.nExp = 1
        self.cmdState.index = 1
        self.assertFalse(self.cmdState.exposures_remain())

    def test_exposures_remain_aborted(self):
        self.cmdState.nExp = 1
        self.cmdState.index = 0
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())


class TestDoMangaSequence(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoMangaSequence,self).setUp()
        self.cmdState = sopActor.CmdState.DoMangaSequenceCmd()
        self.ok_stage = 'dither'

    def test_reset_nonkeywords(self):
        self.cmdState.index = 100
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.index, 0)

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()

    def test_abort(self):
        self._fake_boss_exposing()
        super(TestDoMangaSequence,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop',])

    def test_exposures_remain(self):
        self.assertTrue(self.cmdState.exposures_remain())

    def test_no_exposures_remain(self):
        self.cmdState.index = len(self.cmdState.ditherSeq)
        self.assertFalse(self.cmdState.exposures_remain())

    def test_exposures_remain_aborted(self):
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())


class TestDoMangaDither(CmdStateTester,unittest.TestCase):
    def setUp(self):
        super(TestDoMangaDither,self).setUp()
        self.cmdState = sopActor.CmdState.DoMangaDitherCmd()
        self.ok_stage = 'dither'

    def test_reset_nonkeywords(self):
        self.cmdState.readout = 100
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.readout, True)

    def test_isSlewingDisabled_because_exposing(self):
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self._isSlewingDisabled_because_exposing('MaNGA',1,'INTEGRATING')
    def test_isSlewingDisabled_False(self):
        sopTester.updateModel('boss',TestHelper.bossState['reading'])
        self._isSlewingDisabled_False()

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()

    def test_reinitialize(self):
        self.cmdState.readout = False
        self.cmdState.reinitialize()
        self.assertTrue(self.cmdState.readout)

    def test_abort(self):
        self._fake_boss_exposing()
        super(TestDoMangaDither,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop',])


class TestDoApogeeMangaSequence(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoApogeeMangaSequence,self).setUp()
        self.cmdState = sopActor.CmdState.DoApogeeMangaSequenceCmd()
        self.ok_stage = 'dither'

    def test_reset_nonkeywords(self):
        self.cmdState.index = 100
        self.cmdState.mangaDitherSeq = 'abcd'
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.index, 0)
        self.assertEqual(self.cmdState.mangaDitherSeq, 'NSENSE')

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()

    def test_isSlewingDisabled_because_exposing(self):
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        survey = 'APOGEE&MaNGA'
        self.cmdState.cmd = self.cmd
        result = self.cmdState.isSlewingDisabled()
        self.assertIsInstance(result,str)
        self.assertIn('slewing disallowed for %s'%survey,result)
        self.assertIn('with a sequence in progress.',result)

    def test_isSlewingDisabled_False(self):
        sopTester.updateModel('boss',TestHelper.bossState['reading'])
        self._isSlewingDisabled_False()

    # NOTE: not testing the keywords dict, since it doesn't get applied to the
    # attributes until reinitalize() is called after the SopCmd is issued.
    def test_mangaDither(self):
        self.cmdState.set_mangaDither()
        self.assertFalse(self.cmdState.readout)
        self.assertEqual(self.cmdState.mangaExpTime, 900)
        self.assertEqual(self.cmdState.apogeeExpTime, 450)
    def test_mangaStare(self):
        self.cmdState.set_mangaStare()
        self.assertFalse(self.cmdState.readout)
        self.assertEqual(self.cmdState.mangaExpTime, 900)
        self.assertEqual(self.cmdState.apogeeExpTime, 450)
    def test_apogeeLead(self):
        self.cmdState.set_apogeeLead()
        self.assertTrue(self.cmdState.readout)
        self.assertEqual(self.cmdState.mangaExpTime, 900)
        self.assertEqual(self.cmdState.apogeeExpTime, 500)

    def test_set_apogee_expTime_None(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)

        self.assertEqual(self.cmdState.count, 2)
        self.assertEqual(self.cmdState.keywords['mangaDithers'], 'CC')
        self.assertEqual(self.cmdState.mangaDitherSeq, 'CCCC')

    def test_set_apogee_expTime_1000(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.assertEqual(self.cmdState.apogeeExpTime, 1000)

        self.assertEqual(self.cmdState.count, 2)
        self.assertEqual(self.cmdState.keywords['mangaDithers'], 'CC')
        self.assertEqual(self.cmdState.mangaDitherSeq, 'CCCC')

        self.cmdState.index = len(self.cmdState.mangaDitherSeq) - 1
        self.assertFalse(self.cmdState.exposures_remain())

    def test_set_apogee_expTime_None_after_other_value(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)

    def test_set_apogee_long_False_after_True(self):
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)
        self.assertEqual(self.cmdState.apogee_long, False)

    def test_abort(self):
        self._fake_boss_exposing()
        super(TestDoApogeeMangaSequence,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop','apogee expose stop'])

    def test_exposures_remain(self):
        self.assertTrue(self.cmdState.exposures_remain())

    def test_no_exposures_remain(self):
        self.cmdState.index = len(self.cmdState.mangaDitherSeq)
        self.assertFalse(self.cmdState.exposures_remain())

    def test_exposures_remain_aborted(self):
        self.cmdState.aborted = True
        self.assertFalse(self.cmdState.exposures_remain())


class TestDoApogeeMangaDither(CmdStateTester,unittest.TestCase):
    def setUp(self):
        self.userKeys = True
        super(TestDoApogeeMangaDither,self).setUp()
        self.cmdState = sopActor.CmdState.DoApogeeMangaDitherCmd()
        self.ok_stage = 'dither'

    def test_reset_nonkeywords(self):
        self.cmdState.readout = 100
        self.cmdState.reset_nonkeywords()
        self.assertEqual(self.cmdState.readout, True)

    def test_isSlewingDisabled_because_exposing(self):
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self._isSlewingDisabled_because_exposing('APOGEE&MaNGA',1,'INTEGRATING')

    def test_isSlewingDisabled_False(self):
        sopTester.updateModel('boss',TestHelper.bossState['reading'])
        self._isSlewingDisabled_False()

    def test_apogeeLead(self):
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.mangaExpTime, 900)
        self.assertEqual(self.cmdState.apogeeExpTime, 500)
    def test_manga(self):
        self.cmdState.set_manga()
        self.assertEqual(self.cmdState.mangaExpTime, 900)
        self.assertEqual(self.cmdState.apogeeExpTime, 450)

    def test_set_apogee_expTime_None(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)

    def test_set_apogee_expTime_1000(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.assertEqual(self.cmdState.apogeeExpTime, 1000)

    def test_set_apogee_expTime_None_after_other_value(self):
        self.cmdState.apogeeExpTime = -9999
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)

    def test_set_apogee_long_False_after_True(self):
        self.cmdState.set_apogeeLead(apogeeExpTime=1000)
        self.cmdState.set_apogeeLead()
        self.assertEqual(self.cmdState.apogeeExpTime, 500)
        self.assertEqual(self.cmdState.apogee_long, False)

    def test_isSlewingDisabled_no_cmd(self):
        self._isSlewingDisabled_no_cmd()
    def test_isSlewingDisabled_cmd_finished(self):
        self._isSlewingDisabled_cmd_finished()

    def test_reinitialize(self):
        self.cmdState.readout = False
        self.cmdState.reinitialize()
        self.assertTrue(self.cmdState.readout)

    def test_abort(self):
        self._fake_boss_exposing()
        super(TestDoApogeeMangaDither,self).test_abort()
        self.assertEqual(self.cmd.calls, ['boss exposure stop','apogee expose stop'])


if __name__ == '__main__':
    verbosity = 2

    unittest.main(verbosity=verbosity)
