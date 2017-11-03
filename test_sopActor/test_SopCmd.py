"""
Tests of the SopCmd functions, without executing any threads.

Each of these tests should confirm that a SopCmd command call calls the correct
queue with an appropriately crafted CmdState and other relevant parameters.

If these tests work correctly, each masterThread function should work
correctly when called via a SopCmd (assuming test_masterThread clears).
"""
import unittest
import threading

import sopActor
import sopActor.myGlobals as myGlobals
from sopActor import Queue, CmdState

from actorcore import TestHelper
import sopTester


def build_active_stages(allStages, activeStages):
    """Returns a dictionary of stages and their off/pending state."""
    stages = dict(zip(allStages,['off']*len(allStages)))
    for x in activeStages: stages[x] = 'pending'
    return stages


class SopCmdTester(sopTester.SopTester):
    def setUp(self):
        self.verbose = True
        super(SopCmdTester,self).setUp()
        self.timeout = 1
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues = {}
        myGlobals.actorState.queues[sopActor.MASTER] = Queue('master')
        myGlobals.actorState.queues[sopActor.TCC] = Queue('tcc')
        myGlobals.actorState.queues[sopActor.SLEW] = Queue('slew')
        self.cmd.verbose = False # don't spam initial loadCart messages
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose
        self._clear_bypasses()

    def _pre_command(self, command, queue):
        """Run a text command in advance, without being verbose and clearing any messages."""
        self.cmd.verbose = False
        self._run_cmd(command,queue)
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose
        # in case the above command "finishes"
        self.cmd = TestHelper.Cmd(verbose=self.verbose)

    def _start_thread(self, queue, tid, tname):
        """Start a fakeThread for queue, returning the threading instance."""
        actorState = self.actorState
        actorState.threads = {}
        actorState.threads[tid] = threading.Thread(target=sopTester.FakeThread, name=tname, args=[actorState.actor,actorState.queues])
        actorState.threads[tid].daemon = True
        actorState.threads[tid].start()

    def _stop_thread(self, queue, tid):
        """Stop thread tid associated with queue, and cleanup."""
        queue.put(sopActor.Msg(sopActor.Msg.EXIT, self.cmd))
        self.actorState.threads[tid].join()


class SopAPOCmdTester(SopCmdTester):
    def setUp(self):
        self.name = 'sop'
        super(SopAPOCmdTester, self).setUp()
        self.actor = TestHelper.FakeActor(self.name, self.name+'Actor', location='APO')
        self.sopcmd = self.actor.commandSets['SopCmd_APO']


class SopLCOCmdTester(SopCmdTester):
    def setUp(self):
        self.name = 'sop'
        super(SopLCOCmdTester, self).setUp()
        self.actor = TestHelper.FakeActor(self.name, self.name+'Actor', location='LCO')
        self.sopcmd = self.actor.commandSets['SopCmd_LCO']


class SopLOCALCmdTester(SopCmdTester):
    def setUp(self):
        self.name = 'sop'
        super(SopLOCALCmdTester, self).setUp()
        self.actor = TestHelper.FakeActor(self.name, self.name+'Actor', location='LOCAL')
        self.sopcmd = self.actor.commandSets['SopCmd_LOCAL']


class TestBypass(SopCmdTester, unittest.TestCase):
    """Test setting and clearing bypasses with the sop bypass command."""
    def setUp(self):
        # Cart bypasses can send a guider command, so we should test that.
        self._load_cmd_calls(self.id().split('.')[-2])
        super(TestBypass,self).setUp()
        self._clear_bypasses()

    def _bypass_set(self, system, nInfo, nWarn, nCalls=0, survey=None):
        bypass = myGlobals.bypass
        self.cmd.rawCmd = 'bypass subSystem=%s'%system
        self.actor.runActorCmd(self.cmd)
        for item in bypass._bypassed:
            # all others should be cleared.
            if item != system:
                self.assertFalse(bypass.get(name=item))
            else:
                self.assertTrue(bypass.get(name=item))
        # check that the survey values were updated.
        if bypass.is_cart_bypass(system):
            self.actorState.plateType == survey[0]
            self.actorState.surveyMode == survey[1]
        self._check_cmd(nCalls,nInfo,nWarn,0,True)
    def test_bypass_isBoss(self):
        self._bypass_set('isBoss', 59, 3, 1, ['eBOSS',None])
    def test_bypass_isApogee(self):
        self._bypass_set('isApogee', 59, 3, 1, ['APOGEE',None])
    def test_bypass_isMangaStare(self):
        self._bypass_set('isMangaStare', 59, 3, 1, ['MaNGA','MaNGA stare'])
    def test_bypass_isMaStar(self):
        self._bypass_set('isMaStar', 59, 3, 1, ['MaNGA', 'MaStar'])
    def test_bypass_isMangaDither(self):
        self._bypass_set('isMangaDither', 59, 3, 1, ['MaNGA','MaNGA dither'])
    def test_bypass_isApogeeLead(self):
        self._bypass_set('isApogeeLead', 59, 3, 1, ['APGOEE-2&MaNGA','APOGEE lead'])
    def test_bypass_isApogeeMangaDither(self):
        self._bypass_set('isApogeeMangaDither', 59, 3, 1, ['APGOEE-2&MaNGA','MaNGA dither'])
    def test_bypass_isApogeeMangaStare(self):
        self._bypass_set('isApogeeMangaStare', 59, 3, 1, ['APGOEE-2&MaNGA','MaNGA stare'])
    def test_bypass_isApogeeMangaMaStar(self):
        self._bypass_set('isApogeeMangaMaStar', 59, 3, 1, ['APGOEE-2&MaNGA', 'MaStar'])
    def test_bypass_gangToCart(self):
        self._bypass_set('gangToCart', 58, 4)
    def test_bypass_gangToPodium(self):
        self._bypass_set('gangToPodium', 58, 4)

    def test_bypass_axes(self):
        self._bypass_set('axes', 58, 1)

    def test_bypass_slewToField(self):
        self._bypass_set('slewToField', 58, 1)

    def test_not_bypassable(self):
        self._clear_bypasses()
        self.cmd.rawCmd = 'bypass subSystem=notBypassable'
        self.actor.runActorCmd(self.cmd)
        self._check_cmd(0,0,0,0,True,didFail=True)


class TestStopCmd(SopCmdTester,unittest.TestCase):
    def test_stop_cmd(self):
        # Use a generic cmdState, as I don't want extra aborting stuff.
        self.cmdState = CmdState.CmdState('doBossScience',['foo',])
        # initialize, so it looks like a live command.
        self.cmdState.cmd = self.cmd
        self._fake_boss_exposing()
        self.sopCmd.stop_cmd(self.cmd,self.cmdState,self.actorState,'fakeCmd')
        self.assertTrue(myGlobals.actorState.aborting)
        self._check_cmd(0,6,0,0, True)
    def test_stop_cmd_not_active(self):
        """Fail if there's no active command to operate on."""
        cmdState = self.actorState.doBossScience
        self.sopCmd.stop_cmd(self.cmd,cmdState,self.actorState,'')
        self._check_cmd(0,0,0,0, True, True)

class TestClassifyCartridge(SopCmdTester,unittest.TestCase):
    def _classifyCartridge(self,nCart,plateType,surveyMode,expect):
        """Expect is a tuple of expected survey and surveyMode IDs from sopActor."""
        self.sopCmd.classifyCartridge(self.cmd,nCart,plateType,surveyMode)
        sopState = self.actorState
        self.assertEqual(sopState.survey,expect[0])
        self.assertEqual(sopState.surveyMode,expect[1])
        self.assertEqual(sopState.surveyText,expect[2])

    def test_classifyCartridge_bad(self):
        expect = [sopActor.UNKNOWN,None, ['UNKNOWN','None']]
        self._classifyCartridge(-1,'unknown',None,expect)
    def test_classifyCartridge_bad_survey(self):
        expect = [sopActor.UNKNOWN,sopActor.APOGEELEAD, ['UNKNOWN','APOGEE lead']]
        self._classifyCartridge(1,'mangled','APOGEE lead',expect)
    def test_classifyCartridge_bad_surveyMode(self):
        expect = [sopActor.MANGA, None, ['MaNGA','None']]
        self._classifyCartridge(11,'MaNGA','mangled',expect)
    def test_classifyCartridge_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expect = [sopActor.BOSS, None, ['BOSS','None']]
        self._classifyCartridge(11,'BOSS','None',expect)
    def test_classifyCartridge_eboss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expect = [sopActor.BOSS,None, ['eBOSS','None']]
        self._classifyCartridge(11,'eBOSS','None',expect)
    def test_classifyCartridge_apogee(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = [sopActor.APOGEE,None, ['APOGEE','None']]
        self._classifyCartridge(1,'APOGEE','None',expect)
    def test_classifyCartridge_apogee2(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = [sopActor.APOGEE,None, ['APOGEE-2','None']]
        self._classifyCartridge(1,'APOGEE-2','None',expect)
    def test_classifyCartridge_mangaDither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        expect = [sopActor.MANGA,sopActor.MANGADITHER, ['MaNGA','MaNGA dither']]
        self._classifyCartridge(2,'MaNGA','MaNGA dither',expect)
    def test_classifyCartridge_mangaStare(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaStareLoaded'])
        expect = [sopActor.MANGA,sopActor.MANGASTARE, ['MaNGA','MaNGA stare']]
        self._classifyCartridge(2,'MaNGA','MaNGA stare',expect)
    def test_classifyCartridge_MaStar(self):
        sopTester.updateModel('guider', TestHelper.guiderState['MaStarLoaded'])
        expect = [sopActor.MANGA, sopActor.MASTAR, ['MaNGA', 'MaStar']]
        self._classifyCartridge(2, 'MaNGA', 'MaStar', expect)
    def test_classifyCartridge_apogee_lead(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLeadLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apogeeLead'])
        expect = [sopActor.APOGEEMANGA,sopActor.APOGEELEAD, ['APOGEE-2&MaNGA','APOGEE lead']]
        self._classifyCartridge(3,'APOGEE-2&MaNGA','APOGEE lead',expect)
        expect = [sopActor.APOGEEMANGA,sopActor.APOGEELEAD, ['APOGEE&MaNGA','APOGEE lead']]
        self._classifyCartridge(3,'APOGEE&MaNGA','APOGEE lead',expect)
    def test_classifyCartridge_apogeemanga_dither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apgoeemangaDither'])
        expect = [sopActor.APOGEEMANGA,sopActor.MANGADITHER, ['APOGEE-2&MaNGA','MaNGA dither']]
        self._classifyCartridge(3,'APOGEE-2&MaNGA','MaNGA dither',expect)
        expect = [sopActor.APOGEEMANGA,sopActor.MANGADITHER, ['APOGEE&MaNGA','MaNGA dither']]
        self._classifyCartridge(3,'APOGEE&MaNGA','MaNGA dither',expect)
    def test_classifyCartridge_apogeemanga_stare(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaStareLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apgoeemangaStare'])
        expect = [sopActor.APOGEEMANGA,sopActor.MANGASTARE, ['APOGEE-2&MaNGA','MaNGA stare']]
        self._classifyCartridge(3,'APOGEE-2&MaNGA','MaNGA stare',expect)
        expect = [sopActor.APOGEEMANGA,sopActor.MANGASTARE, ['APOGEE&MaNGA','MaNGA stare']]
        self._classifyCartridge(3,'APOGEE&MaNGA','MaNGA stare',expect)
    def test_classifyCartridge_apogeemanga_mastar(self):
        sopTester.updateModel('guider', TestHelper.guiderState['apogeemangaMaStarLoaded'])
        sopTester.updateModel('platedb', TestHelper.platedbState['apogeemangaMaStar'])
        expect = [sopActor.APOGEEMANGA, sopActor.MASTAR, ['APOGEE-2&MaNGA', 'MaStar']]
        self._classifyCartridge(3, 'APOGEE-2&MaNGA', 'MaStar', expect)
        expect = [sopActor.APOGEEMANGA, sopActor.MASTAR, ['APOGEE&MaNGA', 'MaStar']]
        self._classifyCartridge(3, 'APOGEE&MaNGA', 'MaStar', expect)
    def test_classifyCartridge_ecamera(self):
        expect = [sopActor.ECAMERA,None, ['ecamera',None]]
        self._classifyCartridge(19,'ecamera',None,expect)

    def test_classifyCartridge_boss_bypass(self):
        self._prep_bypass('isBoss',clear=True)
        expect = [sopActor.BOSS,None, ['eBOSS','None']]
        self._classifyCartridge(2,'APOGEE','None',expect)
    def test_classifyCartridge_apogee_bypass(self):
        self._prep_bypass('isApogee',clear=True)
        expect = [sopActor.APOGEE,None, ['APOGEE-2','None']]
        self._classifyCartridge(11,'BOSS','None',expect)
    def test_classifyCartridge_mangaStare_bypass(self):
        self._prep_bypass('isMangaStare',clear=True)
        expect = [sopActor.MANGA,sopActor.MANGASTARE, ['MaNGA','MaNGA stare']]
        self._classifyCartridge(2,'APOGEE','None',expect)
    def test_classifyCartridge_MaStar_bypass(self):
        self._prep_bypass('isMaStar', clear=True)
        expect = [sopActor.MANGA, sopActor.MASTAR, ['MaNGA', 'MaStar']]
        self._classifyCartridge(2, 'APOGEE', 'None', expect)
    def test_classifyCartridge_mangaDither_bypass(self):
        self._prep_bypass('isMangaDither',clear=True)
        expect = [sopActor.MANGA,sopActor.MANGADITHER, ['MaNGA','MaNGA dither']]
        self._classifyCartridge(2,'APOGEE','None',expect)
    def test_classifyCartridge_apogeelead_bypass(self):
        self._prep_bypass('isApogeeLead',clear=True)
        expect = [sopActor.APOGEEMANGA,sopActor.APOGEELEAD, ['APOGEE-2&MaNGA','APOGEE lead']]
        self._classifyCartridge(11,'BOSS','None',expect)
    def test_classifyCartridge_apogeemangaDither_bypass(self):
        self._prep_bypass('isApogeeMangaDither',clear=True)
        expect = [sopActor.APOGEEMANGA,sopActor.MANGADITHER, ['APOGEE-2&MaNGA','MaNGA dither']]
        self._classifyCartridge(11,'BOSS','None',expect)
    def test_classifyCartridge_apogeemangaStare_bypass(self):
        self._prep_bypass('isApogeeMangaStare',clear=True)
        expect = [sopActor.APOGEEMANGA,sopActor.MANGASTARE, ['APOGEE-2&MaNGA','MaNGA stare']]
        self._classifyCartridge(11,'BOSS','None',expect)
    def test_classifyCartridge_apogeemangaMaStar_bypass(self):
        self._prep_bypass('isApogeeMangaMaStar', clear=True)
        expect = [sopActor.APOGEEMANGA, sopActor.MASTAR, ['APOGEE-2&MaNGA', 'MaStar']]
        self._classifyCartridge(11, 'BOSS', 'None', expect)

    def test_classifyCartridge_coObsNone(self):
        sopTester.updateModel('guider',TestHelper.guiderState['noneLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsNone'])
        expect = [sopActor.UNKNOWN,None, ['UNKNOWN','None']]
        self._classifyCartridge(-1,'unknown',None,expect)

    def test_classifyCartridge_coObsNoAPOGEE(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsNoAPOGEE'])
        expect = [sopActor.MANGA,sopActor.MANGADITHER, ['MaNGA','MaNGA dither']]
        self._classifyCartridge(7,'APOGEE-2&MaNGA', 'MaNGA dither', expect)

    def test_classifyCartridge_coObsNoMANGA(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsNoMANGA'])
        expect = [sopActor.APOGEE, None, ['APOGEE-2','None']]
        self._classifyCartridge(7,'APOGEE-2&MaNGA', 'APOGEE lead', expect)

    def test_classifyCartridge_coObsBoth_MaNGA_dither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsBoth'])
        expect = [sopActor.APOGEEMANGA,sopActor.MANGADITHER, ['APOGEE-2&MaNGA','MaNGA dither']]
        self._classifyCartridge(7,'APOGEE-2&MaNGA', 'MaNGA dither', expect)
    def test_classifyCartridge_coObsBoth_MaNGA_stare(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaStareLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsBoth'])
        expect = [sopActor.APOGEEMANGA,sopActor.MANGASTARE, ['APOGEE-2&MaNGA','MaNGA stare']]
        self._classifyCartridge(7,'APOGEE-2&MaNGA', 'MaNGA stare', expect)
    def test_classifyCartridge_coObsBoth_MaNGA_MaStar(self):
        sopTester.updateModel('guider', TestHelper.guiderState['apogeemangaMaStarLoaded'])
        sopTester.updateModel('platedb', TestHelper.platedbState['coObsBoth'])
        expect = [sopActor.APOGEEMANGA, sopActor.MASTAR, ['APOGEE-2&MaNGA', 'MaStar']]
        self._classifyCartridge(7, 'APOGEE-2&MaNGA', 'MaStar', expect)
    def test_classifyCartridge_coObsBoth_APOGEE_Lead(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLeadLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['coObsBoth'])
        expect = [sopActor.APOGEEMANGA,sopActor.APOGEELEAD, ['APOGEE-2&MaNGA','APOGEE lead']]
        self._classifyCartridge(7,'APOGEE-2&MaNGA', 'APOGEE lead', expect)


class TestUpdateCartridge(SopCmdTester,unittest.TestCase):
    """Confirm that we get the right validCommands from each survey type."""
    def _updateCartridge(self, nCart, survey, surveyMode, expected, bypassed=False):
        self.sopCmd.updateCartridge(nCart, survey, surveyMode, bypassed=bypassed)
        sop = myGlobals.actorState.models['sop']
        self.assertEqual(sop.keyVarDict['surveyCommands'].getValue(), expected['surveyCommands'])
        self.assertEqual(sop.keyVarDict['survey'].getValue(), (survey,surveyMode))

    def test_updateCartridge_none(self):
        sopTester.updateModel('guider',TestHelper.guiderState['noneLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopEmptyCommands['surveyCommands']
        self._updateCartridge(-1,'UNKNOWN','None',expected)
    def test_updateCartridge_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopBossCommands['surveyCommands']
        self._updateCartridge(11,'BOSS','None',expected)
    def test_updateCartridge_eboss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopBossCommands['surveyCommands']
        self._updateCartridge(11,'eBOSS','None',expected)

    def test_updateCartridge_mangaDither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopMangaCommands['surveyCommands']
        self._updateCartridge(2,'MaNGA','MaNGA dither',expected)

    def test_updateCartridge_apogee(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE','None',expected)
        self.assertEqual(self.actorState.doApogeeScience.expTime,500)

    def test_updateCartridge_apogee2(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE-2','None',expected)
        self.assertEqual(self.actorState.doApogeeScience.expTime,500)

    def test_updateCartridge_apogee2_long(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apogeeLead1000s'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE-2','None',expected)
        self.assertEqual(self.actorState.doApogeeScience.expTime,1000)

    def test_updateCartridge_apogeemanga_long(self):
        sopTester.updateModel(
            'guider', TestHelper.guiderState['apogeeLeadLoaded'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apogeeLead1000s'])

        expected = {}
        expected['surveyCommands'] = \
            TestHelper.sopApogeeMangaCommands['surveyCommands']

        self._updateCartridge(1, 'APOGEE-2&MaNGA', 'APOGEE lead', expected)

        self.assertEqual(
            self.actorState.doApogeeMangaDither.apogeeExpTime, 1000)
        self.assertEqual(
            self.actorState.doApogeeMangaSequence.apogeeExpTime, 1000)

    def test_updateCartridge_apogeemanga_exp_None(self):
        sopTester.updateModel(
            'guider', TestHelper.guiderState['apogeeLeadLoaded'])
        sopTester.updateModel(
            'platedb', TestHelper.platedbState['apogeeLead'])

        expected = {}
        expected['surveyCommands'] = \
            TestHelper.sopApogeeMangaCommands['surveyCommands']

        self._updateCartridge(1, 'APOGEE-2&MaNGA', 'APOGEE lead', expected)

        self.assertEqual(
            self.actorState.doApogeeMangaDither.apogeeExpTime, 500)
        self.assertEqual(
            self.actorState.doApogeeMangaSequence.apogeeExpTime, 500)

    def test_updateCartridge_apogeemanga_500s_no_survey_mode(self):

        # Loads a double length plate
        sopTester.updateModel('guider', TestHelper.guiderState['apogeeLeadLoaded'])
        sopTester.updateModel('platedb', TestHelper.platedbState['apogeeLead1000s'])

        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeMangaCommands['surveyCommands']

        self._updateCartridge(1, 'APOGEE-2&MaNGA', 'APOGEE lead', expected)

        self.assertEqual(self.actorState.doApogeeMangaDither.apogeeExpTime, 1000)
        self.assertEqual(self.actorState.doApogeeMangaSequence.apogeeExpTime, 1000)
        self.assertEqual(self.actorState.doApogeeScience.expTime, 1000)

        # Now loads a 500s cartridge with survey mode None
        sopTester.updateModel('guider', TestHelper.guiderState['apogeemangaNoneLoaded'])
        sopTester.updateModel('platedb', TestHelper.platedbState['apogeeLead'])

        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeMangaCommands['surveyCommands']

        self._updateCartridge(1, 'APOGEE-2&MaNGA', 'None', expected)

        self.assertEqual(self.actorState.doApogeeMangaDither.apogeeExpTime, 500)
        self.assertEqual(self.actorState.doApogeeMangaSequence.apogeeExpTime, 500)
        self.assertEqual(self.actorState.doApogeeScience.expTime, 500)

    def test_updateCartridge_apogeemanga(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeMangaCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE-2&MaNGA','MaNGA dither',expected)

    def test_updateCartridge_boss_bypass_was_set(self):
        """When a cart is loaded, we should clear the survey bypasses, per #2284."""
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        self._prep_bypass('isApogeeMangaStare',clear=True)
        expected = {}
        expected['surveyCommands'] = TestHelper.sopBossCommands['surveyCommands']
        self._updateCartridge(11,'BOSS','None',expected)

class TestUpdateApogeeDesign(SopCmdTester,unittest.TestCase):
    def _update_apogee_design(self, expect, survey='APOGEE',
                              surveyMode='None', bypassed=False):
        self.actorState.doApogeeScience.expTime = -9999
        self.sopCmd.updateCartridge(1, survey, surveyMode, bypassed=bypassed)
        self.assertEqual(self.actorState.doApogeeScience.expTime, expect)
    def test_update_apogee_design_None(self):
        sopTester.updateModel('platedb',TestHelper.platedbState['apogeeLead'])
        self._update_apogee_design(500)
    def test_update_apogee_design_1000(self):
        sopTester.updateModel('platedb',TestHelper.platedbState['apogeeLead1000s'])
        self._update_apogee_design(1000)

class TestStatus(SopCmdTester,unittest.TestCase):
    def _status(self, nInfo, args=''):
        self._run_cmd('status %s'%args, None)
        self._check_cmd(0,nInfo,0,0,True)
    def test_status(self):
        self._status(59)
    def test_status_geek(self):
        self._status(61,args='geek')
    def test_status_noFinish(self):
        self.sopCmd.status(self.cmd,finish=False)
        self._check_cmd(0,59,0,0,False)

    def _oneCommand(self, nInfo, oneCommand):
        """
        nInfo here is the number of messages specific to oneCommand.
        This is usually 3, or 4 if there are userKeys in that CmdState.
        """
        self.sopCmd.status(self.cmd,oneCommand=oneCommand)
        self._check_cmd(0,4+nInfo,0,0,True)
    def test_gotoGangChange(self):
        self._oneCommand(4,'gotoGangChange')
    def test_doApogeeDomeFlat(self):
        self._oneCommand(4,'doApogeeDomeFlat')
    def test_hartmann(self):
        self._oneCommand(4,'hartmann')
    def test_gotoField(self):
        self._oneCommand(4,'gotoField')
    def test_doBossCalibs(self):
        self._oneCommand(5,'doBossCalibs')
    def test_doApogeeScience(self):
        self._oneCommand(5,'doApogeeScience')
    def test_doApogeeSkyFlats(self):
        self._oneCommand(5,'doApogeeSkyFlats')
    def test_doBossScience(self):
        self._oneCommand(5,'doBossScience')
    def test_doMangaSequence(self):
        self._oneCommand(5,'doMangaSequence')
    def test_doMangaDither(self):
        self._oneCommand(4,'doMangaDither')
    def test_doApogeeMangaDither(self):
        self._oneCommand(5,'doApogeeMangaDither')
    def test_doApogeeMangaSequence(self):
        self._oneCommand(5,'doApogeeMangaSequence')
    def test_gotoStow(self):
        self._oneCommand(4,'gotoStow')
    def test_gotoInstrumentChange(self):
        self._oneCommand(4,'gotoInstrumentChange')


class TestGotoGangChange(SopCmdTester,unittest.TestCase):
    def _gotoGangChange(self, nCart, survey, args, expect):
        allStages = ['domeFlat','slew']
        stages = build_active_stages(allStages, expect.get('stages',allStages))

        self._update_cart(nCart, survey)
        queue = myGlobals.actorState.queues[sopActor.SLEW]
        msg = self._run_cmd('gotoGangChange %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_GANG_CHANGE)
        self.assertEqual(msg.cmdState.alt,expect.get('alt',45))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.doSlew,expect.get('doSlew',True))
        self.assertEqual(msg.cmdState.doDomeFlat,expect.get('doDomeFlat',True))

    def test_gotoGangChange_ok(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat', 'slew'],'alt':15}
        self._gotoGangChange(1,'apogee','alt=15',expect)
    def test_gotoGangChange_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expect = {'stages':['domeFlat','slew']}
        self._gotoGangChange(11,'boss','',expect)
    def test_gotoGangChange_noSlew(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat'],'doSlew':False}
        self._gotoGangChange(1,'apogee','noSlew',expect)
    def test_gotoGangChange_noDomeFlat(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['slew'],'doDomeFlat':False}
        self._gotoGangChange(1,'apogee','noDomeFlat',expect)

    def test_gotoGangChange_abort(self):
        self.actorState.gotoGangChange.cmd = self.cmd
        self._run_cmd('gotoGangChange abort',None)
        self.assertTrue(self.actorState.aborting)

    def test_gotoGangChange_modify(self):
        """Cannot modify this command, so fail and nothing should change."""
        queue = myGlobals.actorState.queues[sopActor.SLEW]
        # create something we can modify.
        msg = self._run_cmd('gotoGangChange alt=10', queue)
        msgNew = self._run_cmd('gotoGangChange alt=20', queue, empty=True)
        self.assertIsNone(msgNew)
        self.assertEqual(msg.cmdState.alt, 10)
        self._check_cmd(0,2,0,0,True,True)


class TestDoMangaDither(SopCmdTester,unittest.TestCase):
    def _doMangaDither(self, expect, args='', cmd_levels=(0,2,0,0)):
        stages = ['expose', 'dither']
        stages = dict(zip(stages,['idle']*len(stages)))

        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaDither %s'%(args),queue)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.type,sopActor.Msg.DO_MANGA_DITHER)
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.dither,expect['dither'])
        self.assertEqual(msg.cmdState.expTime,expect['expTime'])

    def test_doMangaDither_default(self):
        expect = {'expTime':900,
                  'dither':'C',
                  }
        self._doMangaDither(expect)
    def test_doMangaDither_N(self):
        expect = {'expTime':900,
                  'dither':'N',
                  }
        args = 'dither=N'
        self._doMangaDither(expect,args)
    def test_doMangaDither_expTime(self):
        expect = {'expTime':1000,
                  'dither':'C',
                  }
        args = 'expTime=1000'
        self._doMangaDither(expect,args)

    def test_doMangaDither_abort(self):
        self.actorState.doMangaDither.cmd = self.cmd
        self._run_cmd('doMangaDither abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doMangaDither_modify(self):
        """Cannot modify this command, so fail and nothing should change."""
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doMangaDither dither=N expTime=100', queue)
        msgNew = self._run_cmd('doMangaDither dither=S expTime=200', queue, empty=True)
        self.assertIsNone(msgNew)
        self.assertEqual(msg.cmdState.expTime, 100)
        self.assertEqual(msg.cmdState.dither, 'N')
        self._check_cmd(0,2,0,0,True,True)


class TestDoMangaSequence(SopCmdTester, unittest.TestCase):
    def _doMangaSequence(self, expect, args, cmd_levels=(0, 2, 0, 0)):
        stages = ['expose', 'calibs', 'dither']
        stages = dict(zip(stages, ['idle']*len(stages)))

        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaSequence %s' % (args), queue)
        self.assertEqual(msg.type, sopActor.Msg.DO_MANGA_SEQUENCE)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.cmdState.stages, stages)
        self.assertEqual(msg.cmdState.ditherSeq, expect['ditherSeq'])

    def test_doMangaSequence_default(self):
        expect = {'expTime': 900,
                  'ditherSeq': 'NSE'*3}
        self._doMangaSequence(expect, '')

    def test_doMangaSequence_one_set(self):
        expect = {'expTime': 900,
                  'ditherSeq': 'NSE'}
        self._doMangaSequence(expect, 'count=1')

    def test_doMangaSequence_abort(self):
        self.actorState.doMangaSequence.cmd = self.cmd
        self._run_cmd('doMangaSequence abort', None)
        self.assertTrue(self.actorState.aborting)

    def _doMangaSequence_modify(self, args1, args2, cmd_levels=(0, 11, 0, 0), didFail=False):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doMangaSequence %s' % args1, queue)
        msgNew = self._run_cmd('doMangaSequence %s' % args2, queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(*cmd_levels, finish=True, didFail=didFail)
        return msg

    def test_doMangaSequence_modify_count_2to1(self):
        msg = self._doMangaSequence_modify('dithers=NSE count=2', 'dithers=NSE count=1')
        self.assertEqual(msg.cmdState.dithers, 'NSE')
        self.assertEqual(msg.cmdState.count, 1)
        self.assertEqual(msg.cmdState.ditherSeq, 'NNSE')

    def test_doMangaSequence_modify_count_1to2(self):
        msg = self._doMangaSequence_modify('dithers=NSE count=1', 'dithers=NSE count=2')
        self.assertEqual(msg.cmdState.dithers, 'NSE')
        self.assertEqual(msg.cmdState.count, 2)
        self.assertEqual(msg.cmdState.ditherSeq, 'NNSENSE')

    def test_doMangaSequence_modify_dithers(self):
        msg = self._doMangaSequence_modify('dithers=NSE count=2', 'dithers=SEN count=1')
        self.assertEqual(msg.cmdState.count, 1)
        self.assertEqual(msg.cmdState.dithers, 'SEN')
        self.assertEqual(msg.cmdState.ditherSeq, 'NSEN')

    def test_doMangaSequence_modify_dithers_alle(self):
        msg = self._doMangaSequence_modify('dithers=NSE count=3', 'dithers=EE count=3')
        self.assertEqual(msg.cmdState.count, 3)
        self.assertEqual(msg.cmdState.dithers, 'EE')
        self.assertEqual(msg.cmdState.ditherSeq, 'NEEEEEE')


class TestDoApogeeMangaDither(SopCmdTester,unittest.TestCase):
    def _doApogeeMangaDither(self, expect, args=''):
        stages = ['expose', 'dither']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeMangaDither %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_APOGEEMANGA_DITHER)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.mangaExpTime,expect['mangaExpTime'])
        self.assertEqual(msg.cmdState.apogeeExpTime,expect['apogeeExpTime'])
        self.assertEqual(msg.cmdState.mangaDither,expect['mangaDither'])
        return msg

    def test_doApogeeMangaDither_mangaDither(self):
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaNGA dither')
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':450,
                  'mangaDither':'C'}
        self._doApogeeMangaDither(expect)
    def test_doApogeeMangaDither_mangaDither_N(self):
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaNGA dither')
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':450,
                  'mangaDither':'N'}
        args = 'mangaDither=N'
        self._doApogeeMangaDither(expect,args)
    def test_doApogeeMangaDither_apogeeLead(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':500,
                  'mangaDither':'C'}
        args = 'mangaDither=C'
        self._doApogeeMangaDither(expect,args)

    def test_doApogeeMangaDither_apogeeLead_long(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead1000s'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 1000,
                  'mangaDither': 'C'}
        args = 'mangaDither=C'
        self._doApogeeMangaDither(expect, args)

    def test_doApogeeMangaDither_after_apogeeLead_long(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead1000s'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 500,
                  'mangaDither': 'C'}
        args = 'mangaDither=C'
        msg = self._doApogeeMangaDither(expect, args)
        self.assertEqual(msg.cmdState.apogee_long, False)

    def test_doApogeeMangaDither_abort(self):
        self.actorState.doApogeeMangaDither.cmd = self.cmd
        self._run_cmd('doApogeeMangaDither abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doApogeeMangaDither_modify(self):
        """Cannot modify this command, so fail and nothing should change."""
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doApogeeMangaDither mangaDither=N', queue)
        msgNew = self._run_cmd('doApogeeMangaDither mangaDither=S', queue, empty=True)
        self.assertIsNone(msgNew)
        self.assertEqual(msg.cmdState.mangaDither, 'N')
        self._check_cmd(0,2,0,0,True,True)


class TestDoApogeeMangaSequence(SopCmdTester, unittest.TestCase):
    def _doApogeeMangaSequence(self, expect, args=''):
        stages = ['expose', 'dither', 'calibs']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeMangaSequence %s' % (args), queue)
        self.assertEqual(msg.type, sopActor.Msg.DO_APOGEEMANGA_SEQUENCE)
        stages = dict(zip(stages, ['idle'] * len(stages)))
        self.assertEqual(msg.cmdState.stages, stages)
        self.assertEqual(msg.cmdState.mangaExpTime, expect['mangaExpTime'])
        self.assertEqual(msg.cmdState.apogeeExpTime, expect['apogeeExpTime'])
        self.assertEqual(msg.cmdState.mangaDithers, expect['mangaDithers'])
        self.assertEqual(msg.cmdState.count, expect['count'])
        return msg

    def test_doApogeeMangaSequence_mangaDither(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaNGA dither')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 450,
                  'mangaDithers': 'NSE',
                  'count': 2}
        self._doApogeeMangaSequence(expect)

    def test_doApogeeMangaSequence_mangaStare(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaStare'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaNGA stare')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 450,
                  'mangaDithers': 'CCC',
                  'count': 2}
        self._doApogeeMangaSequence(expect)

    def test_doApogeeMangaSequence_MaStar(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeemangaMaStar'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaStar')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 450,
                  'mangaDithers': 'CCC',
                  'count': 2}
        self._doApogeeMangaSequence(expect)

    def test_doApogeeMangaSequence_apogeeLead(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 500,
                  'mangaDithers': 'CC',
                  'count': 2}
        self._doApogeeMangaSequence(expect)

    def test_doApogeeMangaSequence_apogeeLead_CCC_count1(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 500,
                  'mangaDithers': 'CCC',
                  'count': 1}
        args = 'mangaDithers=CCC count=1'
        self._doApogeeMangaSequence(expect, args)

    def test_doApogeeMangaSequence_abort(self):
        self.actorState.doApogeeMangaSequence.cmd = self.cmd
        self._run_cmd('doApogeeMangaSequence abort', None)
        self.assertTrue(self.actorState.aborting)

    def _doApogeeMangaSequence_modify(self, args1, args2, cmd_levels=(0,11,0,0), didFail=False):
        self._update_cart(2, 'APOGEE-2&MaNGA', 'MaNGA dither')
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doApogeeMangaSequence %s' % args1, queue)
        msgNew = self._run_cmd('doApogeeMangaSequence %s' % args2, queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(*cmd_levels, finish=True, didFail=didFail)
        return msg

    def test_doApogeeMangaSequence_modify_count1(self):
        msg = self._doApogeeMangaSequence_modify('mangaDithers=ESN count=2', 'mangaDithers=ESN count=1')
        self.assertEqual(msg.cmdState.mangaDithers, 'ESN')
        self.assertEqual(msg.cmdState.count, 1)
        self.assertEqual(msg.cmdState.mangaDitherSeq, 'EESN')

    def test_doApogeeMangaSequence_modify_count2(self):
        msg = self._doApogeeMangaSequence_modify('mangaDithers=NSE count=1', 'mangaDithers=NSE count=2')
        self.assertEqual(msg.cmdState.mangaDithers, 'NSE')
        self.assertEqual(msg.cmdState.count, 2)
        self.assertEqual(msg.cmdState.mangaDitherSeq, 'NNSENSE')

    def test_doApogeeMangaSequence_modify_dithers(self):
        msg = self._doApogeeMangaSequence_modify('mangaDithers=NSE count=2', 'mangaDithers=SEN count=1')
        self.assertEqual(msg.cmdState.count, 1)
        self.assertEqual(msg.cmdState.mangaDithers, 'SEN')
        self.assertEqual(msg.cmdState.mangaDitherSeq, 'NSEN')

    def test_doApogeeMangaSequence_modify_dithers_alls(self):
        msg = self._doApogeeMangaSequence_modify('mangaDithers=NSE count=2', 'mangaDithers=SS count=2')
        self.assertEqual(msg.cmdState.count, 2)
        self.assertEqual(msg.cmdState.mangaDithers, 'SS')
        self.assertEqual(msg.cmdState.mangaDitherSeq, 'NSSSS')

    def test_doApogeeMangaSequence_after_apogeeLead_long(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead1000s'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')

        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apogeeLead'])
        self._update_cart(2, 'APOGEE-2&MaNGA', 'APOGEE lead')
        expect = {'mangaExpTime': 900,
                  'apogeeExpTime': 500,
                  'mangaDithers': 'CC',
                  'count': 2}
        msg = self._doApogeeMangaSequence(expect)
        self.assertEqual(msg.cmdState.apogee_long, False)


class TestGotoField(SopCmdTester,unittest.TestCase):
    def _gotoField(self, cart, survey, expect, stages, args, cmd_levels=(0,2,0,0)):
        """
        default cmd_levels: Should always output *Stages and *States.
        """
        if survey == 'APOGEE':
            allStages = ['slew', 'guider', 'cleanup']
        else:
            allStages = ['slew', 'hartmann', 'calibs', 'guider', 'cleanup']
        stages = build_active_stages(allStages,stages)

        self._update_cart(cart,survey)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('gotoField %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_FIELD)
        self.assertEqual(msg.cmdState.stages,stages)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.cmdState.arcTime,expect.get('arcTime',4))
        self.assertEqual(msg.cmdState.flatTime,expect.get('flatTime',25))
        self.assertEqual(msg.cmdState.guiderTime,expect.get('guiderTime',5))
        self.assertEqual(msg.cmdState.guiderFlatTime,expect.get('guiderFlatTime',0.5))
        self.assertEqual(msg.cmdState.ra,expect.get('ra',0))
        self.assertEqual(msg.cmdState.dec,expect.get('dec',0))
        self.assertEqual(msg.cmdState.doSlew,expect.get('doSlew',True))
        self.assertEqual(msg.cmdState.doHartmann,expect.get('doHartmann',True))
        self.assertEqual(msg.cmdState.doCalibs,expect.get('doCalibs',True))
        self.assertEqual(msg.cmdState.didArc,expect.get('didArc',False))
        self.assertEqual(msg.cmdState.didFlat,expect.get('didFlat',False))
        self.assertEqual(msg.cmdState.doGuiderFlat,expect.get('doGuiderFlat',True))
        self.assertEqual(msg.cmdState.doGuider,expect.get('doGuider',True))

    def test_gotoField_boss_default(self):
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'')
    def test_gotoField_boss_noSlew(self):
        stages = ['hartmann','calibs','guider','cleanup']
        expect = {'doSlew':False}
        self._gotoField(11,'BOSS',expect,stages,'noSlew')
    def test_gotoField_boss_noHartmann(self):
        stages = ['slew','calibs','guider','cleanup']
        expect = {'ra':10,'dec':20,
                  'doHartmann':False}
        self._gotoField(11,'BOSS',expect,stages,'noHartmann')
    def test_gotoField_boss_noCalibs(self):
        stages = ['slew','hartmann','guider','cleanup']
        expect = {'ra':10,'dec':20,
                  'doCalibs':False, 'doArc':False, 'doFlat':False}
        self._gotoField(11,'BOSS',expect,stages,'noCalibs')
    def test_gotoField_boss_noGuider(self):
        stages = ['slew','hartmann','calibs','cleanup']
        expect = {'ra':10,'dec':20,
                  'doGuider':False, 'doGuiderFlat':False}
        self._gotoField(11,'BOSS',expect,stages,'noGuider')

    def test_gotoField_boss_0s_flat(self):
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'flatTime':0,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'flatTime=0',cmd_levels=(0,2,1,0))
    def test_gotoField_boss_0s_arc(self):
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':0,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'arcTime=0',cmd_levels=(0,2,1,0))


    def test_gotoField_boss_after_apogee(self):
        """BOSS gotofield should revert to useful defaults after an APOGEE gotofield."""
        self._update_cart(1,'APOGEE')
        self._pre_command('gotoField',self.actorState.queues[sopActor.MASTER])
        self.actorState.gotoField.cmd = None
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'')

    def test_gotoField_apogee_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apogee'])
        stages = ['slew','guider','cleanup']
        expect = {'ra':20,'dec':30,
                  'doHartmann':False,'doCalibs':False}
        self._gotoField(1,'APOGEE',expect,stages,'')

    def test_gotoField_manga_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['mangaDither'])
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'ra':30,'dec':40}
        self._gotoField(2,'MaNGA',expect,stages,'')

    def test_gotoField_abort(self):
        self.actorState.gotoField.cmd = self.cmd
        self._run_cmd('gotoField abort', None)
        self.assertTrue(self.actorState.aborting)

    def _gotoField_modify(self, args1, args2, cmd_levels=(0,2,0,0)):
        """Modify a gotoField cmd, only testing cmd_levels. Other tests should come after."""
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('gotoField %s'%args1, queue)
        msgNew = self._run_cmd('gotoField %s'%args2, queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(*cmd_levels,finish=True)
        return msg

    # @unittest.skip("Properly doing these is going to be complicated...")
    def test_gotoField_modify_cancel_calibs(self):
        msg = self._gotoField_modify('','noCalibs', cmd_levels=(0,14,0,0))
        self.assertEqual(msg.cmdState.doCalibs, False)


class TestGotoPosition(SopCmdTester, unittest.TestCase):
    """Tests various commands that use gotoPosition."""

    def _gotoPosition(self, cmd, args, expect):
        allStages = ['slew']
        stages = build_active_stages(allStages, expect.get('stages',
                                                           allStages))
        queue = myGlobals.actorState.queues[sopActor.SLEW]
        msg = self._run_cmd('%s %s' % (cmd, args), queue)
        self.assertEqual(msg.type, sopActor.Msg.GOTO_POSITION)
        self.assertEqual(msg.cmdState.alt, expect.get('alt'))
        self.assertEqual(msg.cmdState.az, expect.get('az'))
        self.assertEqual(msg.cmdState.rot, expect.get('rot'))
        self.assertEqual(msg.cmdState.stages, stages)

    def test_gotoAll60(self):
        expect = {'stages': ['slew'], 'alt': 60, 'rot': 60, 'az': 60}
        cmd = 'gotoAll60'
        self._gotoPosition(cmd, '', expect)

    def test_gotoInstrumentChange(self):
        expect = {'stages': ['slew'], 'alt': 90, 'rot': 0, 'az': 121}
        cmd = 'gotoInstrumentChange'
        self._gotoPosition(cmd, '', expect)

    def test_gotoStow(self):
        expect = {'stages': ['slew'], 'alt': 30, 'rot': 0, 'az': 121}
        cmd = 'gotoStow'
        self._gotoPosition(cmd, '', expect)

    def test_gotoStow60(self):
        expect = {'stages': ['slew'], 'alt': 60, 'rot': 00, 'az': 121}
        cmd = 'gotoStow60'
        self._gotoPosition(cmd, '', expect)


class TestDoBossScience(SopCmdTester,unittest.TestCase):
    def _doBossScience(self, expect, args, cmd_levels=(0,2,0,0), didFail=False):
        stages = ['expose',]
        stages = dict(zip(stages,['idle']*len(stages)))

        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossScience %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_BOSS_SCIENCE)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.nExp,expect.get('nExp',1))
        self.assertEqual(msg.cmdState.expTime,expect.get('expTime',900))
        self.assertEqual(msg.cmdState.index,0)

    def test_doBossScience_default(self):
        self._doBossScience({},'')
    def test_doBossScience_2_exp(self):
        self._doBossScience({'nExp':2},'nExp=2')
    def test_doBossScience_expTime_100(self):
        self._doBossScience({'expTime':100},'expTime=100')

    def _doBossScience_fails(self, args):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossScience %s'%args, queue, empty=True)
        self.assertIsNone(msg)
        self._check_cmd(0,2,0,0,True,True)
    def test_doBossScience_0_exp_fails(self):
        self._doBossScience_fails('nexp=0')
    def test_doBossScience_0_expTime_fails(self):
        self._doBossScience_fails('expTime=0')

    def test_doBossScience_abort(self):
        self.actorState.doBossScience.cmd = self.cmd
        self._run_cmd('doBossScience abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doBossScience_modify(self):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doBossScience nexp=2', queue)
        msgNew = self._run_cmd('doBossScience nexp=1 expTime=100', queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(0,11,0,0,True)
        self.assertEqual(msg.cmdState.nExp, 1)
        self.assertEqual(msg.cmdState.expTime, 100)


class TestDoBossCalibs(SopCmdTester,unittest.TestCase):
    def _bossCalibs(self, expect, stages, args,):
        allStages = ['bias', 'dark', 'flat', 'arc', 'cleanup']
        stages = build_active_stages(allStages,stages)

        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossCalibs %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_BOSS_CALIBS)
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.darkTime,expect.get('darkTime',900))
        self.assertEqual(msg.cmdState.flatTime,expect.get('flatTime',25))
        self.assertEqual(msg.cmdState.arcTime,expect.get('arcTime',4))
        self.assertEqual(msg.cmdState.guiderFlatTime,expect.get('guiderFlatTime',0.5))
        self.assertEqual(msg.cmdState.nBias,expect.get('nBias',0))
        self.assertEqual(msg.cmdState.nDark,expect.get('nDark',0))
        self.assertEqual(msg.cmdState.nFlat,expect.get('nFlat',0))
        self.assertEqual(msg.cmdState.nArc,expect.get('nArc',0))

    def test_doBossCalibs_bias(self):
        stages = ['bias','cleanup']
        expect = {'nBias':2}
        self._bossCalibs(expect,stages,'nbias=2')
    def test_doBossCalibs_dark(self):
        stages = ['dark','cleanup']
        expect = {'nDark':2, 'darkTime':100}
        self._bossCalibs(expect,stages,'ndark=2 darkTime=100')
    def test_doBossCalibs_flat(self):
        stages = ['flat','cleanup']
        expect = {'nFlat':2, 'flatTime':10, 'guiderFlatTime':10}
        self._bossCalibs(expect,stages,'nflat=2 flatTime=10 guiderFlatTime=10')
    def test_doBossCalibs_arc(self):
        stages = ['arc','cleanup']
        expect = {'nArc':2, 'arcTime':10}
        self._bossCalibs(expect,stages,'narc=2 arcTime=10')
    def test_doBossCalibs_all(self):
        stages = ['bias','dark','flat','arc','cleanup']
        expect = {'nBias':1,'nDark':1,'nFlat':1,'nArc':1}
        self._bossCalibs(expect,stages,'nbias=1 ndark=1 nflat=1 narc=1')

    def test_no_cart_loaded(self):
        """guiderFlatTime is 0 if no cart is loaded."""
        self._update_cart(-1, None)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossCalibs nflat=1',queue)
        self.assertEqual(msg.cmdState.guiderFlatTime,0)

    def _doBossCalibs_fails(self, args, nInfo=2):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossCalibs %s'%args, queue, empty=True)
        self.assertIsNone(msg)
        self._check_cmd(0,nInfo,0,0,True,True)
    def test_apogee_loaded_fails(self):
        """Fail if APOGEE is loaded with no bypass."""
        self._update_cart(1, 'APOGEE')
        self._doBossCalibs_fails('nflat=1', nInfo=0)
    def test_no_exposures_fails(self):
        self._doBossCalibs_fails('')
    def test_zero_dark_time_fails(self):
        self._doBossCalibs_fails('ndark=1 darkTime=0')

    def test_doBossCalibs_abort(self):
        self.actorState.doBossCalibs.cmd = self.cmd
        self._run_cmd('doBossCalibs abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doBossCalibs_modify(self):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doBossCalibs nbias=2 ndark=2 nflat=2 narc=2', queue)
        msgNew = self._run_cmd('doBossCalibs nbias=1 ndark=1 nflat=1 narc=1 darkTime=10 flatTime=10 guiderFlatTime=10 arcTime=10', queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(0,13,0,0,True)
        for nExp in ['nBias', 'nDark', 'nFlat', 'nArc']:
            self.assertEqual(getattr(msg.cmdState,nExp), 1)
        for expTime in ['darkTime', 'flatTime', 'guiderFlatTime', 'arcTime']:
            self.assertEqual(getattr(msg.cmdState,expTime), 10)


class TestHartmann(SopCmdTester,unittest.TestCase):
    def _hartmann(self, expect, stages, args):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('hartmann %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.HARTMANN)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.expTime,expect['expTime'])

    def test_hartmann_default(self):
        stages = ['left','right','cleanup']
        expect = {'expTime':4}
        self._hartmann(expect,stages,'')

    def test_hartmann_expTime5(self):
        stages = ['left','right','cleanup']
        expect = {'expTime':5}
        self._hartmann(expect,stages,'expTime=5')


class TestCollimateBoss(SopCmdTester,unittest.TestCase):
    def _collimateBoss(self):
        stages = {'collimate', 'cleanup'}
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('collimateBoss',queue)
        self.assertEqual(msg.type,sopActor.Msg.COLLIMATE_BOSS)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
    def test_collimateBoss(self):
        self._collimateBoss()


class TestDoApogeeScience(SopCmdTester,unittest.TestCase):
    def _doApogeeScience(self, expect, args, cmd_levels=(0,2,0,0), didFail=False):
        stages = ['expose',]
        stages = dict(zip(stages,['idle']*len(stages)))

        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeScience %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_APOGEE_EXPOSURES)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.expTime,expect.get('expTime',500))
        self.assertEqual(msg.cmdState.ditherPairs,expect.get('ditherPairs',4))
        self.assertEqual(msg.cmdState.index,0)
        self.assertEqual(msg.cmdState.expType,'object')

    def test_doApogeeScience_default(self):
        self._doApogeeScience({},'')
    def test_doApogeeScience_450_expTime(self):
        self._doApogeeScience({'expTime':450},'expTime=450')
    def test_doApogeeScience_2_pairs(self):
        self._doApogeeScience({'ditherPairs':2},'ditherPairs=2')

    def _doApogeeScience_fails(self, args):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeScience %s'%args, queue, empty=True)
        self.assertIsNone(msg)
        self._check_cmd(0,2,0,0,True,True)
    def test_no_count_fails(self):
        self._doApogeeScience_fails('ditherPairs=0')

    def test_doApogeeScience_abort(self):
        self.actorState.doApogeeScience.cmd = self.cmd
        self._run_cmd('doApogeeScience abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doApogeeScience_modify_ditherPairs(self):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doApogeeScience', queue)
        msgNew = self._run_cmd('doApogeeScience ditherPairs=3', queue, empty=True)
        self.assertIsNone(msgNew)
        self.assertEqual(msg.cmdState.ditherPairs, 3)


class TestDoApogeeSkyFlats(SopCmdTester,unittest.TestCase):
    def _doApogeeSkyFlats(self, expect, args):
        stages = ['offset','expose']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeSkyFlats %s'%args,queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_APOGEE_SKY_FLATS)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.expTime,expect.get('expTime',150))
        self.assertEqual(msg.cmdState.ditherPairs,expect.get('ditherPairs',2))
        self.assertEqual(msg.cmdState.index,0)
        self.assertEqual(msg.cmdState.expType,'object')
        self.assertEqual(msg.cmdState.comment,'sky flat, offset 0.01 degree in RA')

    def test_doApogeeSkyFlats_default(self):
        self._doApogeeSkyFlats({},'')
    def test_doApogeeSkyFlats_100_expTime(self):
        self._doApogeeSkyFlats({'ditherPairs':3, 'expTime':100},'ditherPairs=3 expTime=100')
    def test_doApogeeSkyFlats_1_pairs(self):
        self._doApogeeSkyFlats({'ditherPairs':1},'ditherPairs=1')

    def test_doApogeeSkyFlats_abort(self):
        self.actorState.doApogeeSkyFlats.cmd = self.cmd
        self._run_cmd('doApogeeSkyFlats abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doApogeeSkyFlats_modify(self):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        # create something we can modify.
        msg = self._run_cmd('doApogeeSkyFlats ditherPairs=2 expTime=150', queue)
        msgNew = self._run_cmd('doApogeeSkyFlats ditherPairs=1', queue, empty=True)
        self.assertIsNone(msgNew)
        self.assertEqual(msg.cmdState.ditherPairs, 1)

class TestDoApogeeDomeFlat(SopCmdTester,unittest.TestCase):
    def test_doApogeeDomeFlat(self):
        stages = ['domeFlat']
        queue = myGlobals.actorState.queues[sopActor.SLEW]
        msg = self._run_cmd('doApogeeDomeFlat',queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_APOGEE_DOME_FLAT)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)

    def test_doApogeeDomeFlat_abort(self):
        self.actorState.doApogeeDomeFlat.cmd = self.cmd
        self._run_cmd('doApogeeDomeFlat abort', None)
        self.assertTrue(self.actorState.aborting)

    def test_doApogeeDomeFlat_modify(self):
        queue = myGlobals.actorState.queues[sopActor.SLEW]
        # create something we can modify.
        self._run_cmd('doApogeeDomeFlat', queue)
        msgNew = self._run_cmd('doApogeeDomeFlat', queue, empty=True)
        self.assertIsNone(msgNew)
        self._check_cmd(0,2,0,0,True,True)


class TestIsSlewingDisabled(SopCmdTester,unittest.TestCase):
    def _slewing_is_disabled(self,expect):
        self.cmdState.reinitialize(cmd=self.cmd)
        result = self.sopCmd.isSlewingDisabled(self.cmd)
        self.assertIn(expect,result)
    def test_slewing_disabled_apogee_science(self):
        self._update_cart(2, 'APOGEE')
        self.cmdState = self.actorState.doApogeeScience
        self._slewing_is_disabled('slewing disallowed for APOGEE,')
    def test_slewing_disabled_apogee_sky_flats(self):
        self._update_cart(2, 'APOGEE')
        self.cmdState = self.actorState.doApogeeSkyFlats
        self._slewing_is_disabled('slewing disallowed for APOGEE,')
    def test_slewing_disabled_boss_science(self):
        self._update_cart(11, 'BOSS')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doBossScience
        self._slewing_is_disabled('slewing disallowed for BOSS,')
    def test_slewing_disabled_manga_dither(self):
        self._update_cart(2, 'MaNGA')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doMangaDither
        self._slewing_is_disabled('slewing disallowed for MaNGA,')
    def test_slewing_disabled_manga_sequence(self):
        self._update_cart(2, 'MaNGA')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doMangaSequence
        self._slewing_is_disabled('slewing disallowed for MaNGA,')
    def test_slewing_disabled_apogeemanga_dither(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        self._update_cart(2, 'APOGEE-2&MaNGA')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doApogeeMangaDither
        self._slewing_is_disabled('slewing disallowed for APOGEE&MaNGA,')
    def test_slewing_disabled_apogeemanga_sequence(self):
        sopTester.updateModel('platedb',
                              TestHelper.platedbState['apgoeemangaDither'])
        self._update_cart(2, 'APOGEE-2&MaNGA')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doApogeeMangaSequence
        self._slewing_is_disabled('slewing disallowed for APOGEE&MaNGA,')

    def _slewing_is_enabled(self):
        result = self.sopCmd.isSlewingDisabled(self.cmd)
        self.assertFalse(result)
    def test_slewing_enabled_bogus_cart(self):
        self._update_cart(2, '???')
        self._slewing_is_enabled()
    def test_slewing_enabled_apogee_not_alive(self):
        self._update_cart(2, 'APOGEE')
        self.cmd.finished = True
        self.actorState.doApogeeScience.reinitialize(self.cmd)
        self._slewing_is_enabled()
    def test_slewing_enabled_boss_not_alive(self):
        self._update_cart(11, 'BOSS')
        self.cmd.finished = True
        self.actorState.doBossScience.reinitialize(self.cmd)
        self._slewing_is_enabled()
    def test_slewing_enabled_manga_not_alive(self):
        self._update_cart(2, 'MaNGA')
        self.cmd.finished = True
        self.actorState.doMangaDither.reinitialize(self.cmd)
        self._slewing_is_enabled()


if __name__ == '__main__':
    verbosity = 2

    suite = None
    # to test just one piece
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoGangChange)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestDoMangaDither)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoMangaSequence)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoApogeeMangaSequence)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestClassifyCartridge)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestUpdateCartridge)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestHartmann)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoField)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoPosition)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoBossCalibs)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoBossScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoApogeeScience)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestDoApogeeSkyFlats)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestStatus)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestIsSlewingDisabled)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestBypass)
    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
