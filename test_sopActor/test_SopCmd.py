"""
Tests of the SopCmd functions, without executing any threads.

Each of these tests should confirm that a SopCmd command call calls the correct
queue with an appropriately crafted CmdState and other relevant parameters.

If these tests work correctly, each masterThread function should work
correctly when called via a SopCmd (assuming test_masterThread clears).
"""
import unittest

from sopActor import *
import sopActor
import sopActor.myGlobals as myGlobals
from sopActor import Queue

from actorcore import TestHelper
import sopTester

class SopCmdTester(sopTester.SopTester):
    def setUp(self):
        self.verbose = True
        super(SopCmdTester,self).setUp()
        self.timeout = 1
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues = {}
        myGlobals.actorState.queues[sopActor.MASTER] = Queue('master')
        self.cmd.verbose = False # don't spam initial loadCart messages
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose
        self._clear_bypasses()

    def _prep_bypass(self,bypass,clear=False):
        """
        Help setting up a bypass, so we don't spam with status messages.
        Set clear to unset all bypasses before setting the specified one.
        """
        self.cmd.verbose = False
        if clear:
            self._clear_bypasses()
        Bypass.set(bypass,True)
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose
    
    def _pre_command(self, command, queue):
        """Run a text command in advance, without being verbose and clearing any messages."""
        self.cmd.verbose = False
        self._run_cmd(command,queue)
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose
        # in case the above command "finishes"
        self.cmd = TestHelper.Cmd(verbose=self.verbose)


class TestBypass(SopCmdTester,unittest.TestCase):
    """Test setting and clearing bypasses with the sop bypass command."""
    def _bypass_set(self, system):
        self._clear_bypasses()
        self.cmd.rawCmd = 'bypass subSystem=%s'%system
        self.actor.runActorCmd(self.cmd)
        for item in Bypass._bypassed:
            # all others should be cleared.
            if item != system:
                self.assertFalse(Bypass.get(name=item))
            else:
                self.assertTrue(Bypass.get(name=item))
    def test_bypass_isBoss(self):
        self._bypass_set('isBoss')
    def test_bypass_isApogee(self):
        self._bypass_set('isApogee')
    def test_bypass_isMangaStare(self):
        self._bypass_set('isMangaStare')
    def test_bypass_isMangaDither(self):
        self._bypass_set('isMangaDither')
    def test_bypass_isApogeeLead(self):
        self._bypass_set('isApogeeLead')
    def test_bypass_isApogeeMangaDither(self):
        self._bypass_set('isApogeeMangaDither')
    def test_bypass_isApogeeMangaStare(self):
        self._bypass_set('isApogeeMangaStare')

    def test_bypass_gangCart(self):
        self._bypass_set('gangCart')
    def test_bypass_gangPodium(self):
        self._bypass_set('gangPodium')


class TestClassifyCartridge(SopCmdTester,unittest.TestCase):
    def _classifyCartridge(self,nCart,plateType,surveyMode,expect):
        """Expect is a tuple of expected survey and surveyMode IDs from sopActor."""
        self.sopCmd.classifyCartridge(self.cmd,nCart,plateType,surveyMode)
        sopState = self.actorState
        self.assertEqual(sopState.survey,expect[0])
        self.assertEqual(sopState.surveyMode,expect[1])
    
    def test_classifyCartridge_bad(self):
        self._classifyCartridge(-1,'unknown',None,[sopActor.UNKNOWN,None])
    def test_classifyCartridge_bad_survey(self):
        self._classifyCartridge(1,'mangled','APOGEE lead',[sopActor.UNKNOWN,sopActor.APOGEELEAD])
    def test_classifyCartridge_bad_surveyMode(self):
        self._classifyCartridge(11,'MaNGA','mangled',[sopActor.MANGA,None])
    def test_classifyCartridge_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        self._classifyCartridge(11,'BOSS','None',[sopActor.BOSS,None])
    def test_classifyCartridge_eboss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        self._classifyCartridge(11,'eBOSS','None',[sopActor.BOSS,None])
    def test_classifyCartridge_apogee(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        self._classifyCartridge(1,'APOGEE','None',[sopActor.APOGEE,None])
    def test_classifyCartridge_apogee2(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        self._classifyCartridge(1,'APOGEE-2','None',[sopActor.APOGEE,None])
    def test_classifyCartridge_mangaDither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        self._classifyCartridge(2,'MaNGA','MaNGA dither',[sopActor.MANGA,sopActor.MANGADITHER])
    def test_classifyCartridge_mangaStare(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaStareLoaded'])
        self._classifyCartridge(2,'MaNGA','MaNGA stare',[sopActor.MANGA,sopActor.MANGASTARE])
    def test_classifyCartridge_apogee_lead(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLeadLoaded'])
        self._classifyCartridge(3,'APOGEE-2&MaNGA','APOGEE lead',[sopActor.APOGEEMANGA,sopActor.APOGEELEAD])
        self._classifyCartridge(3,'APOGEE&MaNGA','APOGEE lead',[sopActor.APOGEEMANGA,sopActor.APOGEELEAD])
    def test_classifyCartridge_apogeemanga_dither(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        self._classifyCartridge(3,'APOGEE-2&MaNGA','MaNGA dither',[sopActor.APOGEEMANGA,sopActor.MANGADITHER])
        self._classifyCartridge(3,'APOGEE&MaNGA','MaNGA dither',[sopActor.APOGEEMANGA,sopActor.MANGADITHER])
    def test_classifyCartridge_apogeemanga_stare(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaStareLoaded'])
        self._classifyCartridge(3,'APOGEE-2&MaNGA','MaNGA stare',[sopActor.APOGEEMANGA,sopActor.MANGASTARE])
        self._classifyCartridge(3,'APOGEE&MaNGA','MaNGA stare',[sopActor.APOGEEMANGA,sopActor.MANGASTARE])
    
    def test_classifyCartridge_boss_bypass(self):
        self._prep_bypass('isBoss',clear=True)
        self._classifyCartridge(2,'APOGEE','None',[sopActor.BOSS,None])
    def test_classifyCartridge_apogee_bypass(self):
        self._prep_bypass('isApogee',clear=True)
        self._classifyCartridge(11,'BOSS','None',[sopActor.APOGEE,None])
    def test_classifyCartridge_mangaStare_bypass(self):
        self._prep_bypass('isMangaStare',clear=True)
        self._classifyCartridge(2,'APOGEE','None',[sopActor.MANGA,sopActor.MANGASTARE])
    def test_classifyCartridge_mangaDither_bypass(self):
        self._prep_bypass('isMangaDither',clear=True)
        self._classifyCartridge(2,'APOGEE','None',[sopActor.MANGA,sopActor.MANGADITHER])
    def test_classifyCartridge_apogeelead_bypass(self):
        self._prep_bypass('isApogeeLead',clear=True)
        self._classifyCartridge(11,'BOSS','None',[sopActor.APOGEEMANGA,sopActor.APOGEELEAD])
    def test_classifyCartridge_apogeemangaDither_bypass(self):
        self._prep_bypass('isApogeeMangaDither',clear=True)
        self._classifyCartridge(11,'BOSS','None',[sopActor.APOGEEMANGA,sopActor.MANGADITHER])
    def test_classifyCartridge_apogeemangaStare_bypass(self):
        self._prep_bypass('isApogeeMangaStare',clear=True)
        self._classifyCartridge(11,'BOSS','None',[sopActor.APOGEEMANGA,sopActor.MANGASTARE])


class TestUpdateCartridge(SopCmdTester,unittest.TestCase):
    """Confirm that we get the right validCommands from each survey type."""
    def _updateCartridge(self, nCart, survey, surveyMode, expected):
        self.sopCmd.updateCartridge(nCart, survey, surveyMode)
        sop = myGlobals.actorState.models['sop']
        self.assertEqual(sop.keyVarDict['surveyCommands'].getValue(), expected['surveyCommands'])

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

    def test_updateCartridge_apogee2(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE-2','None',expected)

    def test_updateCartridge_apogeemanga(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaDitherLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeMangaCommands['surveyCommands']
        self._updateCartridge(1,'APOGEE-2&MaNGA','MaNGA dither',expected)

class TestStatus(SopCmdTester,unittest.TestCase):
    def _status(self, nInfo, args=''):
        self._run_cmd('status %s'%args, None)
        self._check_cmd(0,nInfo,0,0,True)
    def test_status(self):
        self._status(51)
    def test_status_geek(self):
        self._status(53,args='geek')
    def test_status_noFinish(self):
        self.sopCmd.status(self.cmd,finish=False)
        self._check_cmd(0,51,0,0,False)

    def _oneCommand(self, nInfo, oneCommand):
        """
        nInfo here is the number of messages specific to oneCommand.
        This is usually 3, or 4 if there are userKeys in that CmdState.
        """
        self.sopCmd.status(self.cmd,oneCommand=oneCommand)
        self._check_cmd(0,6+nInfo,0,0,True)
    def test_gotoGangChange(self):
        self._oneCommand(3,'gotoGangChange')
    def test_doApogeeDomeFlat(self):
        self._oneCommand(3,'doApogeeDomeFlat')
    def test_hartmann(self):
        self._oneCommand(3,'hartmann')
    def test_gotoField(self):
        self._oneCommand(3,'gotoField')
    def test_doBossCalibs(self):
        self._oneCommand(4,'doBossCalibs')
    def test_doApogeeScience(self):
        self._oneCommand(4,'doApogeeScience')
    def test_doApogeeSkyFlats(self):
        self._oneCommand(3,'doApogeeSkyFlats')
    def test_doBossScience(self):
        self._oneCommand(4,'doBossScience')
    def test_doMangaSequence(self):
        self._oneCommand(4,'doMangaSequence')
    def test_doMangaDither(self):
        self._oneCommand(3,'doMangaDither')
    def test_doApogeeMangaDither(self):
        self._oneCommand(3,'doApogeeMangaDither')
    def test_doApogeeMangaSequence(self):
        self._oneCommand(4,'doApogeeMangaSequence')
    def test_gotoStow(self):
        self._oneCommand(2,'gotoStow')
    def test_gotoInstrumentChange(self):
        self._oneCommand(2,'gotoInstrumentChange')


class TestGotoGangChange(SopCmdTester,unittest.TestCase):
    def _gotoGangChange(self, nCart, survey, args, expect):
        self._update_cart(nCart, survey)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('gotoGangChange %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_GANG_CHANGE)
        self.assertEqual(msg.cmdState.alt,expect.get('alt',45))
        stages = dict(zip(expect['stages'],['idle']*len(expect['stages'])))
        self.assertEqual(msg.cmdState.stages,stages)
    
    def test_gotoGangChange_ok(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat', 'slew'],'alt':15}
        self._gotoGangChange(1,'apogee','alt=15',expect)
    def test_gotoGangChange_stop(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {}
        self._gotoGangChange(1,'apogee','stop',expect)
    def test_gotoGangChange_abort(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat', 'slew'],
                  'abort':True}
        self._gotoGangChange(1,'apogee','abort',expect)
    def test_gotoGangChange_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expect = {'stages':['domeFlat','slew']}
        self._gotoGangChange(11,'boss','',expect)


class TestDoMangaDither(SopCmdTester,unittest.TestCase):
    def _doMangaDither(self, expect, args=''):
        stages = ['expose', 'dither']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaDither %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_MANGA_DITHER)
        stages = dict(zip(stages,['idle']*len(stages)))
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
        expect = {}
        self._doMangaDither(expect,'abort')
    def test_doMangeaDither_stop(self):
        expect = {}
        self._doMangaDither(expect,'stop')


class TestDoMangaSequence(SopCmdTester,unittest.TestCase):
    def _doMangaSequence(self,expect,args):
        stages = ['expose', 'calibs', 'dither']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaSequence %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_MANGA_SEQUENCE)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.ditherSeq,expect['ditherSeq'])
    
    def test_doMangaSequence_default(self):
        expect = {'expTime':900,
                  'ditherSeq':'NSE'*3}
        self._doMangaSequence(expect,'')
    def test_doMangaSequence_one_set(self):
        expect = {'expTime':900,
                  'ditherSeq':'NSE'}
        self._doMangaSequence(expect,'count=1')

    def test_doMangaSequence_abort(self):
        expect = {}
        self._doMangaSequence(expect,'abort')
    def test_doMangeaSequence_stop(self):
        expect = {}
        self._doMangaSequence(expect,'stop')


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

    def test_doApogeeMangaDither_default(self):
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':450,
                  'mangaDither':'C'
                  }
        self._doApogeeMangaDither(expect)
    def test_doApogeeMangaDither_N(self):
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':450,
                  'mangaDither':'N'
                  }
        args = 'mangaDither=N'
        self._doApogeeMangaDither(expect,args)
    def test_doApogeeMangaDither_apogeeLead(self):
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':500,
                  'mangaDither':'C'
                  }
        args = 'apogeeExpTime=500'
        self._doApogeeMangaDither(expect,args)

    def test_doApogeeMangaDither_abort(self):
        expect = {}
        self._doApogeeMangaDither(expect,'abort')
    def test_doApogeeMangeaDither_stop(self):
        expect = {}
        self._doApogeeMangaDither(expect,'stop')


class TestDoApogeeMangaSequence(SopCmdTester,unittest.TestCase):
    def _doApogeeMangaSequence(self, expect, args=''):
        stages = ['expose', 'dither', 'calibs']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeMangaSequence %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_APOGEEMANGA_SEQUENCE)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.mangaExpTime,expect['mangaExpTime'])
        self.assertEqual(msg.cmdState.apogeeExpTime,expect['apogeeExpTime'])
        self.assertEqual(msg.cmdState.mangaDithers,expect['mangaDithers'])

    def test_doApogeeMangaSequence_default(self):
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':450,
                  'mangaDithers':'NSE'
                  }
        self._doApogeeMangaSequence(expect)
    def test_doApogeeMangaSequence_apogeeLead(self):
        expect = {'mangaExpTime':900,
                  'apogeeExpTime':500,
                  'mangaDithers':'CCC'
                  }
        args = 'apogeeExpTime=500 mangaDithers=CCC'
        self._doApogeeMangaSequence(expect,args)

    def test_doApogeeMangaSequence_abort(self):
        expect = {}
        self._doApogeeMangaSequence(expect,'abort')
    def test_doApogeeMangeaSequence_stop(self):
        expect = {}
        self._doApogeeMangaSequence(expect,'stop')


class TestGotoField(SopCmdTester,unittest.TestCase):
    def _gotoField(self, cart, survey, expect, stages, args, cmd_levels=(0,2,0,0)):
        """
        default cmd_levels: Should always output *Stages and *States.
        """
        self._update_cart(cart,survey)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('gotoField %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_FIELD)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self._check_levels(*cmd_levels)
        self.assertEqual(msg.cmdState.arcTime,expect.get('arcTime',4))
        self.assertEqual(msg.cmdState.flatTime,expect.get('flatTime',30))
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
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'')
    def test_gotoField_boss_noSlew(self):
        stages = ['hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'doSlew':False}
        self._gotoField(11,'BOSS',expect,stages,'noSlew')
    def test_gotoField_boss_noHartmann(self):
        stages = ['slew','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20,
                  'doHartmann':False}
        self._gotoField(11,'BOSS',expect,stages,'noHartmann')
    def test_gotoField_boss_noCalibs(self):
        stages = ['slew','hartmann','guider','cleanup']
        expect = {'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20,
                  'doCalibs':False, 'doArc':False, 'doFlat':False}
        self._gotoField(11,'BOSS',expect,stages,'noCalibs')
    def test_gotoField_boss_noGuider(self):
        stages = ['slew','hartmann','calibs','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'ra':10,'dec':20,
                  'doGuider':False, 'doGuiderFlat':False}
        self._gotoField(11,'BOSS',expect,stages,'noGuider')

    def test_gotoField_boss_0s_flat(self):
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':0,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'flatTime=0',cmd_levels=(0,2,1,0))
    def test_gotoField_boss_0s_arc(self):
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':0,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'arcTime=0',cmd_levels=(0,2,1,0))


    def test_gotoField_boss_after_apogee(self):
        """BOSS gotofield should revert to useful defaults after an APOGEE gotofield."""
        self._update_cart(1,'APOGEE')
        self._pre_command('gotoField',self.actorState.queues[sopActor.MASTER])
        self.actorState.gotoField.cmd = None
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20}
        self._gotoField(11,'BOSS',expect,stages,'')

    def test_gotoField_apogee_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apogee'])
        stages = ['slew','guider','cleanup']
        expect = {'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':20,'dec':30,
                  'doHartmann':False,'doCalibs':False}
        self._gotoField(1,'APOGEE',expect,stages,'')

    def test_gotoField_manga_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaDitherLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['mangaDither'])
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':30,'dec':40}
        self._gotoField(2,'MaNGA',expect,stages,'')


class TestBossCalibs(SopCmdTester,unittest.TestCase):
    def _bossCalibs(self, expect, stages, args,):
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doBossCalibs %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_BOSS_CALIBS)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.darkTime,expect.get('darkTime',900))
        self.assertEqual(msg.cmdState.flatTime,expect.get('FlatTime',30))
        self.assertEqual(msg.cmdState.arcTime,expect.get('arcTime',4))
        self.assertEqual(msg.cmdState.guiderFlatTime,expect.get('guiderFlatTime',0.5))
        self.assertEqual(msg.cmdState.nBias,expect.get('nBias',0))
        self.assertEqual(msg.cmdState.nDark,expect.get('nDark',0))
        self.assertEqual(msg.cmdState.nFlat,expect.get('nFlat',0))
        self.assertEqual(msg.cmdState.nArc,expect.get('nArc',0))
    
    def test_bossCalibs_bias(self):
        stages = ['bias','cleanup']
        expect = {'nBias':2}
        self._bossCalibs(expect,stages,'nbias=2')
    def test_bossCalibs_dark(self):
        stages = ['dark','cleanup']
        expect = {'nDark':2}
        self._bossCalibs(expect,stages,'ndark=2')
    def test_bossCalibs_flat(self):
        stages = ['flat','cleanup']
        expect = {'nFlat':2}
        self._bossCalibs(expect,stages,'nflat=2')
    def test_bossCalibs_arc(self):
        stages = ['arc','cleanup']
        expect = {'nArc':2}
        self._bossCalibs(expect,stages,'narc=2')
    def test_bossCalibs_all(self):
        stages = ['bias','dark','flat','arc','cleanup']
        expect = {'nBias':1,'nDark':1,'nFlat':1,'nArc':1}
        self._bossCalibs(expect,stages,'nbias=1 ndark=1 nflat=1 narc=1')


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


class TestDoApogeeDomeFlat(SopCmdTester,unittest.TestCase):
    def test_doApogeeDomeFlat(self):
        stages = ['domeFlat']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doApogeeDomeFlat',queue)
        self.assertEqual(msg.type,sopActor.Msg.APOGEE_DOME_FLAT)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)


class TestIsSlewingDisabled(SopCmdTester,unittest.TestCase):
    def _slewing_is_disabled(self,expect):
        self.cmdState.reinitialize(cmd=self.cmd)
        result = self.sopCmd.isSlewingDisabled(self.cmd)
        self.assertIn(expect,result)
    def test_slewing_disabled_apogee_science(self):
        self._update_cart(2, 'APOGEE')
        self.cmdState = self.actorState.doApogeeScience
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
        self._update_cart(2, 'APOGEE-2&MaNGA')
        sopTester.updateModel('boss',TestHelper.bossState['integrating'])
        self.cmdState = self.actorState.doApogeeMangaDither
        self._slewing_is_disabled('slewing disallowed for APOGEE&MaNGA,')
    def test_slewing_disabled_apogeemanga_sequence(self):
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
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestClassifyCartridge)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestHartmann)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoField)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestBossCalibs)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestUpdateCartridge)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestStatus)
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestIsSlewingDisabled)
    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
