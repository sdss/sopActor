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
from sopActor.Commands import SopCmd
from sopActor import Queue

from actorcore import TestHelper
import sopTester

class SopCmdTester(sopTester.SopTester):
    def setUp(self):
        self.verbose = True
        self.actor = TestHelper.FakeActor('sop','sopActor')
        super(SopCmdTester,self).setUp()
        self.timeout = 1
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues = {}
        myGlobals.actorState.queues[sopActor.MASTER] = Queue('master')
        self.sopCmd = SopCmd.SopCmd(self.actor)
        self.cmd.verbose = False # don't spam initial loadCart messages
        self.sopCmd.initCommands()
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
    
    def _update_cart(self,nCart):
        """Update cartridge without being verbose, and clear those messages."""
        self.cmd.verbose = False
        self.sopCmd.updateCartridge(nCart)
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose


class TestClassifyCartridge(SopCmdTester,unittest.TestCase):
    def _classifyCartridge(self,nCart,survey):
        surveyGot = self.sopCmd.classifyCartridge(self.cmd,nCart)
        self.assertEqual(surveyGot,survey)
    
    def test_classifyCartridge_bad(self):
        self._classifyCartridge(-1,sopActor.UNKNOWN)
    def test_classifyCartridge_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        self._classifyCartridge(11,sopActor.BOSS)
    def test_classifyCartridge_apogee(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        self._classifyCartridge(1,sopActor.APOGEE)
    def test_classifyCartridge_manga(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaLoaded'])
        self._classifyCartridge(2,sopActor.MANGA)
    def test_classifyCartridge_apogeemanga(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeemangaLoaded'])
        self._classifyCartridge(3,sopActor.APOGEEMANGA)
    
    def test_classifyCartridge_brightbypass(self):
        self._prep_bypass('brightPlate',clear=True)
        self._classifyCartridge(2,sopActor.BOSS)
    def test_classifyCartridge_darkbypass(self):
        self._prep_bypass('darkPlate',clear=True)
        self.cmd.clear_msgs()
        self._classifyCartridge(11,sopActor.APOGEE)

class TestUpdateCartridge(SopCmdTester,unittest.TestCase):
    """Confirm that we get the right validCommands from each survey type."""
    def _updateCartridge(self,nCart,expected):
        surveyGot = self.sopCmd.updateCartridge(nCart)
        import pdb
        pdb.set_trace()
        sop = myGlobals.actorState.models['sop']
        self.assertEqual(sop.keyVarDict['surveyCommands'].getValue(), expected['surveyCommands'])

    def test_updateCartridge_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopBossCommands['surveyCommands']
        self._updateCartridge(11,expected)

    def test_updateCartridge_manga(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopMangaCommands['surveyCommands']
        self._updateCartridge(2,expected)

    def test_updateCartridge_apogge(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expected = {}
        expected['surveyCommands'] = TestHelper.sopApogeeCommands['surveyCommands']
        self._updateCartridge(1,expected)


class TestGotoGangChange(SopCmdTester,unittest.TestCase):
    def _gotoGangChange(self, nCart, args, expect):
        self._update_cart(nCart)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('gotoGangChange %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_GANG_CHANGE)
        self.assertEqual(msg.cmdState.alt,expect.get('alt',45))
        stages = dict(zip(expect['stages'],['idle']*len(expect['stages'])))
        self.assertEqual(msg.cmdState.stages,stages)
    
    def test_gotoGangChange_ok(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat', 'slew'],'alt':15}
        self._gotoGangChange(1,'alt=15',expect)
    @unittest.skip('This will fail until I actually write the abort/stop logic.')
    def test_gotoGangChange_stop(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {}
        self._gotoGangChange(1,'stop',expect)
    @unittest.skip('This will fail until I actually write the abort/stop logic.')
    def test_gotoGangChange_abort(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        expect = {'stages':['domeFlat', 'slew'],
                  'abort':True}
        self._gotoGangChange(1,'abort',expect)
    def test_gotoGangChange_boss(self):
        sopTester.updateModel('guider',TestHelper.guiderState['bossLoaded'])
        expect = {'stages':['domeFlat','slew']}
        self._gotoGangChange(11,'',expect)


class TestDoMangaDither(SopCmdTester,unittest.TestCase):
    def _DoMangaDither(self, expect, args=''):
        stages = ['expose', 'dither']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaDither %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_MANGA_DITHER)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.dither,expect['dither'])
        self.assertEqual(msg.cmdState.expTime,expect['expTime'])

    def test_DoMangaDither_default(self):
        expect = {'expTime':900,
                  'dither':'C',
                  }
        self._DoMangaDither(expect)
    def test_DoMangaDither_N(self):
        expect = {'expTime':900,
                  'dither':'N',
                  }
        args = 'dither=N'
        self._DoMangaDither(expect,args)
    def test_DoMangaDither_expTime(self):
        expect = {'expTime':1000,
                  'dither':'C',
                  }
        args = 'expTime=1000'
        self._DoMangaDither(expect,args)
    @unittest.skip("Can't abort yet!")
    def test_DoMangaDither_abort(self):
        self._DoMangaDither(expect,'abort')
    @unittest.skip("Can't abort yet!")
    def test_DoMangaDither_stop(self):
        self._DoMangaDither(expect,'stop')


class TestDoMangaSequence(SopCmdTester,unittest.TestCase):
    def _doMangaSequence(self,expect,args):
        stages = ['expose', 'calibs', 'dither']
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('doMangaSequence %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.DO_MANGA_SEQUENCE)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self.assertEqual(msg.cmdState.ditherSeq,expect['ditherSeq'])
        self.assertEqual(msg.cmdState.arcTime,expect['arcTime'])
    
    def test_doMangaSequence_default(self):
        expect = {'expTime':900,
                  'ditherSeq':'NSE'*3,
                  'arcTime':4}
        self._doMangaSequence(expect,'')
    def test_doMangaSequence_one_set(self):
        expect = {'expTime':900,
                  'ditherSeq':'NSE',
                  'arcTime':4}
        self._doMangaSequence(expect,'count=1')


class TestGotoField(SopCmdTester,unittest.TestCase):
    def _gotoField(self, expect, stages, args):
        self._update_cart(self)
        queue = myGlobals.actorState.queues[sopActor.MASTER]
        msg = self._run_cmd('gotoField %s'%(args),queue)
        self.assertEqual(msg.type,sopActor.Msg.GOTO_FIELD)
        stages = dict(zip(stages,['idle']*len(stages)))
        self.assertEqual(msg.cmdState.stages,stages)
        self._check_levels(0,2,0,0) # should always output *Stages and *States
        self.assertEqual(msg.cmdState.arcTime,expect.get('arcTime',0))
        self.assertEqual(msg.cmdState.flatTime,expect.get('flatTime',0))
        self.assertEqual(msg.cmdState.guiderTime,expect.get('guiderTime',0))
        self.assertEqual(msg.cmdState.guiderFlatTime,expect.get('guiderFlatTime',0))
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
        self._gotoField(expect,stages,'')
    def test_gotoField_boss_noSlew(self):
        stages = ['hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'doSlew':False}
        self._gotoField(expect,stages,'noSlew')
    def test_gotoField_boss_noHartmann(self):
        stages = ['slew','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20,
                  'doHartmann':False}
        self._gotoField(expect,stages,'noHartmann')
    def test_gotoField_boss_noCalibs(self):
        stages = ['slew','hartmann','guider','cleanup']
        expect = {'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':10,'dec':20,
                  'doCalibs':False, 'doArc':False, 'doFlat':False}
        self._gotoField(expect,stages,'noCalibs')
    def test_gotoField_boss_noGuider(self):
        stages = ['slew','hartmann','calibs','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'ra':10,'dec':20,
                  'doGuider':False, 'doGuiderFlat':False}
        self._gotoField(expect,stages,'noGuider')
    
    def test_gotoField_apogee_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['apogeeLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['apogee'])
        stages = ['slew','guider','cleanup']
        expect = {'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':20,'dec':30,
                  'doHartmann':False,'doCalibs':False}
        self._gotoField(expect,stages,'')

    def test_gotoField_manga_default(self):
        sopTester.updateModel('guider',TestHelper.guiderState['mangaLoaded'])
        sopTester.updateModel('platedb',TestHelper.platedbState['manga'])
        stages = ['slew','hartmann','calibs','guider','cleanup']
        expect = {'arcTime':4,'flatTime':30,
                  'guiderTime':5,'guiderFlatTime':0.5,
                  'ra':30,'dec':40}
        self._gotoField(expect,stages,'')


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
    

if __name__ == '__main__':
    verbosity = 2
    
    suite = None
    # to test just one piece
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoGangChange)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestDoMangaDither)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestDoMangaSequence)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestClassifyCartridge)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestHartmann)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestGotoField)
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestBossCalibs)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUpdateCartridge)
    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
