"""
Test the various commands in SOP apogeeThread
"""

import unittest

from actorcore import TestHelper
import sopTester

import sopActor
import sopActor.myGlobals as myGlobals

from sopActor import apogeeThread

# False for less printing, True for more printing
verbose = True
#verbose = False


class TestApogeeThread(sopTester.SopThreadTester,unittest.TestCase):
    """
    Tests for the various functions in sop apogeeThread.
    """
    def setUp(self):
        self.verbose = verbose
        self.useThreads = [("apogee",sopActor.APOGEE, apogeeThread.main),
                           ("apogeeScript",  sopActor.APOGEE_SCRIPT, apogeeThread.script_main),]
        super(TestApogeeThread,self).setUp()
    
    # These first two are kinda dumb, since all the "stuff" happens in the APOGEE ICC
    # but they're a useful and simple demonstration of how to test command calls.
    # Plus, they were useful for debugging the actual fake Cmd.call system.
    def _do_dither(self,dither):
        """Test moving the dither position."""
        sopTester.updateModel('apogee',TestHelper.apogeeState['unknown'])
        cmdVar = apogeeThread.do_dither(self.cmd,myGlobals.actorState,dither)
        self._check_cmd(1,0,0,0,finish=False)
        self.assertEquals(dither,myGlobals.actorState.models['apogee'].keyVarDict['ditherPosition'][1])
        self.assertFalse(cmdVar.didFail)
    def test_do_dither_A(self):
        self._do_dither('A')
    def test_do_dither_B(self):
        self._do_dither('B')


    def _do_shutter(self,position):
        """Test commanding the shutter open/closed."""
        sopTester.updateModel('apogee',TestHelper.apogeeState['unknown'])
        cmdVar = apogeeThread.do_shutter(self.cmd,myGlobals.actorState,position)
        self._check_cmd(1,0,0,0, finish=False)
        self.assertFalse(cmdVar.didFail)
    def test_do_shutter_open(self):
        self._do_shutter('open')
        self.assertTrue(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][0])
        self.assertFalse(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][1])
    def test_do_shutter_close(self):
        self._do_shutter('close')
        self.assertFalse(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][0])
        self.assertTrue(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][1])

    
    def _do_expose(self, nCall,nInfo,nWarn,nErr, expTime, dither, didFail=False):
        success = apogeeThread.do_expose(self.cmd,myGlobals.actorState,expTime,dither,'dark','')
        self.assertEqual(success, not didFail)
        self._check_cmd(nCall,nInfo,nWarn,nErr, finish=False)
    def test_expose(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_expose(2,1,0,0, 500, 'A')
    def test_expose_no_dither(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_expose(1,1,0,0, 500, None)

    def test_expose_dither_fails(self):
        self.cmd.failOn = 'apogee dither namedpos=A'
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_expose(1,0,0,1, 500, 'A', didFail=True)


    def _do_expose_dither_set(self, nCall,nInfo,nWarn,nErr, expTime, dithers, didFail=False):
        success = apogeeThread.do_expose_dither_set(self.cmd,myGlobals.actorState,expTime,dithers,'object','')
        self.assertEqual(success, not didFail)
        self._check_cmd(nCall,nInfo,nWarn,nErr, finish=False, didFail=didFail)
    def test_expose_dither_set(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_expose_dither_set(4,2,0,0, 500, 'AB')
    def test_expose_dither_set_no_first_dither(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self._do_expose_dither_set(3,3,0,0, 500, 'BA')

    def test_expose_dither_set_B_dither_fails(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self.cmd.failOn = 'apogee dither namedpos=B'
        self._do_expose_dither_set(3,1,0,1, 500, 'AB', didFail=True)
    def test_expose_dither_set_expose_fails(self):
        sopTester.updateModel('apogee',TestHelper.apogeeState['B_open'])
        self.cmd.failOn = 'apogee expose time=500.0 object=object'
        self._do_expose_dither_set(2,0,0,1, 500, 'AB', didFail=True)


# NOTE: commented out, as it doesn't actually test the thing I want it to test: 
# the failure of RO.AddCallback on ApogeeCB.listenToReads.
# class TestApogeeThreadExit(sopTester.ThreadExitTester,unittest.TestCase):
#     """Does apogeeThread.main exit cleanly?"""
#     def setUp(self):
#         self.verbose = verbose
#         self.useThreads = [("apogee",sopActor.APOGEE, apogeeThread.main),]
#         super(TestApogeeThreadExit,self).setUp()

# class TestScriptThreadExit(sopTester.ThreadExitTester,unittest.TestCase):
#     """Does apogeeThread.script_main exit cleanly?"""
#     def setUp(self):
#         self.verbose = verbose
#         self.useThreads = [("apogeeScript",sopActor.APOGEE_SCRIPT, apogeeThread.script_main),]
#         super(TestScriptThreadExit,self).setUp()
#

if __name__ == '__main__':
    verbosity = 1
    if verbose:
        verbosity = 2
    
    unittest.main(verbosity=verbosity)
