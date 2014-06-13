"""
Test the various commands in SOP apogeeThread
"""

import unittest

from actorcore import TestHelper
import sopTester

import sopActor
import sopActor.myGlobals as myGlobals
import sopActor.Commands.SopCmd as SopCmd

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
    
    # These are kinda dumb, since all the "stuff" happens in the APOGEE ICC
    # but they're a useful and simple demonstration of how to test command calls.
    # Plus, they were useful for debugging the actual fake Cmd.call system.
    def _doDither(self,dither):
        """Test moving the dither position."""
        sopTester.updateModel('apogee',TestHelper.apogeeState['unknown'])
        cmdVar = apogeeThread.doDither(self.cmd,myGlobals.actorState,dither)
        self._check_levels(1,0,0,0)
        self.assertEquals(dither,myGlobals.actorState.models['apogee'].keyVarDict['ditherPosition'][1])        
        self.assertFalse(cmdVar.didFail)
    def test_doDither_A(self):
        self._doDither('A')
    def test_doDither_B(self):
        self._doDither('B')

    def _doShutter(self,position):
        """Test commanding the shutter open/closed."""
        sopTester.updateModel('apogee',TestHelper.apogeeState['unknown'])
        cmdVar = apogeeThread.doShutter(self.cmd,myGlobals.actorState,position)
        self._check_levels(1,0,0,0)
        self.assertFalse(cmdVar.didFail)
    def test_doShutter_open(self):
        self._doShutter('open')
        self.assertTrue(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][0])
        self.assertFalse(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][1])
    def test_doShutter_close(self):
        self._doShutter('close')
        self.assertFalse(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][0])
        self.assertTrue(myGlobals.actorState.models['apogee'].keyVarDict['shutterLimitSwitch'][1])
    

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
