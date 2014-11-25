"""
Test the various commands in SOP tccThread
"""

import unittest

from actorcore import TestHelper
import sopTester

import sopActor
from sopActor import Queue
import sopActor.myGlobals as myGlobals

from sopActor import tccThread

class TccThreadTester(sopTester.SopThreadTester):
    """subclass this, then unittest.TestCase, in that order."""
    def setUp(self):
        self.useThreads = []#[("master", sopActor.MASTER,  masterThread.main)]
        self.verbose = True
        super(TccThreadTester,self).setUp()
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues['tcc'] = Queue('tcc')
        self.queues = myGlobals.actorState.queues


class TestAxisChecks(sopTester.SopTester, unittest.TestCase):
    """Test tccThread stop button and axis state functions"""
    def setUp(self):
        self.verbose = True
        super(TestAxisChecks,self).setUp()

    def test_axes_are_clear(self):
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        self.assertTrue(tccThread.axes_are_clear(self.actorState))
    def test_axes_are_clear_False(self):
        sopTester.updateModel('tcc',TestHelper.tccState['bad'])
        self.assertFalse(tccThread.axes_are_clear(self.actorState))

    def test_check_stop_in(self):
        sopTester.updateModel('tcc',TestHelper.tccState['stopped'])
        self.assertTrue(tccThread.check_stop_in(self.actorState))
    def test_check_stop_in_False(self):
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        self.assertFalse(tccThread.check_stop_in(self.actorState))

    def test_below_alt_limit_no(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted'])
        self.assertFalse(tccThread.below_alt_limit(self.actorState))
    def test_below_alt_limit_yes(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted_low'])
        self.assertTrue(tccThread.below_alt_limit(self.actorState))


class TestMcpSemaphoreOk(TccThreadTester, unittest.TestCase):
    def test_mcp_semaphore_ok(self):
        result = tccThread.mcp_semaphore_ok(self.cmd, self.actorState)
        self.assertEqual(result, 'None')
    def test_mcp_semaphore_ok_False(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['bad_semaphore'])
        result = tccThread.mcp_semaphore_ok(self.cmd, self.actorState)
        self.assertFalse(result)
        self._check_cmd(0,0,0,3,False)
    def test_mcp_semaphore_ok_no_semaphore(self):
        # empty list is the state if the keyword has not yet been issued.
        self.actorState.models['mcp'].keyVarDict['semaphoreOwner'].valueList = []
        result = tccThread.mcp_semaphore_ok(self.cmd, self.actorState)
        self.assertEqual(result, 'None')
        self._check_cmd(1,0,0,0,False)
    def test_mcp_semaphore_fails_no_semaphore(self):
        self.cmd.failOn = 'mcp sem.show'
        # empty list is the state if the keyword has not yet been issued.
        self.actorState.models['mcp'].keyVarDict['semaphoreOwner'].valueList = []
        result = tccThread.mcp_semaphore_ok(self.cmd, self.actorState)
        self.assertFalse(result)
        self._check_cmd(1,0,0,1,False)


class TestAxisInit(TccThreadTester, unittest.TestCase):
    """Test tccThread.axis_init()"""
    def _test_axis_init(self):
        tccThread.axis_init(self.cmd, self.actorState, self.queues['tcc'])
        msg = self.queues['tcc'].get()
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        return msg

    def test_axis_init_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        msg = self._test_axis_init()
        self.assertTrue(msg.success)
        self._check_cmd(2,1,0,0,False)
    def test_axis_init_no_init_needed(self):
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        sopTester.updateModel('mcp',TestHelper.mcpState['tcc_semaphore'])
        msg = self._test_axis_init()
        self.assertTrue(msg.success)
        self._check_cmd(1,1,0,0,False)
    def test_axis_init_alt_low(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted_low'])
        msg = self._test_axis_init()
        self.assertTrue(msg.success)
        self._check_cmd(2,1,1,0,False)


    def test_axis_init_no_axis_status(self):
        self.cmd.failOn = 'tcc axis status'
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        msg = self._test_axis_init()
        self.assertFalse(msg.success)
        self._check_cmd(1,0,0,2,False)
    def test_axis_init_stop_in(self):
        sopTester.updateModel('tcc',TestHelper.tccState['stopped'])
        msg = self._test_axis_init()
        self.assertFalse(msg.success)
        self._check_cmd(2,0,0,1,False)
    def test_axis_init_bad_semaphore(self):
        sopTester.updateModel('mcp',TestHelper.mcpState['bad_semaphore'])
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        msg = self._test_axis_init()
        self.assertFalse(msg.success)
        self._check_cmd(1,0,0,3,False)
    def test_axis_init_fails(self):
        self.cmd.failOn = 'tcc axis init'
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        msg = self._test_axis_init()
        self.assertFalse(msg.success)
        self._check_cmd(2,1,0,2,False)


class TestAxisStop(TccThreadTester, unittest.TestCase):
    def _axis_stop(self):
        tccThread.axis_stop(self.cmd, self.actorState, self.queues['tcc'])
        msg = self.queues['tcc'].get()
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        return msg

    def test_axis_stop(self):
        msg = self._axis_stop()
        self.assertTrue(msg.success)
        self._check_cmd(1,0,0,0, False)

    def test_axis_stop_fails(self):
        self.cmd.failOn = 'tcc axis stop'
        msg = self._axis_stop()
        self.assertFalse(msg.success)
        self._check_cmd(1,0,0,1, False, True)



if __name__ == '__main__':
    verbosity = 2
    
    unittest.main(verbosity=verbosity)
