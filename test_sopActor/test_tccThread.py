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

class TestSlew(TccThreadTester, unittest.TestCase):
    def setUp(self):
        super(TestSlew,self).setUp()
        self.slewHandler = tccThread.SlewHandler(self.actorState, self.actorState.tccState, self.queues['tcc'])
        self.update_tccState()

    def update_tccState(self):
        """
        Have to call the appropriate functions after we updateModel:
            no callbacks in a fakeModel."""
        tccState = self.actorState.tccState
        model = self.actorState.models['tcc']
        # TBD: can't use moveItems, since these keyVars don't have reply.
        # So, Robert's Ughh. comment at the top of that implies Craig lied... (at least for my tests...)
        # tccState.listenToMoveItems(model.keyVarDict["moveItems"])
        tccState.listenToBadStatusMask(model.keyVarDict["axisBadStatusMask"])
        tccState.listenToAxisStat(model.keyVarDict["altStat"])
        tccState.listenToAxisStat(model.keyVarDict["azStat"])
        tccState.listenToAxisStat(model.keyVarDict["rotStat"])
        tccState.listenToTccStatus(model.keyVarDict["tccStatus"])

    def test_parse_args_radecrot(self):
        ra, dec, rot = 10, 20, 30
        msg = sopActor.Msg(sopActor.Msg.SLEW, self.cmd, ra=ra, dec=dec, rot=rot, keepOffsets=True)
        self.slewHandler.parse_args(msg)
        self.assertEqual(self.slewHandler.ra, ra)
        self.assertEqual(self.slewHandler.dec, dec)
        self.assertEqual(self.slewHandler.rot, rot)
        self.assertTrue(self.slewHandler.keepOffsets)
    def test_parse_args_altazrot(self):
        alt, az, rot = 100, 200, 300
        msg = sopActor.Msg(sopActor.Msg.SLEW, self.cmd, alt=alt, az=az, rot=rot)
        self.slewHandler.parse_args(msg)
        self.assertEqual(self.slewHandler.alt, alt)
        self.assertEqual(self.slewHandler.az, az)
        self.assertEqual(self.slewHandler.rot, rot)
    def test_parse_args_keepOffsets(self):
        msg = sopActor.Msg(sopActor.Msg.SLEW, self.cmd, keepOffsets=True)
        self.slewHandler.parse_args(msg)
        self.assertTrue(self.slewHandler.keepOffsets)

    def _not_ok_to_slew(self, expect):
        self.update_tccState()
        result = self.slewHandler.not_ok_to_slew(self.cmd)
        self.assertEqual(result, expect)
    def test_not_ok_to_slew_halted_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted'])
        self._not_ok_to_slew(False)
    def test_not_ok_to_slew_halted_low_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted_low'])
        self._not_ok_to_slew(False)
    def test_not_ok_to_slew_stopped_not_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['stopped'])
        self._not_ok_to_slew(True)
    def test_not_ok_to_slew_bad_not_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['bad'])
        self._not_ok_to_slew(True)

    def test_not_ok_to_slew_ignore_az_ok(self):
        """Start at low alt, pretend to move up, continued bad alt should be ok."""
        sopTester.updateModel('tcc',TestHelper.tccState['halted_low'])
        self.update_tccState()
        self.slewHandler.not_ok_to_slew(self.cmd)
        sopTester.updateModel('tcc',TestHelper.tccState['badAz'])
        self._not_ok_to_slew(False)
        self._check_cmd(0,0,1,0,False)

    def _slew_radec(self, ra, dec, rot):
        self.slewHandler.ra = ra
        self.slewHandler.dec = dec
        self.slewHandler.rot = rot
        self.slewHandler.do_slew(self.cmd, self.queues['tcc'])
        msg = self.queues['tcc'].get(timeout=0.1)
        return msg
    def test_slew(self):
        msg = self._slew_radec(10,20,30)
        self.assertEqual(msg.type, sopActor.Msg.WAIT_FOR_SLEW_END)
        self._check_cmd(1,1,0,0,False)

    def test_slew_twice(self):
        msg = self._slew_radec(10,20,30)
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        self.update_tccState()
        self.assertEqual(msg.type, sopActor.Msg.WAIT_FOR_SLEW_END)
        msg = self._slew_radec(40,50,60)
        self.assertEqual(msg.type, sopActor.Msg.WAIT_FOR_SLEW_END)
        self._check_cmd(2,2,0,0,False)

    def _slew_altaz(self, alt, az, rot):
        self.slewHandler.alt = alt
        self.slewHandler.az = az
        self.slewHandler.rot = rot
        self.slewHandler.slew(self.cmd, self.queues['tcc'])
        return self.queues['tcc'].get(timeout=0.1)
        
    def test_slew_altaz(self):
        msg = self._slew_altaz(100,200,300)
        self.assertEqual(msg.type, sopActor.Msg.WAIT_FOR_SLEW_END)
        self._check_cmd(1,1,0,0,False)

    def test_slew_fails(self):
        self.cmd.failOn = "tcc track 10.000000, 20.000000 icrs /rottype=object/rotang=30/rotwrap=mid"
        msg = self._slew_radec(10,20,30)
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        self.assertFalse(msg.success)
        self._check_cmd(1,1,1,0,False,False)

    def test_slew_axis_stopped(self):
        sopTester.updateModel('tcc',TestHelper.tccState['stopped'])
        self.update_tccState()
        self.slewHandler.slew(self.cmd, self.queues['tcc'])
        msg = self.queues['tcc'].get(timeout=0.1)
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        self.assertFalse(msg.success)
        self._check_cmd(0,0,1,0,False,False)

    def test_wait_for_slew_end_waiting(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted'])
        # prep that we're in a slew, don't care about the return msg.
        self._slew_radec(10,20,30)
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        self.update_tccState()
        # TBD: have to fake the slewing state, because I'm not using a proper Model.
        tccState = self.actorState.tccState
        tccState.slewing = True
        tccState.halted = False
        self.slewHandler.wait_for_slew_end(self.cmd, self.queues['tcc'])
        msg = self.queues['tcc'].get(timeout=0.1)
        self.assertEqual(msg.type, sopActor.Msg.WAIT_FOR_SLEW_END)

    def test_wait_for_slew_end_waiting_from_low(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted_low'])
        # prep that we're in a slew, don't care about the return msg.
        self._slew_altaz(30,121,0)
        sopTester.updateModel('tcc',TestHelper.tccState['badAz'])
        self.update_tccState()
        self.slewHandler.wait_for_slew_end(self.cmd, self.queues['tcc'])
        msg = self.queues['tcc'].get(timeout=0.1)
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        self.assertTrue(msg.success)

    def test_wait_for_slew_end_done_ok(self):
        sopTester.updateModel('tcc',TestHelper.tccState['halted'])
        # prep that we're in a slew, don't care about the return msg.
        self._slew_radec(10,20,30)
        sopTester.updateModel('tcc',TestHelper.tccState['moving'])
        self.update_tccState()
        # TBD: have to fake the slewing state, because I'm not using a proper Model.
        tccState = self.actorState.tccState
        tccState.slewing = False
        tccState.halted = False
        self.slewHandler.wait_for_slew_end(self.cmd, self.queues['tcc'])
        msg = self.queues['tcc'].get(timeout=0.1)
        self.assertEqual(msg.type, sopActor.Msg.REPLY)
        self.assertTrue(msg.success)


if __name__ == '__main__':
    verbosity = 2
    
    suite = None
    # to test just one piece
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestSlew)

    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
