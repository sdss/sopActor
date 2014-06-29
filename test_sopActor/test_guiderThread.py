"""
Test the functions in guiderThread.
"""
import unittest

from actorcore import TestHelper
import sopTester

import sopActor
import sopActor.myGlobals as myGlobals
import sopActor.CmdState as CmdState

from sopActor import guiderThread

# False for less printing, True for more printing
verbose = True
#verbose = False


class GuiderThreadTester(sopTester.SopThreadTester,unittest.TestCase):
    def setUp(self):
        self.verbose = verbose
        self.useThreads = [("gcamera", sopActor.GCAMERA,  sopTester.FakeThread)]
        super(GuiderThreadTester,self).setUp()
        self.fail_on_no_cmd_calls = True # we need cmd_calls for all of these.
        # Do this after super setUp, as that's what creates actorState.
        self.actorState.queues['reply'] = sopActor.Queue('reply')


class TestGuiderStart(GuiderThreadTester):
    """Tests for starting the guider."""
    def _guider_start(self, nCall,nInfo,nWarn,nErr, args, finish=False, didFail=False):
        start, expTime, clearCorrections, force, oneExposure = args
        replyQueue = self.actorState.queues['reply']
        guiderThread.guider_start(self.cmd, replyQueue, self.actorState, start, expTime, clearCorrections, force, oneExposure)
        msg = self._queue_get(replyQueue)
        self.assertEqual(msg.type,sopActor.Msg.DONE)
        self._check_cmd(nCall,nInfo,nWarn,nErr,finish,didFail)

    def test_guider_start(self):
        args = (True, 5, True, '', '')
        self._guider_start(4,0,0,0,args)
    def test_guider_start_fails(self):
        args = (True, 5, True, '', '')
        self.cmd.failOn = 'guider on time=5'
        self._guider_start(4,0,0,1,args)
    def test_guider_start_fails_axes(self):
        args = (True, 5, True, '', '')
        self.cmd.failOn = 'guider axes off'
        self._guider_start(1,0,0,1,args)

if __name__ == '__main__':
    verbosity = 1
    if verbose:
        verbosity = 2

    suite = None

    if suite:
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
    else:
        unittest.main(verbosity=verbosity)
