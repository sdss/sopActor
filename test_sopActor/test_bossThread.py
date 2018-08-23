"""
Test the various commands in SOP bossThread
"""

import unittest

import sopActor
import sopActor.myGlobals as myGlobals
import sopTester
from sopActor import Queue, bossThread, masterThread


class TestBossThread(sopTester.SopThreadTester, unittest.TestCase):
    """Test calls to boss thread."""

    def setUp(self):
        self.useThreads = [('master', sopActor.MASTER, masterThread.main)]
        self.verbose = True
        super(TestBossThread, self).setUp()
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues['boss'] = Queue('boss')
        self.queues = myGlobals.actorState.queues

    def _single_hartmann(self,
                         nCall,
                         nInfo,
                         nWarn,
                         nErr,
                         expTime,
                         mask,
                         finish=False,
                         didFail=False):
        # These commands are issued via masterThread
        replyQueue = self.queues['boss']
        bossThread.single_hartmann(self.cmd, myGlobals.actorState, replyQueue, expTime, mask)
        self._check_cmd(
            nCall,
            nInfo,
            nWarn,
            nErr,
            finish,
            didFail,
            reply=['boss', sopActor.Msg.EXPOSURE_FINISHED])

    def test_single_hartmann_left(self):
        self._single_hartmann(1, 1, 0, 0, 4, 'left')

    def test_single_hartmann_right(self):
        self._single_hartmann(1, 1, 0, 0, 2, 'right')

    def test_single_hartmann_out(self):
        self._single_hartmann(1, 1, 0, 0, 3, 'out')

    def test_single_hartmann_bad(self):
        self._single_hartmann(0, 0, 0, 1, 3, 'bad')

    def _hartmann(self, nCall, nInfo, nWarn, nErr, args='', didFail=False):
        replyQueue = self.queues['boss']
        bossThread.hartmann(self.cmd, myGlobals.actorState, replyQueue, args)
        self._check_cmd(
            nCall,
            nInfo,
            nWarn,
            nErr,
            False,
            didFail,
            reply=['boss', sopActor.Msg.EXPOSURE_FINISHED])

    def test_hartmann_default(self):
        self._hartmann(1, 1, 0, 0)

    def test_hartmann_afternoon(self):
        self._hartmann(1, 1, 0, 0, args='ignoreResiduals noSubFrame')

    def test_hartmann_fails(self):
        self.cmd.failOn = 'hartmann collimate'
        self._hartmann(1, 1, 0, 1, didFail=True)


if __name__ == '__main__':
    verbosity = 2

    unittest.main(verbosity=verbosity)
