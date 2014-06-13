"""
Test the various commands in SOP bossThread
"""

import unittest

from actorcore import TestHelper
import sopTester

import sopActor
from sopActor import Queue
import sopActor.myGlobals as myGlobals
import sopActor.CmdState as CmdState

from sopActor import bossThread
from sopActor import masterThread

class TestBossThread(sopTester.SopThreadTester,unittest.TestCase):
    """Test calls to boss thread."""
    def setUp(self):
        self.useThreads = [("master", sopActor.MASTER,  masterThread.main)]
        self.verbose = True
        super(TestBossThread,self).setUp()
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues['boss'] = Queue('boss')
        self.queues = myGlobals.actorState.queues

    
    def _hartmann(self,nCall,nInfo,nWarn,nErr,expTime,mask, finish=False,didFail=False):
        # These commands are issued via masterThread
        replyQueue = self.queues['boss']
        bossThread.hartmann(self.cmd, myGlobals.actorState, replyQueue, expTime, mask)
        self._check_cmd(nCall, nInfo, nWarn, nErr, finish, didFail)
    def test_hartmann_left(self):
        """exposure"""
        self._hartmann(1,1,0,0,4,'left')
    def test_hartmann_right(self):
        """exposure"""
        self._hartmann(1,1,0,0,2,'right')
    def test_hartmann_out(self):
        """exposure"""
        self._hartmann(1,1,0,0,3,'out')
    def test_hartmann_bad(self):
        """Nothing!"""
        self._hartmann(0,0,0,1,3,'bad')


if __name__ == '__main__':
    verbosity = 2
    
    unittest.main(verbosity=verbosity)
