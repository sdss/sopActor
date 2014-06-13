"""
Test the various commands in SOP lampThread
"""

import unittest
import time

import sopTester

import sopActor
from sopActor import Bypass
from sopActor import Queue
import sopActor.myGlobals as myGlobals

from sopActor import lampThreads
from sopActor import masterThread


class TestLampThread(sopTester.SopThreadTester,unittest.TestCase):
    """Test calls to boss thread."""
    def setUp(self):
        self.useThreads = []#("master", sopActor.MASTER,  masterThread.main)]
        self.verbose = True
        super(TestLampThread,self).setUp()
        # Do this after super setUp, as that's what creates actorState.
        myGlobals.actorState.queues['lamp'] = Queue('lamp')
        myGlobals.actorState.queues['reply'] = Queue('reply')
        queues = myGlobals.actorState.queues
        self.replyQueue = queues['reply']
        self.lampQueue = queues['lamp']

    def lamp_helper(self, nCall, nInfo, nWarn, nErr, queue, reply, didFail):
        msg = self._queue_get(queue)
        self.assertEqual(msg.type, reply)
        self._check_cmd(nCall, nInfo, nWarn, nErr, False, didFail)
        with self.assertRaises(queue.Empty): # ensure the queue is empty.
            queue.get(block=False)
        return msg

    def _do_lamp(self, nCall, nInfo, nWarn, nErr, name, action, didFail=False,
                 reply=None,noWait=False):
        if reply is None:
            reply = sopActor.Msg.LAMP_COMPLETE
        lampHandler = lampThreads.LampHandler(myGlobals.actorState, self.lampQueue, name)
        lampHandler.do_lamp(self.cmd, action, self.replyQueue, noWait)
        msg = self.lamp_helper(nCall, nInfo, nWarn, nErr, self.replyQueue, reply, didFail)
        self.assertEqual(msg.success, not didFail)
    
    def test_ff_on(self):
        self._do_lamp(1,0,0,0,'ff','on')
    def test_hgcd_on(self):
        self._do_lamp(1,0,0,0,'hgcd','on')
    def test_ne_on(self):
        self._do_lamp(1,0,0,0,'ne','on')
    def test_ff_off(self):
        self._do_lamp(1,0,0,0,'ff','off')

    def test_ne_on_delay(self):
        delay=20
        name = 'ne'
        action = 'on'
        reply = sopActor.Msg.WAIT_UNTIL
        lampHandler = lampThreads.LampHandler(myGlobals.actorState, self.lampQueue, name)
        lampHandler.do_lamp(self.cmd, action, self.replyQueue, delay=delay)
        msg = self.lamp_helper(1, 1, 0, 0, self.lampQueue, reply, False)

    def test_ff_on_noWait_succeeded(self):
        self._do_lamp(1,0,1,0,'ff','on',noWait=True)
    def test_ff_on_noWait_failed(self):
        self.cmd.failOn = "mcp ff.on"
        self._do_lamp(1,0,1,0,'ff','on',noWait=True)

    def test_ff_on_fail(self):
        self.cmd.failOn = "mcp ff.on"
        self._do_lamp(1,0,0,1,'ff','on',didFail=True)
    def test_ff_on_fail_bypassed(self):
        self.cmd.failOn = "mcp ff.on"
        Bypass.set('ff_lamp')
        self._do_lamp(1,0,1,1,'ff','on',didFail=False)
        Bypass.set('ff_lamp',bypassed=False)

    def test_uv_on_ignored(self):
        reply = sopActor.Msg.REPLY
        self._do_lamp(0,0,0,0,'uv','on',reply=reply)
    def test_wht_on_ignored(self):
        reply = sopActor.Msg.REPLY
        self._do_lamp(0,0,0,0,'wht','on',reply=reply)


    def _wait_until(self, nCall, nInfo, nWarn, nErr, name, endTime,
                    didFail=False, reply=None, replyQueue=None):
        if reply is None:
            reply = sopActor.Msg.WAIT_UNTIL
        if replyQueue is None:
            replyQueue = myGlobals.actorState.queues['lamp']
        lampHandler = lampThreads.LampHandler(myGlobals.actorState, self.lampQueue, name)
        lampHandler.wait_until(self.cmd, endTime, replyQueue)
        self.lamp_helper(nCall, nInfo, nWarn, nErr, replyQueue, reply, didFail)

    def test_wait_until_10(self):
        endTime = time.time() + 10.5 # to account for int() rounding down.
        self._wait_until(0,1,0,0,'ne',endTime)
    def test_wait_until_1(self):
        endTime = time.time() + 1
        self._wait_until(0,0,0,0,'ne',endTime)

    def test_wait_until_done(self):
        endTime = time.time() - 1
        reply = sopActor.Msg.LAMP_COMPLETE
        self._wait_until(0,0,0,0,'ne',endTime,reply=reply,replyQueue=self.lampQueue)

    def test_wait_until_aborting(self):
        endTime = time.time() + 10.5
        myGlobals.actorState.aborting = True
        reply = sopActor.Msg.LAMP_COMPLETE
        self._wait_until(0,0,1,0,'ne',endTime,reply=reply,replyQueue=self.lampQueue,didFail=True)


if __name__ == '__main__':
    verbosity = 2
    
    unittest.main(verbosity=verbosity)
