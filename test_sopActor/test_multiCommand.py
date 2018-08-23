"""
Test the multiCommand system.
"""
import threading
import unittest

import sopTester
from sopActor import Msg, Queue, myGlobals
from sopActor.multiCommand import MultiCommand, Precondition


class PreconditionUnneeded(Precondition):

    def required(self):
        return False


class TestMultiCommand(sopTester.SopTester, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        """Load up the cmd calls for this test class."""

        unittest.TestCase.__init__(self, *args, **kwargs)

        class_name = self.id().split('.')[-2]
        self._load_cmd_calls(class_name)

    def setUp(self):
        self.verbose = True
        self.tid = sopTester.TEST_QUEUE
        self.queueName = 'testMultiCmd'
        self.msgs = [Msg.LAMP_ON, Msg.FFS_MOVE, Msg.STATUS, Msg.SLEW]
        super(TestMultiCommand, self).setUp()
        self.timeout = 2
        self.multiCmd = MultiCommand(self.cmd, self.timeout, 'testMultiCmd.stage')

        self.queue = Queue(self.queueName, 0)
        self.queues = {self.tid: self.queue}
        actorState = myGlobals.actorState
        actorState.queues = self.queues
        actorState.threads[self.tid] = threading.Thread(
            target=sopTester.FakeThread,
            name=self.queueName,
            args=[actorState.actor, actorState.queues])
        actorState.threads[self.tid].daemon = True
        actorState.threads[self.tid].start()

    def _prep_multiCmd_nopre(self):
        """Prep self.multiCmd with pre-arranged messages"""
        self.multiCmd.append(self.tid, self.msgs[0])
        self.multiCmd.append(self.tid, self.msgs[1])

    def _prep_multiCmd_pre(self):
        """Prep self.multiCmd with pre-arranged messages"""
        self.multiCmd.append(Precondition(self.tid, self.msgs[0]))
        self.multiCmd.append(Precondition(self.tid, self.msgs[1]))
        self.multiCmd.append(self.tid, self.msgs[2])
        self.multiCmd.append(self.tid, self.msgs[3])

    def test_append_not_precondition(self):
        self.multiCmd.append(self.tid, Msg.DONE, timeout=10, blah=1)
        msg = self.multiCmd.commands[0]
        self.assertEqual(msg[0], self.queue)
        self.assertFalse(msg[1])
        self.assertEqual(msg[2].type, Msg.DONE)
        self.assertEqual(msg[2].type, Msg.DONE)
        self.assertEqual(msg[2].cmd, self.cmd)
        self.assertEqual(msg[2].blah, 1)
        self.assertEqual(self.multiCmd.timeout, 10)

    def test_append_precondition(self):
        self.multiCmd.append(Precondition(self.tid, Msg.DONE, timeout=1, blah=1))
        msg = self.multiCmd.commands[0]
        self.assertEqual(msg[0], self.queue)
        self.assertTrue(msg[1])
        self.assertEqual(msg[2].type, Msg.DONE)
        self.assertEqual(msg[2].type, Msg.DONE)
        self.assertEqual(msg[2].cmd, self.cmd)
        self.assertEqual(self.multiCmd.timeout, self.timeout)

    def test_append_precondition_unneeded(self):
        self.multiCmd.append(PreconditionUnneeded(self.tid, Msg.DONE, timeout=10, blah=1))
        self.assertEqual(self.multiCmd.commands, [])

    def test_append_precondition_with_msgId(self):
        with self.assertRaises(AssertionError):
            self.multiCmd.append(Precondition(self.tid), Msg.DONE)

    def test_append_no_msgId(self):
        with self.assertRaises(AssertionError):
            self.multiCmd.append(self.tid)

    def test_run_nopre(self):
        self._prep_multiCmd_nopre()
        result = self.multiCmd.run()
        self.assertTrue(result)
        self._check_cmd(2, 3, 0, 0, False, didFail=not result)

    def test_run_pre(self):
        self._prep_multiCmd_pre()
        result = self.multiCmd.run()
        self.assertTrue(result)
        self._check_cmd(4, 6, 0, 0, False, didFail=not result)

    def test_run_nopre_fails(self):
        self.cmd.failOn = 'testMultiCmd sopActor.LAMP_ON'
        self._prep_multiCmd_nopre()
        result = self.multiCmd.run()
        self.assertFalse(result)
        self._check_cmd(2, 3, 0, 0, False, didFail=not result)

    def test_run_timesout(self):
        self.multiCmd.append(self.tid, Msg.EXIT)
        self._prep_multiCmd_nopre()
        result = self.multiCmd.run()
        self.assertFalse(result)
        self._check_cmd(0, 3, 1, 0, False, didFail=not result)


if __name__ == '__main__':
    verbosity = 2

    unittest.main(verbosity=verbosity)
