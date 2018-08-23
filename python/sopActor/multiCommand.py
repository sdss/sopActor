"""
System for handling multiple commands in sequence or in parallel.
"""

import re
import time

from sopActor import Msg, Queue, myGlobals


class Precondition(object):
    """
    A class to capture a precondition for a MultiCommand; we require
    that it be satisfied before the non-Precondition actions are begun
    """

    def __init__(self, queueName, msgId=None, timeout=None, **kwargs):
        self.queueName = queueName
        self.msgId = msgId
        self.timeout = timeout
        self.kwargs = kwargs

    def required(self):
        """Is this precondition needed?"""
        return True


class MultiCommand(object):
    """Process a set of commands, waiting for the last to complete"""

    def __init__(self, cmd, timeout, label, *args, **kwargs):
        self.cmd = cmd
        self._replyQueue = Queue('(replyQueue)', 0)
        self.timeout = timeout
        self.label = label
        self.commands = []
        self.status = True

        if args:
            self.append(*args, **kwargs)

    def setMsgDuration(self, queueName, msg):
        """Set msg's expected duration in seconds"""
        pass

    def append(self, queueName, msgId=None, timeout=None, isPrecondition=False, **kwargs):
        """
        Append msgId or Precondition.msgId (one of the classes under try: Msg in
        __init__) to this MultiCommand, to be run under queue queueName (one of
        the classes under 'try: MASTER' in __init__).
        """
        if isinstance(queueName, Precondition):
            assert msgId is None

            pre = queueName
            if not pre.required():
                return

            return self.append(
                pre.queueName, pre.msgId, pre.timeout, isPrecondition=True, **pre.kwargs)

        assert msgId is not None

        if timeout is not None and timeout > self.timeout:
            self.timeout = timeout

        msg = Msg(msgId, cmd=self.cmd, replyQueue=self._replyQueue, **kwargs)
        self.setMsgDuration(queueName, msg)
        self.commands.append((myGlobals.actorState.queues[queueName], isPrecondition, msg))

    def run(self):
        """Actually submit that set of commands and wait for them to reply. Return status"""
        self.start()

        return self.finish()

    def start(self):
        """Actually submit that set of commands"""

        nPre = 0
        duration = 0  # guess at duration
        for queue, isPrecondition, msg in self.commands:
            if isPrecondition:
                nPre += 1
                if msg.duration > duration:
                    duration = msg.duration

                queue.put(msg)

        if nPre:
            self.cmd.inform('text="%s expectedDuration=%d expectedEnd=%d"' %
                            (self.label, duration, time.time() + duration))
            if self.label:
                self.cmd.inform('stageState="%s","prepping",0.0,0.0' % (self.label))

            if not self.finish(runningPreconditions=True):
                self.commands = []
                self.status = False

        if myGlobals.actorState.aborting:  # don't schedule those commands
            if not myGlobals.actorState.ignoreAborting:  # override for e.g. status command
                self.commands = []
                self.status = False

        duration = 0
        for queue, isPrecondition, msg in self.commands:
            if not isPrecondition:
                if msg.duration > duration:
                    duration = msg.duration

                queue.put(msg)

        if self.label:
            self.cmd.inform('stageState="%s","running",%0.1f,0.0' % (self.label, duration))
        self.cmd.inform('text="expectedDuration=%d"' % duration)

    def finish(self, runningPreconditions=False):
        """Wait for set of commands to reply. Return status"""

        seen = {}
        for tname in myGlobals.actorState.threads.values():
            seen[tname.name] = False

        failed = False
        for queue, isPrecondition, msg in self.commands:
            if runningPreconditions != isPrecondition:
                continue

            try:
                msg = self._replyQueue.get(timeout=self.timeout)
                seen[msg.senderName] = True

                if not msg.success and not myGlobals.bypass.get(msg.senderName0, cmd=self.cmd):
                    failed = True
            except Queue.Empty:
                responsive = [re.sub(r"(-\d+)?$", '', k) for k in seen.keys() if seen[k]]
                cmdNames = [str(cmd[0]) for cmd in self.commands if cmd[1] == runningPreconditions]
                nonResponsive = [cmd for cmd in cmdNames if cmd not in responsive]

                self.cmd.warn('text="%d tasks failed to respond: %s"' % (len(nonResponsive),
                                                                         ' '.join(nonResponsive)))
                failed = True
                break

        if self.label:
            if failed or not self.status:
                state = 'failed'
            else:
                state = 'done' if not runningPreconditions else 'prepped'
            self.cmd.inform('stageState="%s","%s",0.0,0.0' % (self.label, state))
        return not failed and self.status
