import Queue as _Queue
import re, threading

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#
# Survey names; use classes so that the unique IDs are automatically generated
#
try:
    APOGEE
except NameError:
    class APOGEE(): pass
    class BOSS(): pass
    class MARVELS(): pass
    class UNKNOWN(): pass
    
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#
# Queue names; use classes so that the unique IDs are automatically generated
#
try:
    MASTER
except NameError:
    class MASTER(): pass
    class FFS(): pass                       # Flat Field Screen
    class FF_LAMP(): pass                   # FF lamps
    class HGCD_LAMP(): pass                 # HgCd lamps
    class HARTMANN(): pass                  # Do a Hartmann sequence
    class NE_LAMP(): pass                   # Ne lamps
    class UV_LAMP(): pass                   # uv lamps
    class WHT_LAMP(): pass                  # WHT lamps
    class BOSS(): pass                      # command the Boss ICC
    class GCAMERA(): pass                   # command the gcamera ICC
    class GUIDER(): pass                    # command the guider
    class TCC(): pass                       # command the TCC

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

try:
    Msg
except NameError:
    class Msg(object):
        # Priorities
        CRITICAL = 0
        HIGH = 2
        MEDIUM = 4
        NORMAL = 6

        # Command types; use classes so that the unique IDs are automatically generated
        class DO_CALIBS(): pass
        class DITHERED_FLAT(): pass
        class HARTMANN(): pass
        class DO_SCIENCE(): pass
        class DONE(): pass
        class EXIT(): pass
        class ENABLE(): pass
        class FFS_MOVE(): pass
        class FFS_COMPLETE(): pass
        class GOTO_FIELD(): pass
        class START(): pass
        class LAMP_ON(): pass
        class LAMP_COMPLETE(): pass
        class STATUS(): pass
        class EXPOSE(): pass
        class EXPOSURE_FINISHED(): pass
        class REPLY(): pass
        class SLEW(): pass
        class WAIT_UNTIL(): pass

        def __init__(self, type, cmd, **data):
            self.type = type
            self.cmd = cmd
            self.priority = Msg.NORMAL      # may be overridden by **data
            #
            # convert data[] into attributes
            #
            for k, v in data.items():
                self.__setattr__(k, v)
            self.__data = data.keys()

        def __repr__(self):
            values = []
            for k in self.__data:
                values.append("%s : %s" % (k, self.__getattribute__(k)))

            return "%s, %s: {%s}" % (self.type.__name__, self.cmd, ", ".join(values))

        def __cmp__(self, rhs):
            """Used when sorting the messages in a priority queue"""
            return self.priority - rhs.priority

class Queue(_Queue.PriorityQueue):
    """A queue type that checks that the message is of the desired type"""

    Empty = _Queue.Empty

    def __init__(self, name, *args):
        _Queue.Queue.__init__(self, *args)
        self.name = name        

    def __str__(self):
        return self.name

    def put(self, arg0, *args, **kwds):
        """Put  messaage onto the queue, calling the superclass's put method
Expects a Msg, otherwise tries to construct a Msg from its arguments"""
        
        if isinstance(arg0, Msg):
            msg = arg0
        else:
            msg = Msg(arg0, *args, **kwds)

        msg.senderName = threading.current_thread().name
        msg.senderName0 =  re.sub(r"(-\d+)?$", "", msg.senderName)
        msg.senderQueue = self

        _Queue.Queue.put(self, msg)

    def flush(self):
        """flush the queue"""
    
        while True:
            try:
                msg = self.get(timeout=0)
            except Queue.Empty:
                return

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Bypass(object):
    """
    Provide bypasses for subsystems

    A failure code from a bypassed subsystem will not cause a MultiCmd to fail
    """
    _bypassed = {}

    @staticmethod
    def set(name, bypassed=True, define=False):
        if define:
            Bypass._bypassed[name] = None

        if Bypass._bypassed.has_key(name):
            Bypass._bypassed[name] = bypassed
        else:
            return None

        return bypassed

    @staticmethod
    def get(cmd=None, name=None):
        if name:
            bypassed = Bypass._bypassed.get(name, False)
            if bypassed:
                if cmd:
                    cmd.warn('text="System %s failed but is bypassed"' % name)

            return bypassed

        return  Bypass._bypassed.items()

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Precondition(object):
    """A class to capture a precondition for a MultiCmd;  we require that it be satisfied
before the non-Precondition actions are begun
"""

    def __init__(self, queue, msgId=None, timeout=None, **kwargs):
        self.queue = queue
        self.msgId = msgId
        self.timeout = timeout
        self.kwargs = kwargs

    def required(self):
        """Is this precondition needed?"""
        return True

class MultiCommand(object):
    """Process a set of commands, waiting for the last to complete"""
    
    def __init__(self, cmd, timeout, *args, **kwargs):
        self.cmd = cmd
        self._replyQueue = Queue("(replyQueue)", 0)
        self.timeout = timeout
        self.commands = []
        self.status = True
        
        if args:
            self.append(*args, **kwargs)

    def append(self, queueName, msgId=None, timeout=None, isPrecondition=False, **kwargs):
        if isinstance(queueName, Precondition):
            assert msgId is None
            
            pre = queueName
            if not pre.required():
                return

            return self.append(pre.queueName, pre.msgId, pre.timeout, isPrecondition=True, **pre.kwargs)

        assert msgId is not None

        if timeout is not None and timeout > self.timeout:
            self.timeout = timeout
            
        try:
            self.commands.append((myGlobals.actorState.queues[queueName], isPrecondition,
                                  Msg(msgId, cmd=self.cmd, replyQueue=self._replyQueue, **kwargs)))
        except Exception, e:
            print "RHL", e

    def run(self):
        """Actually submit that set of commands and wait for them to reply. Return status"""
        self.start()

        return self.finish()

    def start(self):
        """Actually submit that set of commands"""

        nPre = 0
        for queue, isPrecondition, msg in self.commands:
            if isPrecondition:
                nPre += 1
                queue.put(msg)

        if nPre:
            if not self.finish(runningPreconditions=True):
                self.commands = []
                self.status = False

        if myGlobals.actorState.aborting: # don't schedule those commands
            if not myGlobals.actorState.ignoreAborting: # override for e.g. status command
                self.commands = []
                self.status = False

        for queue, isPrecondition, msg in self.commands:
            if not isPrecondition:
                queue.put(msg)

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

                if not msg.success and not Bypass.get(self.cmd, msg.senderName0):
                    failed = True
            except Queue.Empty:
                responsive = [re.sub(r"(-\d+)?$", "", k) for k in seen.keys() if seen[k]]
                cmdNames = [str(cmd[0]) for cmd in self.commands if cmd[1] == runningPreconditions]
                nonResponsive = [cmd for cmd in cmdNames if cmd not in responsive]

                self.cmd.warn('text="%d tasks failed to respond: %s"' % (
                    len(nonResponsive), " ".join(nonResponsive)))
                return False

        return not failed and self.status

__all__ = ["MASTER", "Msg", "Precondition", "Bypass"]
