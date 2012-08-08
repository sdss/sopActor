import Queue as _Queue
import re, time, threading

from opscore.utility.qstr import qstr

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
    class APOGEE_SCRIPT(): pass
    class SCRIPT(): pass

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
        class DO_APOGEE_EXPOSURES(): pass
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
        class AXIS_INIT(): pass
        class WAIT_UNTIL(): pass
        class DITHER(): pass
        class GOTO_GANG_CHANGE(): pass
        class POST_FLAT(): pass
        class APOGEE_SHUTTER(): pass            # control the internal APOGEE shutter 
        class APOGEE_PARK_DARKS(): pass 
        class APOGEE_SKY_FLATS(): pass 
        class NEW_SCRIPT(): pass
        class STOP_SCRIPT(): pass
        class SCRIPT_STEP(): pass

        def __init__(self, type, cmd, **data):
            self.type = type
            self.cmd = cmd
            self.priority = Msg.NORMAL

            self.duration = 0           # how long this command is expected to take (may be overridden by data)
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
        """
        Put  messaage onto the queue, calling the superclass's put method
        Expects a Msg, otherwise tries to construct a Msg from its arguments
        """
        
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

    A failure code from a bypassed subsystem will not cause a MultiCommand to fail
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
    """
    A class to capture a precondition for a MultiCommand; we require
    that it be satisfied before the non-Precondition actions are begun
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
    
    def __init__(self, cmd, timeout, label, *args, **kwargs):
        self.cmd = cmd
        self._replyQueue = Queue("(replyQueue)", 0)
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
        Append msgId (or Precondition.msgId) to this MultiCommand, to be run
        under queue queueName (one of the classes under 'try: MASTER' above).
        """
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
            msg = Msg(msgId, cmd=self.cmd, replyQueue=self._replyQueue, **kwargs)
            self.setMsgDuration(queueName, msg)

            self.commands.append((myGlobals.actorState.queues[queueName], isPrecondition, msg))
            
        except Exception, e:
            print "RHL appending to MultiCommand queue:", e

    def run(self):
        """Actually submit that set of commands and wait for them to reply. Return status"""
        self.start()

        return self.finish()

    def start(self):
        """Actually submit that set of commands"""

        nPre = 0
        duration = 0                    # guess at duration
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

        if myGlobals.actorState.aborting: # don't schedule those commands
            if not myGlobals.actorState.ignoreAborting: # override for e.g. status command
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

                if not msg.success and not Bypass.get(self.cmd, msg.senderName0):
                    failed = True
            except Queue.Empty:
                responsive = [re.sub(r"(-\d+)?$", "", k) for k in seen.keys() if seen[k]]
                cmdNames = [str(cmd[0]) for cmd in self.commands if cmd[1] == runningPreconditions]
                nonResponsive = [cmd for cmd in cmdNames if cmd not in responsive]

                self.cmd.warn('text="%d tasks failed to respond: %s"' % (
                    len(nonResponsive), " ".join(nonResponsive)))
                failed = True
                break

        if self.label:
            if failed or not self.status:
                state = "failed"
            else:
                state = "done" if not runningPreconditions else "prepped"
            self.cmd.inform('stageState="%s","%s",0.0,0.0' % (self.label, state))
        return not failed and self.status

class CmdState(object):
    validStageStates = ('starting', 'prepping', 'running', 'done', 'failed', 'aborted')

    """A class that's intended to hold command state data"""

    def __init__(self, name, allStages, keywords={}, hiddenKeywords=()):
        self.name = name
        self.cmd = None
        self.cmdState = "idle"
        self.stateText="OK"
        self.keywords = dict(keywords)
        self.hiddenKeywords = hiddenKeywords
        for k, v in keywords.iteritems():
            setattr(self, k, v)
            
        self.setStages(allStages)

    def _getCmd(self, cmd=None):
        if cmd:
            return cmd
        if self.cmd:
            return self.cmd
        return myGlobals.actorState.actor.bcast
    
    def setStages(self, allStages):
        self.allStages = allStages
        self.stages = dict(zip(self.allStages, ["idle"] * len(self.allStages)))

    def setupCommand(self, name, cmd, activeStages):
        self.name = name
        self.cmd = cmd
        self.stateText="OK"
        self.activeStages = activeStages
        for s in self.allStages:
            self.stages[s] = "pending" if s in activeStages else "off"
        self.genCommandKeys()

    def setCommandState(self, state, genKeys=True, stateText=None):
        self.cmdState = state
        if stateText:
            self.stateText=stateText

        if genKeys:
            self.genKeys()

    def setStageState(self, name, stageState, genKeys=True):
        assert name in self.stages
        assert stageState in self.validStageStates, "state %s is unknown" % (stageState)
        self.stages[name] = stageState

        if genKeys:
            self.genCmdStateKeys()

    def abortStages(self):
        """ Mark all unstarted stages as aborted. """
        for s in self.allStages:
            if not self.stages[s] in ("pending", "done", "failed"):
                self.stages[s] = "aborted"
        self.genCmdStateKeys()

    def setActiveStages(self, stages, genKeys=True):
        raise NotImplementedError()
        for s in stages:
            assert s in self.allStages

    def genCmdStateKeys(self, cmd=None):
        cmd = self._getCmd(cmd)
        cmd.inform("%sState=%s,%s,%s" % (self.name, qstr(self.cmdState),
                                         qstr(self.stateText),
                                         ",".join([qstr(self.stages[sname]) \
                                                       for sname in self.allStages])))

    def genCommandKeys(self, cmd=None):
        """ Return a list of the keywords describing our command. """

        cmd = self._getCmd(cmd)
        cmd.inform("%sStages=%s" % (self.name,
                                    ",".join([qstr(sname) \
                                                  for sname in self.allStages])))
        self.genCmdStateKeys(cmd=cmd)

    def getUserKeys(self):
        return []
    
    def genStateKeys(self, cmd=None):
        cmd = self._getCmd(cmd)

        msg = []
        for keyName, default in self.keywords.iteritems():
            val = getattr(self, keyName)
            if type(default) == str:
                val = qstr(val)
                default = qstr(default)
            msg.append("%s_%s=%s,%s" % (self.name, keyName,
                                        val, default))
        if msg:
            cmd.inform("; ".join(msg))

        try:
            userKeys = self.getUserKeys()
        except:
            userKeys = []
            cmd.warn('text="failed to fetch all keywords for %s"' % (self.name))

        if userKeys:
            cmd.inform(";".join(userKeys))
        
    def genKeys(self, cmd=None, trimKeys=False):
        """ generate all our keywords. """

        self.genCommandKeys(cmd=cmd)
        if not trimKeys or trimKeys == self.name:
            self.genStateKeys(cmd=cmd)
        
__all__ = ["MASTER", "Msg", "Precondition", "Bypass", "CmdState"]
