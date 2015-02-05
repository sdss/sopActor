import Queue, threading
import time

from sopActor import Msg
import sopActor
import sopActor.myGlobals as myGlobals

print "Loading TCC thread"

def check_stop_in(actorState, axes=('az','alt','rot')):
    """
    Return true if any stop bit is set in the <axis>Stat TCC keywords.
    The [az,alt,rot]Stat[3] bits show the exact status:
    http://www.apo.nmsu.edu/Telescopes/HardwareControllers/AxisCommands.html
    """
    try:
        tccModel = actorState.models['tcc']
        return any((tccModel.keyVarDict['%sStat'%axis][3] & 0x2000) for axis in axes)
    except TypeError:
        # some axisStat is unknown (and thus None)
        return False

def axes_are_ok(actorState, axes=('az','alt','rot')):
    """
    No bad bits set in any axis status field.
    Also return False if the badStatusMask or [axis]Stat is None.
    """
    try:
        tccModel = actorState.models['tcc']
        mask = tccModel.keyVarDict['axisBadStatusMask'][0]
        return not any((tccModel.keyVarDict['%sStat'%axis][3] & mask) for axis in axes)
    except TypeError:
        # axisStat or axisBadStatusMask is unknown (and thus None)
        return False

def axes_are_clear(actorState, axes=('az','alt','rot')):
    """No bits set in any axis status field."""
    try:
        tccModel = actorState.models['tcc']
        return all((tccModel.keyVarDict['%sStat'%axis][3] == 0) for axis in axes)
    except TypeError:
        # some axisStat is unknown (and thus None)
        return False

def axes_state(axisCmdState, state, axes=('az','alt','rot')):
    """Return True if all axes are in the given state."""
    ax = {'az':0, 'alt':1, 'rot':2}
    return all(state in axisCmdState[ax[x]].lower() for x in axes)

def some_axes_state(axisCmdState, state, axes=('az','alt','rot')):
    """Return True if any axis is in the given state."""
    ax = {'az':0, 'alt':1, 'rot':2}
    return any(state in axisCmdState[ax[x]].lower() for x in axes)

def below_alt_limit(actorState):
    """Check if we are below the alt=18 limit that prevents init/motion in az."""
    return actorState.models['tcc'].keyVarDict['axePos'][1] < 18

def mcp_semaphore_ok(cmd, actorState):
    """
    Return the semaphore if the semaphore is ok to take: owned by the TCC or nobody
    Return False, and issue error messages if not.
    """

    # If tron+sop have been restarted and mcp hasn't issued the keyword, there
    # will be no list to get a value from.
    try:
        sem = actorState.models['mcp'].keyVarDict['semaphoreOwner'][0]
    except IndexError:
        cmdVar = actorState.actor.cmdr.call(actor="mcp", forUserCmd=cmd, cmdStr="sem.show")
        if cmdVar.didFail:
            cmd.error('text="Error: Cannot get mcp semaphore. Is the mcp alive?"')
            return False
        sem = actorState.models['mcp'].keyVarDict['semaphoreOwner'][0]

    if (sem != 'TCC:0:0') and (sem != '') and (sem != 'None') and (sem != None):
        cmd.error('text="Cannot axis init: Semaphore is owned by '+sem+'"')
        cmd.error('text="If you are the owner (e.g., via MCP Menu), release it and try again."')
        cmd.error('text="If you are not the owner, confirm that you can steal it from them, then issue: mcp sem.steal"')
        return False

    return sem

def axis_init(cmd, actorState, replyQueue):
    """Send 'tcc axis init', and return status."""

    # need to send an axis status first, just to make sure the status bits have cleared
    cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis status")
    # "tcc axis status" should never fail!
    if cmdVar.didFail:
        cmd.error('text="Cannot check axis status. Something is very wrong!"')
        cmd.error('text="Is the TCC in a responsive state?"')
        replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
        return

    # TBD: Another (cleaner?) solution would be to check the mcp
    # "sdssdc.status.i6.il0.*_stop" bits directly. See mcp/src/axis_cmds.c:check_stop_in()
    # for a list of the different stop bits.

    # if a stop button is in, we can't slew at all.
    if check_stop_in(actorState):
        # wait a couple seconds, then try again: the stop bits behave like sticky bits,
        # and may require two "tcc axis status" queries to fully clear.
        time.sleep(2)
        cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis status")
        if check_stop_in(actorState):
            cmd.error('text="Cannot tcc axis init because of bad axis status: Check stop buttons on Interlocks panel."')
            replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
            return

    sem = mcp_semaphore_ok(cmd, actorState)
    if not sem:
        replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
        return

    if sem == 'TCC:0:0' and axes_are_clear(actorState):
        cmd.inform('text="Axes clear and TCC has semaphore. No axis init needed, so none sent."')
        replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
        return
    else:
        cmd.inform('text="Sending tcc axis init."')
        cmdStr = "axis init"
        if below_alt_limit(actorState):
            cmd.warn('text="Altitude below interlock limit! Only initializing altitude and rotator: cannot move in az."')
            cmdStr = ' '.join((cmdStr,"rot,alt"))
        cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr=cmdStr)
    
    if cmdVar.didFail:
        cmd.error('text="Cannot slew telescope: failed tcc axis init."')
        cmd.error('text="Cannot slew telescope: check and clear interlocks?"')
        replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
    else:
        replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
#...

def axis_stop(cmd, actorState, replyQueue):
    cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis stop")
    if cmdVar.didFail:
        cmd.error('text="Error: failed to cleanly stop telescope via tcc axis stop."')
        replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
    else:
        replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
    return


class SlewHandler(object):
    """
    Handle slew commands, which can include a variety of separate arguments.
    Example:
        slewHandler.parse_args(msg)
        slewHandler.do_slew(msg.cmd, msg.replyQueue)
    """
    def __init__(self, actorState, queue):
        self.actorState = actorState
        self.queue = queue
        self.reset()

    def reset(self):
        """Reset everything so we know the type of slew that is commanded."""
        self.alt, self.az = None, None
        self.ra, self.dec = None, None
        self.rot = None
        self.keepOffsets = False
        self.ignoreBadAz = False # for when we start below the alt limit.

    def parse_args(self, msg):
        """Extract the various potential arguments of a slew msg."""
        self.alt = getattr(msg, 'alt', None)
        self.az = getattr(msg, 'az', None)
        self.rot = getattr(msg, 'rot', None)
        self.ra = getattr(msg, 'ra', None)
        self.dec = getattr(msg, 'dec', None)
        self.keepOffsets = getattr(msg, 'keepOffsets', False)

    def slew(self, cmd, replyQueue):
        """Issue the commanded tcc track."""
        tccModel = self.actorState.models['tcc']
        # Just fail if there's something wrong with an axis.
        if self.not_ok_to_slew(cmd):
            axisCmdState = tccModel.keyVarDict['axisCmdState']
            cmd.warn('text="Trying to start slew with tcc.axisCmdState = %s"'%(','.join(axisCmdState)))
            replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
            return

        self.do_slew(cmd, replyQueue)

    def axes_ok_with_bypass(self, axes=('az','alt','rot')):
        """Ignore the state of az when determing axis status."""
        return axes_are_ok(self.actorState,axes) or myGlobals.bypass.get(name='axes')

    def not_ok_to_slew(self, cmd):
        """Return True if we should not command a slew."""

        if self.ignoreBadAz:
            # If we were coming up from low alt, az will still be bad.
            return not self.axes_ok_with_bypass(('alt','rot'))
        if below_alt_limit(self.actorState):
            cmd.warn("text='Below alt=18 limit! Ignoring errors in Az, since we cannot move it anyway.'")
            self.ignoreBadAz = True
            return not self.axes_ok_with_bypass(('alt','rot'))
        else:
            return not self.axes_ok_with_bypass()

    def wait_for_slew_end(self, cmd, replyQueue):
        """
        Check and report on the status of a currently running slew.
        Need to do this because the tcc may report a slew as complete before
        the axes settle or there may be a delay before a bad slew is fully reported.

        ! TBD: NOTE: we probably don't need this any more: the new tcc should
        ! only claim that tcc track finished *after* it has output the various keywords.
        ! So this method can probably disappear...
        """
        tccModel = self.actorState.models['tcc']
        axisCmdState = tccModel.keyVarDict['axisCmdState']
        # Only output as warn level if some axis halted.
        if some_axes_state(axisCmdState, 'halt'):
            lvl = cmd.warn
        else:
            lvl = cmd.inform
        lvl('text="Waiting for slew end with tcc.axisCmdState = %s"'%(','.join(axisCmdState)))
        # fail if some axis status went bad during/after the slew.
        if self.not_ok_to_slew(cmd):
            replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
            return

        if not axes_state(axisCmdState, 'slewing'):
            if self.ignoreBadAz:
                success = self.axes_ok_with_bypass(('alt','rot'))
            else:
                success = not axes_state(axisCmdState, 'halt')
            replyQueue.put(Msg.REPLY, cmd=cmd, success=success)
            return

        time.sleep(1)
        self.queue.put(Msg.WAIT_FOR_SLEW_END, cmd=cmd, replyQueue=replyQueue)

    def do_slew(self, cmd, replyQueue):
        """Correctly handle a slew command, given what parse_args had received."""
        call = self.actorState.actor.cmdr.call
        
        # NOTE: TBD: We should limit which offsets are kept.
        keepArgs = "/keep=(obj,arc,gcorr,calib,bore)" if self.keepOffsets else ""

        if self.ra is not None and self.dec is not None:
            cmd.inform('text="slewing to (%.04f, %.04f, %g)"' % (self.ra, self.dec, self.rot))
            if keepArgs:
                cmd.warn('text="keeping all offsets"')
            cmdVar = call(actor="tcc", forUserCmd=cmd,
                          cmdStr="track %f, %f icrs /rottype=object/rotang=%g/rotwrap=mid %s" % \
                          (self.ra, self.dec, self.rot, keepArgs))
        else:
            cmd.inform('text="slewing to (az, alt, rot) == (%.04f, %.04f, %0.4f)"' % (self.az, self.alt, self.rot))
            cmdVar = call(actor="tcc", forUserCmd=cmd,
                          cmdStr="track %f, %f mount/rottype=mount/rotangle=%f" % \
                          (self.az, self.alt, self.rot))
            
        if cmdVar.didFail:
            cmd.warn('text="Failed to start slew"')
            replyQueue.put(Msg.REPLY, cmd=cmd, success=False)
            return

        # Wait for slew to end
        self.queue.put(Msg.WAIT_FOR_SLEW_END, cmd=cmd, replyQueue=replyQueue)


def main(actor, queues):
    """Main loop for TCC thread"""

    threadName = "tcc"
    actorState = myGlobals.actorState
    timeout = actorState.timeout
    slewHandler = SlewHandler(actorState, queues[sopActor.TCC])

    while True:
        try:
            msg = queues[sopActor.TCC].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.AXIS_INIT:
                axis_init(msg.cmd, actorState, msg.replyQueue)

            elif msg.type == Msg.AXIS_STOP:
                axis_stop(msg.cmd, actorState, msg.replyQueue)

            elif msg.type == Msg.SLEW:
                slewHandler.reset()
                slewHandler.parse_args(msg)
                slewHandler.slew(msg.cmd, msg.replyQueue)
            elif msg.type == Msg.WAIT_FOR_SLEW_END:
                slewHandler.wait_for_slew_end(msg.cmd, msg.replyQueue)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                msg.cmd.warn("Unknown message type %s" % msg.type)
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor, e, threadName, msg)
