import Queue, threading
import time

from sopActor import *
import sopActor
import sopActor.myGlobals as myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from sopActor.utils.tcc import TCCState

print "Loading TCC thread"

def check_stop_in(actorState):
    """
    Return true if any stop bit is set in the <axis>Stat TCC keywords.
    The [az,alt,rot]Stat[3] bits show the exact status:
    http://www.apo.nmsu.edu/Telescopes/HardwareControllers/AxisCommands.html
    """
    return (actorState.models['tcc'].keyVarDict['azStat'][3] & 0x2000) | \
           (actorState.models['tcc'].keyVarDict['altStat'][3] & 0x2000) | \
           (actorState.models['tcc'].keyVarDict['rotStat'][3] & 0x2000)
#...

def axes_are_clear(actorState):
    return ((actorState.models['tcc'].keyVarDict['azStat'][3] == 0) and 
            (actorState.models['tcc'].keyVarDict['altStat'][3] == 0) and 
            (actorState.models['tcc'].keyVarDict['rotStat'][3] == 0))

def axis_init(msg, actorState):
    """Send 'tcc axis init', and return status."""
    cmd = msg.cmd

    # need to send an axis status first, just to make sure the status bits have cleared
    cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis status")
    # "tcc axis status" should never fail!
    if cmdVar.didFail:
        cmd.error('text="Cannot check axis status. Something is very wrong!"')
        cmd.error('text="Is the TCC in a responsive state?"')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
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
            msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
            return

    # the semaphore should be owned by the TCC or nobody
    sem = actorState.models['mcp'].keyVarDict['semaphoreOwner'][0]
    if (sem != 'TCC:0:0') and (sem != '') and (sem != 'None') and (sem != None):
        cmd.error('text="Cannot axis init: Semaphore is owned by '+sem+'"')
        cmd.error('text="If you are the owner (e.g., via MCP Menu), release it and try again."')
        cmd.error('text="If you are not the owner, confirm that you can steal it from them, then issue: mcp sem.steal"')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
        return

    if sem == 'TCC:0:0' and axes_are_clear(actorState):
        cmd.inform('text="Axes clear and TCC has semaphore. No axis init needed, so none sent."')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
        return
    else:
        cmd.inform('text="Sending tcc axis init."')
        cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis init")
    
    if cmdVar.didFail:
        cmd.error('text="Cannot slew telescope: failed tcc axis init."')
        cmd.error('text="Cannot slew telescope: check and clear interlocks?"')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
    else:
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
#...

def main(actor, queues):
    """Main loop for TCC thread"""

    threadName = "tcc"
    actorState = myGlobals.actorState
    tccState = actorState.tccState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.TCC].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.AXIS_INIT:
                axis_init(msg,actorState)

            elif msg.type == Msg.SLEW:
                cmd = msg.cmd

                startSlew = False
                try:
                    msg.waitForSlewEnd
                except AttributeError, e:
                    startSlew = True
                    
                # Do not _start_ slew if an axis is wedged.
                if tccState.badStat and not myGlobals.bypass.get(name='axes'):
                    cmd.warn('text="in slew with badStat=%s halted=%s slewing=%s"' % \
                                 (tccState.badStat, tccState.halted, tccState.slewing))
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
                    continue

                if not startSlew:
                    cmd.warn('text="in slew with halted=%s slewing=%s"' % (tccState.halted, tccState.slewing))
                    if not tccState.slewing:
                        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=not tccState.halted)
                        continue
                    
                    time.sleep(1)
                    queues[sopActor.TCC].put(Msg.SLEW, cmd=msg.cmd,
                                             replyQueue=msg.replyQueue, waitForSlewEnd=True)
                    continue

                # Yuck, yuck, yuck. At the very least we should limit which offsets are kept.
                try:
                    keepArgs = "/keep=(obj,arc,gcorr,calib,bore)" if msg.keepOffsets else ""
                except:
                    keepArgs = ""

                try:
                    cmd.inform('text="slewing to (%.04f, %.04f, %g)"' % (msg.ra, msg.dec, msg.rot))
                    if keepArgs:
                        cmd.warn('text="keeping all offsets"')
                    
                    cmdVar = msg.actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                            cmdStr="track %f, %f icrs /rottype=object/rotang=%g/rotwrap=mid %s" % \
                                                            (msg.ra, msg.dec, msg.rot, keepArgs))
                except AttributeError:
                    cmd.inform('text="slewing to (az, alt, rot) == (%.04f, %.04f, %0.4f)"' % (msg.az, msg.alt, msg.rot))
                    
                    cmdVar = msg.actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                            cmdStr="track %f, %f mount/rottype=mount/rotangle=%f" % \
                                                            (msg.az, msg.alt, msg.rot))
                    
                if cmdVar.didFail:
                    cmd.warn('text="Failed to start slew"')
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
                #
                # Wait for slew to end
                #
                queues[sopActor.TCC].put(Msg.SLEW, cmd=msg.cmd, replyQueue=msg.replyQueue, waitForSlewEnd=True)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                msg.cmd.warn("Unknown message type %s" % msg.type)
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor, e, threadName, msg)
