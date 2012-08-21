import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from sopActor.utils.tcc import TCCState

print "Loading TCC thread"


def axis_init(msg,actorState):
    """Send 'tcc axis init', and return status."""
    cmd = msg.cmd

    # need to send an axis status first, just to make sure the status bits have cleared
    cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis status")
    # TODO: do I need to check whether tcc axis status fails?
    
    # You can check [az,alt,rot]Stat[3] for the exact status:
    # http://www.apo.nmsu.edu/Telescopes/HardwareControllers/AxisCommands.html
    if (actorState.models['tcc'].keyVarDict['azStat'][3] & 0x2000) | \
       (actorState.models['tcc'].keyVarDict['altStat'][3] & 0x2000) | \
       (actorState.models['tcc'].keyVarDict['rotStat'][3] & 0x2000):
        cmd.fail('text="Cannot tcc axis init because of bad axis status: Check stop buttons on Interlocks panel."')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
        return

    cmd.inform('text="sending tcc axis init"')
    # the semaphore should be owned by the TCC or nobody
    sem = actorState.models['mcp'].keyVarDict['semaphoreOwner'][0]
    if (sem != 'TCC:0:0') and (sem != '') and (sem != 'None') and (sem != None):
        cmd.fail('text="Cannot axis init: Semaphore is owned by '+sem+'"')
        cmd.fail('text="If you are the owner (e.g., via MCP Menu), release it and try again."')
        cmd.fail('text="If you are not the owner, confirm that you can steal it from them: mcp sem.steal"')
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
        return

    cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis init")

    if cmdVar.didFail:
        cmd.fail('text="Aborting GotoField: failed tcc axis init."')
        cmd.fail('text="Aborting GotoField: check and clear interlocks?"')
        # TODO: Should we send another message describing why we failed, or is this enough?
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
    else:
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
#...

def main(actor, queues):
    """Main loop for TCC thread"""

    threadName = "tcc"
    actorState = sopActor.myGlobals.actorState
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
                if tccState.badStat and not Bypass.get(name='axes'):
                    cmd.warn('text="in slew with badStat=%s halted=%s slewing=%s"' % \
                                 (tccState.badStat, tccState.halted, tccState.slewing))
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
                    continue

                if not startSlew:
                    cmd.warn('text="in slew with halted=%s slewing=%s"' % (tccState.halted, tccState.slewing))
                    if not tccState.slewing:
                        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=not tccState.halted)
                        continue
                    
                    import time; time.sleep(1)
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
            errMsg = "Unexpected exception %s in sop %s thread" % (e, threadName)
            actor.bcast.warn('text="%s"' % errMsg)
            tback(errMsg, e)

            try:
                msg.replyQueue.put(Msg.EXIT, cmd=msg.cmd, success=False)
            except Exception, e:
                pass
