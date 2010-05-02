import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from sopActor.utils.tcc import TCCState

print "Loading TCC thread"

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

            elif msg.type == Msg.SLEW:
                cmd = msg.cmd

                startSlew = False
                try:
                    msg.waitForSlewEnd
                except AttributeError, e:
                    startSlew = True
                    
                if not startSlew:
                    if not tccState.slewing:
                        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=not tccState.halted)
                        continue
                    
                    import time; time.sleep(1)
                    queues[sopActor.TCC].put(Msg.SLEW, cmd=msg.cmd, waitForSlewEnd=True)

                    continue

                try:
                    cmd.inform('text="slewing to (%.04f, %.04f, %g)"' % (msg.ra, msg.dec, msg.rot))
                    
                    cmdVar = msg.actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                            cmdStr="track %f, %f icrs /rottype=object/rotang=%g" % \
                                                            (msg.ra, msg.dec, msg.rot))
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
