import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def main(actor, queues):
    """Main loop for gcamera ICC thread"""

    threadName = "gcamera"
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.GCAMERA].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform('text="Exiting thread %s"' % (threading.current_thread().name))

                return

            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond('text="starting exposure"')

                timeLim = msg.expTime + 180.0  # seconds
                cmdVar = actorState.actor.cmdr.call(actor="gcamera", forUserCmd=msg.cmd,
                                                    cmdStr=("%s time=%g cartridge=%d" %
                                                            (msg.expType, msg.expTime, msg.cartridge)),
                                                    keyVars=[], timeLim=timeLim)

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
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
