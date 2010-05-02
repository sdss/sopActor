import Queue, threading, time
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def doLamp(cmd, queue, lampName, action):
    """Perform action for the named lamp (e.g. "FF", "on")"""
    
    actorState = sopActor.myGlobals.actorState

    timeLim = 10.0  # seconds
    cmdVar = actorState.actor.cmdr.call(actor="mcp", forUserCmd=cmd,
                                        cmdStr=("%s.%s" % (lampName.lower(), action)),
                                        timeLim=timeLim)
    if cmdVar.didFail:
        cmd.warn('text="Failed to turn %s lamps %s"' % (lampName, action))

    queue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=not cmdVar.didFail)

def lamp_main(actor, queue, lampName):
    """Main loop for lamps thread"""

    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout
    threadName = lampName

    while True:
        try:
            msg = queue.get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform('text="Exiting thread %s"' % (threading.current_thread().name))

                return
            elif msg.type == Msg.LAMP_ON:
                action = "on" if msg.on else "off"

                if lampName in ["uv", "wht",]:
                    msg.cmd.diag('text="ignoring %s.%s"' % (action, lampName))
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
                    continue

                timeLim = 10.0          # seconds
                cmdVar = actorState.actor.cmdr.call(actor="mcp", forUserCmd=msg.cmd, 
                                                    cmdStr=("%s.%s" % (lampName.lower(), action)),
                                                    timeLim=timeLim)
                if cmdVar.didFail:
                    msg.cmd.warn('text="Failed to turn %s lamps %s"' % (lampName, action))
                elif hasattr(msg, "delay"):
                    msg.cmd.inform('text="Waiting %gs for %s lamps to warm up"' % (msg.delay, lampName))
                    time.sleep(msg.delay)

                msg.replyQueue.put(Msg.LAMP_COMPLETE, cmd=msg.cmd, success=not cmdVar.didFail)

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

def ff_main(actor, queues):
    """Main loop for FF lamps thread"""

    lamp_main(actor, queues[sopActor.FF_LAMP], "FF")

def ne_main(actor, queues):
    """Main loop for Ne lamps thread"""

    lamp_main(actor, queues[sopActor.NE_LAMP], "Ne")

def hgcd_main(actor, queues):
    """Main loop for HgCd lamps thread"""

    lamp_main(actor, queues[sopActor.HGCD_LAMP], "HgCd")

def uv_main(actor, queues):
    """Main loop for UV lamps thread"""

    lamp_main(actor, queues[sopActor.UV_LAMP], "uv")

def wht_main(actor, queues):
    """Main loop for WHT lamps thread"""

    lamp_main(actor, queues[sopActor.WHT_LAMP], "wht")
