import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def doDither(cmd, actorState, dither):
    timeLim = 30.0  # seconds
    if True:
        cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd,
                                            cmdStr=("dither namedpos=%s" % dither),
                                            keyVars=[], timeLim=timeLim)
    else:
        cmd.warn('text="Not setting dither to %s"' % dither)

        class Foo(object):
            @property
            def didFail(self): return False
        cmdVar = Foo()

    return cmdVar
    
def main(actor, queues):
    """Main loop for APOGEE ICC thread"""

    threadName = "apogee"
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.APOGEE].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                return

            elif msg.type == Msg.DITHER:
                ret = doDither(msg.cmd, msg.dither)
                
            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond("text=\"starting %s%s exposure\"" % (
                        ((("%gs " % msg.expTime) if msg.expTime > 0 else ""), msg.expType)))

                try:
                    dither = msg.dither
                except AttributeError, e:
                    dither = None

                if dither != None:
                    cmdVar = doDither(msg.cmd, actorState, dither)
                    if cmdVar.didFail:
                        msg.cmd.warn('text="Failed to set dither position to %s"' % (dither))
                        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
                        continue
                                      
                # msg.cmd.warn('text="Not issuing a %gs exposure"' % (msg.expTime))
                timeLim = msg.expTime + 30.0  # seconds
                if True:                # really take data
                    cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=msg.cmd,
                                                        cmdStr="expose time=%0.1f object=OBJECT" % (msg.expTime),
                                                        keyVars=[], timeLim=timeLim)
                else:
                    msg.cmd.warn('text="Not taking a %gs exposure"' % (msg.expTime))
                    
                    class Foo(object):
                        @property
                        def didFail(self): return False
                    cmdVar = Foo()
                    
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
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
