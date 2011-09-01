import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def main(actor, queues):
    """Main loop for boss ICC thread"""

    threadName = "boss"
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.BOSS].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.EXPOSE:
                if msg.readout and msg.expTime <= 0:
                    msg.cmd.respond("text=\"starting exposure readout\"")
                else:
                    msg.cmd.respond("text=\"starting %s%s exposure\"" % (
                        ((("%gs " % msg.expTime) if msg.expTime > 0 else ""), msg.expType)))

                expTimeCmd = expTypeCmd = ""
                if msg.expTime >= 0:
                    if msg.expType != "bias":
                        expTimeCmd = ("itime=%g" % msg.expTime)
                    expTypeCmd = msg.expType
                    readoutCmd = "" if msg.readout else "noreadout"
                else:
                    readoutCmd = "readout"
                    if not msg.readout:
                        msg.cmd.warn('text="Saw msg.readout == False but msg.expTime == %g"' % msg.expTime)

                timeLim = msg.expTime + 180.0  # seconds
                timeLim += 100
                if True:                # really take data
                    cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=msg.cmd,
                                                        cmdStr=("exposure %s %s %s" %
                                                                (expTypeCmd, expTimeCmd, readoutCmd)),
                                                        keyVars=[], timeLim=timeLim)
                else:
                    msg.cmd.inform('text="Not taking a %gs exposure"' % msg.expTime)

                    class Foo(object):
                        @property
                        def didFail(self): return False
                    cmdVar = Foo()
                    
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.HARTMANN:
                msg.cmd.respond("text=\"starting Hartmann sequence\"")

                if True:
                    timeLim = 240
                    cmdVar = actorState.actor.cmdr.call(actor="sos", forUserCmd=msg.cmd,
                                                        cmdStr="doHartmann",
                                                        keyVars=[], timeLim=timeLim)
                else:
                    msg.cmd.warn('text="Faking Hartmann sequence"')
                    import time; time.sleep(4)

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
