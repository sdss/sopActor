import Queue, threading
import math, numpy
import time

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def main(actor, queues):
    """Main loop for guider thread"""

    threadName = "guider"
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.GUIDER].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.ENABLE:
                msg.cmd.respond("text=\"%s guiding on %s\"" %
                                (("enabling" if msg.on else "disabling"), msg.what))

                timeLim = 10
                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr=("%s %s" % (msg.what, "on" if msg.on else "off")),
                                                    keyVars=[], timeLim=timeLim)
                    
                msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.START:
                msg.cmd.respond("text=\"%s guider\"" % (("starting" if msg.on else "stopping")))

                if msg.expTime > 0:
                    expTimeOpt = ("time=%g" % msg.expTime)
                else:
                    expTimeOpt = ""

                forceOpt = "force" if (hasattr(msg, 'force') and msg.force) else ""
                oneExposureOpt = "oneExposure" if (hasattr(msg, 'oneExposure') and msg.oneExposure) else ""
                clearCorrections = "clearCorrections" if (hasattr(msg, 'clearCorrections')
                                                          and msg.clearCorrections) else ""

                if clearCorrections:
                    for corr in ("axes", "scale", "focus"):
                        cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                            cmdStr=("%s off" % (corr)),
                                                            keyVars=[], timeLim=3)
                        if cmdVar.didFail:
                            cmd.cmd.warn('text="failed to disable %s guider corrections!!!"' (corr))
                            msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)
                            continue
                    
                timeLim = msg.expTime   # seconds

                # If we are starting a "permanent" guide loop, we can't wait for the command to finish.
                # But wait long enough to see whether it blows up on the pad. Ugh - CPL.
                timeLim += 15
                cmdStr = "%s %s %s %s" % (("on" if msg.on else "off"),
                                          expTimeOpt, forceOpt, oneExposureOpt)
                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr=cmdStr,
                                                    keyVars=[], timeLim=timeLim)
                if msg.on and not oneExposureOpt:
                    # Waiting for word on whether there's a proper way to distinguish between
                    # 'failed' and 'timed out' -- CPL
                
                    if "Timeout" in cmdVar.lastReply.keywords:
                        # command timed out -- assume the loop is running OK.
                        msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=True)
                    else:
                        cmd.warn('text="probably failed to start guide exposure loop: %s"' % (cmdStr))
                        msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=False)
                    continue
                else:
                    if cmdVar.didFail:
                        cmd.warn('text="guider command failed: %s"' % (cmdStr))
                           
                msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond('text="starting guider flat"')

                if msg.expTime > 0:
                    expTimeOpt = ("time=%g" % msg.expTime)
                else:
                    expTimeOpt = ""

                #timeLim = msg.expTime   # seconds
                timeLim = msg.expTime    
                timeLim += 15

                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr="flat %s" % (expTimeOpt), 
                                                    keyVars=[], timeLim=timeLim)
                msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)

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
