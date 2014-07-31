import Queue, threading
import math, numpy
import time

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def get_expTime(msg):
    """Return expTime (float) and expTimeOpt (string to pass to guider cmd)."""
    try:
        if msg.expTime > 0:
            expTimeOpt = ("time=%g" % msg.expTime)
            expTime = msg.expTime
        else:
            expTimeOpt = ""
            expTime = 0
    except AttributeError:
        expTimeOpt = ""
        expTime = 0
    return expTime,expTimeOpt

def time_text(expTime):
    """Return an appropriate time=value string to pass to the gcamera."""
    return '' if not expTime else "time=%g" % expTime

def guider_start(cmd, replyQueue, actorState, start, expTime, clearCorrections, force, oneExposure):
    """Start/stop the guider and put an appropriate message on replyQueue if it succeeded."""

    if clearCorrections:
        for corr in ("axes", "scale", "focus"):
            cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=cmd,
                                                cmdStr=("%s off" % (corr)),
                                                keyVars=[], timeLim=3)
            if cmdVar.didFail:
                cmd.error('text="failed to disable %s guider corrections!!!"' % (corr))
                replyQueue.put(Msg.DONE, cmd=cmd, success=not cmdVar.didFail)
                return

    # If we are starting a "permanent" guide loop, we can't wait for the command to finish.
    # So, wait long enough to see whether it blows up on the pad,
    # and watch the guideState value for success.
    timeLim = expTime + 15  # seconds

    cmdStr = "%s %s %s %s" % (("on" if start else "off"),
                              time_text(expTime), force, oneExposure)
    cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=cmd,
                                        cmdStr=cmdStr,
                                        keyVars=[], timeLim=timeLim)
    if start and not oneExposure:
        # The value of the guider.guideState keyword tells us if it started successfully:
        # it will go to "starting", then "on" if it succeeded,
        # or "stopping"/"failed" if something went wrong.
        guideState = actorState.models["guider"].keyVarDict['guideState']
        if guideState[0] == 'on':
            replyQueue.put(Msg.DONE, cmd=cmd, success=True)
        elif guideState[0] == 'failed' or guideState[0] == 'stopping':
            cmd.error('text="Failed to start guide exposure loop: %s"' % (cmdStr))
            replyQueue.put(Msg.DONE, cmd=cmd, success=False)
        elif "Timeout" in cmdVar.lastReply.keywords:
            # command timed out -- assume the loop is running OK.
            replyQueue.put(Msg.DONE, cmd=cmd, success=True)
        else:
            # Uncertain, but probably didn't work.
            cmd.warn('text="probably failed to start guide exposure loop: %s"' % (cmdStr))
            replyQueue.put(Msg.DONE, cmd=cmd, success=False)
        return
    else:
        if cmdVar.didFail:
            cmd.warn('text="guider command failed: %s"' % (cmdStr))
    replyQueue.put(Msg.DONE, cmd=cmd, success=not cmdVar.didFail)


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
                expTime,expTimeOpt = get_expTime(msg)
                force = "force" if (hasattr(msg, 'force') and msg.force) else ""
                oneExposure = "oneExposure" if (hasattr(msg, 'oneExposure') and msg.oneExposure) else ""
                clearCorrections = "clearCorrections" if (hasattr(msg, 'clearCorrections')
                                                          and msg.clearCorrections) else ""
                start = msg.on

                guider_start(msg.cmd, msg.replyQueue, actorState, start, expTime, clearCorrections, force, oneExposure)

            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond('text="starting guider flat"')
                expTime,expTimeOpt = get_expTime(msg)

                timeLim = expTime
                timeLim += 30

                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr="flat %s" % (expTimeOpt),
                                                    keyVars=[], timeLim=timeLim)
                msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)
                
            elif msg.type == Msg.DECENTER:
                state = ("on" if msg.on else "off")
                msg.cmd.respond('text="Turning decentered guiding %s."'%state)
                timeLim = 30 # could take as long as a long guider exposure.
                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr="decenter %s"%(state),
                                                    keyVars=[], timeLim=timeLim)
                msg.replyQueue.put(Msg.DONE, cmd=msg.cmd, success=not cmdVar.didFail)
            
            elif msg.type == Msg.MANGA_DITHER:
                msg.cmd.respond('text="Changing guider dither position."')
                timeLim = 30 # could take as long as a long guider exposure.
                ditherPos = "ditherPos=%s"%msg.dither
                cmdVar = actorState.actor.cmdr.call(actor="guider", forUserCmd=msg.cmd,
                                                    cmdStr="mangaDither %s" % (ditherPos),
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
            sopActor.handle_bad_exception(actor,e,threadName,msg)
