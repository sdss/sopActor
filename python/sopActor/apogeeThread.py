import Queue, threading
import math, numpy, time

import sopActor
from sopActor import *
import sopActor.myGlobals as myGlobals

from opscore.utility.qstr import qstr

from twisted.internet import reactor, defer
def twistedSleep(secs):
    d = defer.deferred()
    reactor.callLater(secs, d.callback, None)
    return d

def checkFailure(msg,cmdVar,failmsg,finish=True):
    """
    Test whether cmdVar has failed, and if so issue failmsg as a 'warn' level text.
    Returns True if cmdVar failed, False if not.
    Send a success=True REPLY, if finish is True and we cmdVar didn't fail.
    Always send success=False if it did fail.
    """
    if cmdVar.didFail:
        msg.cmd.warn('text=%s'%qstr(failmsg))
        msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
        return True
    else:
        if finish:
            msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
        return False
#...

def doDither(cmd, actorState, dither):
    """Move the APOGEE dither position."""
    timeLim = 30.0  # seconds
    cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd,
                                        cmdStr=("dither namedpos=%s" % dither),
                                        keyVars=[], timeLim=timeLim)
    return cmdVar
#...

def doShutter(cmd,actorState,position):
    """Move the APOGEE shutter position."""
    cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd,
                                        cmdStr="shutter %s" % (position),
                                        timeLim=20)
    return cmdVar
#...

class ApogeeCB(object):
    def __init__(self):
        self.cmd = myGlobals.actorState.actor.bcast
        self.reset()
        myGlobals.actorState.models['apogee'].keyVarDict["utrReadState"].addCallback(self.listenToReads, callNow=True)

    def shutdown(self):
        # Setting doRaise=False since we don't care if the callback function
        # has ceased to exist when the thread exits (which is the only time this is called).
        myGlobals.actorState.models['apogee'].keyVarDict["utrReadState"].removeCallback(self.listenToReads,doRaise=False)

    def listenToReads(self, key):
        try:
            state = key[1]
            n = key[2]
        except:
            state="gack"
            n=42

        if not self.cmd.isAlive():
            self.cmd = myGlobals.actorState.actor.bcast
        self.cmd.diag('text="utrReadState=%s,%s count=%s trigger=%s"' %
                      (state, n, self.count, self.triggerCount))

        if self.triggerCount < 0:
            return
        if str(state) != "Reading":
            return

        self.count += 1

        try:
            #if not self.cmd.isAlive():
            #    self.cmd = myGlobals.actorState.actor.bcast
            self.cmd.diag('text="utrReadState2=%s,%s count=%s trigger=%s"' %
                          (state, n, self.count, self.triggerCount))

            if self.count == self.triggerCount:
                self.reset()
                # time.sleep(1)
                self.cb()
        except Exception, e:
            self.cmd.warn('text="failed to call callback: %s"' % (e))
            tback("cb", e)

    def reset(self):
        self.count = 0
        self.triggerCount = -1

    def waitForNthRead(self, cmd, n, q, cb=None):
        self.reset()
        self.cmd = cmd
        self.triggerCount = n
        self.q = q

        self.cb = cb if cb else self.flashLamps

    def turnOffLamps(self):
        self.cmd.warn('text="calling ff_lamp.off"')
        replyQueue = sopActor.Queue("apogeeFlasher")
        myGlobals.actorState.queues[sopActor.FF_LAMP].put(Msg.LAMP_ON, cmd=self.cmd, on=False, replyQueue=replyQueue)

        # This is dreadful. Why is this .get() and test commented out? Some bad threading/reactor interaction. CPL
        #ret = replyQueue.get()
        #self.cmd.diag('text="ff_lamp.off ret: %s"' % (ret))
        self.cmd.diag('text="called ff_lamp.off"')

    def flashLamps(self):
        time2flash = 4.0       # seconds
        t0 = time.time()

        cmdr = myGlobals.actorState.actor.cmdr
        replyQueue = sopActor.Queue("apogeeFlasher")
        self.cmd.diag('text="calling ff_lamp.on"')
        # NOTE: calling with noWait, so we don't wait for the mcp to respond.
        # This prevents the problem where one lamp doesn't turn on within 4 seconds.
        myGlobals.actorState.queues[sopActor.FF_LAMP].put(Msg.LAMP_ON, cmd=self.cmd, on=True, replyQueue=replyQueue, noWait=True)

        # See angry comment in .turnOffLamps(). In this case I wonder if calling reactor.callLater _before_
        # the put(LAMP_ON) would help. Bad, bad, bad.
        #ret = replyQueue.get(True)
        #self.cmd.diag('text="ff_lamp.on ret: %s"' % (ret))

        self.cmd.warn('text="called ff_lamp.on"')
        t1 = time.time()
        if False: # cmdVar.didFail: # ret.success:
            self.cmd.warn('text="ff lamp on command failed"')
        else:
            self.cmd.diag('text="pausing..."')
            reactor.callLater(time2flash, self.turnOffLamps)

def main(actor, queues):
    """Main loop for APOGEE ICC thread"""

    threadName = "apogee"
    actorState = myGlobals.actorState
    timeout = actorState.timeout

    # Set up readout callback object:

    while True:
        try:
            msg = actorState.queues[sopActor.APOGEE].get(timeout=timeout)
            
            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                return

            elif msg.type == Msg.DITHER:
                cmdVar = doDither(msg.cmd, actorState, msg.dither)
                checkFailure(msg,cmdVar,"Failed to move APOGEE dither to %s position."%(dither))
                
            elif msg.type == Msg.APOGEE_SHUTTER:
                position = "open" if msg.open else "close"
                cmdVar = doShutter(msg.cmd, actorState, position)
                checkFailure(msg,cmdVar,"Failed to %s APOGEE internal shutter."%(position))

            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond("text=\"starting %s%s exposure\"" % (
                        ((("%gs " % msg.expTime) if msg.expTime > 0 else ""), msg.expType)))

                try:
                    dither = msg.dither
                except AttributeError as e:
                    dither = None

                try:
                    expType = msg.expType
                except AttributeError as e:
                    expType = "dark"

                try:
                    comment = msg.comment
                except AttributeError as e:
                    comment = ""

                if dither != None:
                    cmdVar = doDither(msg.cmd, actorState, dither)
                    if checkFailure(msg,cmdVar,"Failed to move APOGEE dither to %s position."%(dither),finish=False):
                        continue

                timeLim = msg.expTime + 15.0  # seconds
                if True:                # really take data
                    cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=msg.cmd,
                                                        cmdStr="expose time=%0.1f object=%s %s" %
                                                        (msg.expTime, expType,
                                                         ("comment=%s" % qstr(comment)) if comment else ""),
                                                        keyVars=[], timeLim=timeLim)
                    success = not cmdVar.didFail

                else:
                    msg.cmd.warn('text="Not taking a %gs exposure"' % (msg.expTime))
                    success = True

                if not success:
                    msg.cmd.warn('text="failed to start %s exposure"' % (expType))
                else:
                    msg.cmd.inform('text="done with %s exposure"' % (expType))
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=success)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception as e:
            sopActor.handle_bad_exception(actor,e,threadName,msg)

def script_main(actor, queues):
    """Main loop for APOGEE scripting thread"""

    threadName = "apogeeScript"
    actorState = myGlobals.actorState
    timeout = actorState.timeout
    apogeeFlatCB = ApogeeCB()

    script = None

    # Set up readout callback object:

    while True:
        try:
            msg = actorState.queues[sopActor.APOGEE_SCRIPT].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                apogeeFlatCB.shutdown()
                return

            elif msg.type == Msg.NEW_SCRIPT:
                if self.script:
                    msg.cmd.warn('text="%s thread is already running a script: %s"' %
                             (threadName, script.name))
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
                self.script = msg.script
                self.script.genStartKeys()
                actorState.queues[sopActor.APOGEE_SCRIPT].put(Msg.SCRIPT_STEP, msg.cmd)

            elif msg.type == Msg.SCRIPT_STEP:
                pass

            elif msg.type == Msg.STOP_SCRIPT:
                if not self.script:
                    msg.cmd.warn('text="%s thread is not running a script, so cannot stop it."' %
                             (threadName))
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)

            elif msg.type == Msg.POST_FLAT:
                cmd = msg.cmd
                n = 3

                if False:
                    cmd.warn('text="SKIPPING flat exposure"')
                else:
                    actorState.queues[sopActor.APOGEE].put(Msg.EXPOSE, cmd, replyQueue=msg.replyQueue,
                                                           expTime=50, expType='DomeFlat')
                    apogeeFlatCB.waitForNthRead(cmd, n, msg.replyQueue)

            elif msg.type == Msg.APOGEE_PARK_DARKS:
                cmd = msg.cmd
                n = 2
                expTime = 100.0

                if True:
                    cmd.warn('text="SKIPPING darks"')
                    success = True
                else:
                    success = True

                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=success)

            elif msg.type == Msg.EXPOSURE_FINISHED:
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=msg.success)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor,e,threadName,msg)
