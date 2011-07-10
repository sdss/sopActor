import Queue, threading
import math, numpy, time

import sopActor
from sopActor import *
import sopActor.myGlobals as myGlobals

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from twisted.internet import reactor, defer
def twistedSleep(secs):
    d = defer.deferred()
    reactor.callLater(secs, d.callback, None)
    return d
                
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

class ApogeeCB(object):
    def __init__(self):
        self.cmd = myGlobals.actorState.actor.bcast
        self.reset()
        myGlobals.actorState.models['apogee'].keyVarDict["utrReadState"].addCallback(self.listenToReads, callNow=False)

    def shutdown(self):
        myGlobals.actorState.models['apogee'].keyVarDict["utrReadState"].removeCallback(self.listenToReads)

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
        self.cmd.warn('text="calling wht.off"')
        replyQueue = sopActor.Queue("apogeeFlasher")
        myGlobals.actorState.queues[sopActor.FF_LAMP].put(Msg.LAMP_ON, cmd=self.cmd, on=False, replyQueue=replyQueue)
        #ret = replyQueue.get()
        #self.cmd.diag('text="wht.off ret: %s"' % (ret))
        self.cmd.diag('text="called wht.off"')
                               
        
    def flashLamps(self):
        timeLim = 5.0          # seconds
        t0 = time.time()

        self.cmd.warn('text="might flash lamps"')
    
        cmdr = myGlobals.actorState.actor.cmdr
        replyQueue = sopActor.Queue("apogeeFlasher")
        self.cmd.diag('text="calling wht.on"')
        myGlobals.actorState.queues[sopActor.FF_LAMP].put(Msg.LAMP_ON, cmd=self.cmd, on=True, replyQueue=replyQueue)
        #ret = replyQueue.get(True)
        #self.cmd.diag('text="wht.on ret: %s"' % (ret))

        self.cmd.warn('text="called wht.on"')
        t1 = time.time()
        if False: # cmdVar.didFail: # ret.success:
            self.cmd.warn('text="ff lamp on command failed"')
        else:
            self.cmd.diag('text="pausing..."')
            reactor.callLater(5.0, self.turnOffLamps)
            

        # self.q.put(Msg(Msg.EXPOSURE_FINISHED, cmd=self.cmd,  success=not cmdVar.didFail))
        
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
                ret = doDither(msg.cmd, actorState, msg.dither)
                
            elif msg.type == Msg.EXPOSE:
                msg.cmd.respond("text=\"starting %s%s exposure\"" % (
                        ((("%gs " % msg.expTime) if msg.expTime > 0 else ""), msg.expType)))

                try:
                    dither = msg.dither
                except AttributeError, e:
                    dither = None

                try:
                    expType = msg.expType
                except AttributeError, e:
                    expType = "dark"
                    
                try:
                    comment = msg.comment
                except AttributeError, e:
                    comment = ""
                    
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
                                                        cmdStr="expose time=%0.1f object=%s comment=%s" %
                                                        (msg.expTime, expType, qstr(comment)),
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
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
            except Exception, e:
                pass

def script_main(actor, queues):
    """Main loop for APOGEE ICC thread"""

    threadName = "apogeeScript"
    actorState = myGlobals.actorState
    timeout = actorState.timeout
    apogeeFlatCB = ApogeeCB()
    
    # Set up readout callback object:
    
    while True:
        try:
            msg = actorState.queues[sopActor.APOGEE_SCRIPT].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                apogeeFlatCB.shutdown()
                return

            elif msg.type == Msg.POST_FLAT:
                cmd = msg.cmd
                n = 2

                if False:
                    cmd.warn('text="SKIPPING flat exposure"')
                else:
                    actorState.queues[sopActor.APOGEE].put(Msg.EXPOSE, cmd, replyQueue=msg.replyQueue,
                                                           expTime=50, expType='DomeFlat')
                    apogeeFlatCB.waitForNthRead(cmd, n, msg.replyQueue)

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
            errMsg = "Unexpected exception %s in sop %s thread" % (e, threadName)
            actor.bcast.warn('text="%s"' % errMsg)
            tback(errMsg, e)

            try:
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
            except Exception, e:
                pass

