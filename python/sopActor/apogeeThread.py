import Queue, threading
import math, numpy, time

import sopActor
from sopActor import *
import sopActor.myGlobals as myGlobals
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

class ApogeeCB(object):
    def __init__(self):
        apogeeModel = myGlobals.actorState.models['apogee']
        apogeeModel.keyVarDict["utrReadState"].addCallback(self.listenToReads, callNow=False)
        self.reset()
        self.cmd = myGlobals.actorState.actor.bcast

    def shutdown(self):
        apogeeModel.keyVarDict["utrReadState"].removeCallback(self.listenToReads)

    def listenToReads(self, key):
        state = key[1]
        n = key[2]

        if self.triggerCount < 0:
            return
        if str(state) != "Saving":
            return

        self.count += 1
        
        if not self.cmd.isAlive():
            self.cmd = myGlobals.actorState.actor.bcast
        self.cmd.warn('text="utrReadState=%s,%s count=%s trigger=%s"' %
                      (state, n, self.count, self.triggerCount))

        if self.count == self.triggerCount:
            self.reset()
            self.cb()

    def reset(self):
        self.count = 0
        self.triggerCount = -1

    def waitForNthRead(self, cmd, n, q, msg, cb=None):
        self.reset()
        self.cmd = cmd
        self.triggerCount = n
        self.q = q
        self.msg = msg

        self.cb = cb if cb else self.flashLamps

    def flashLamps(self):
        timeLim = 5.0          # seconds
        t0 = time.time()

        if self.cmd:
            self.cmd.warn('text="would flash lamps"')
        cmdr = myGlobals.actorState.actor.cmdr
        replyQueue = Queue.Queue()
        sopActor.queues[sopActor.FF_LAMP].put(Msg.LAMP_ON, cmd, replyQueue=replyQueue)
        replyQueue.get(True, 10)
        #cmdVar = cmdr.call(actor="mcp", forUserCmd=self.cmd, 
        #                   cmdStr="ff.on",
        #                   timeLim=timeLim)
        t1 = time.time()
        if cmdVar.didFail:
            cmd.warn('text="ff lamp on command failed"')

        time.sleep(5.0)
        t2 = time.time()
        sopActor.queues[sopActor.FF_LAMP].put(Msg.LAMP_OFF, cmd, replyQueue=replyQueue)
        replyQueue.get(True, 10)
        #cmdVar = cmdr.call(actor="mcp", forUserCmd=msg.cmd, 
        #                   cmdStr="ff.off",
        #                   timeLim=timeLim)
        t3 = time.time()
        if cmdVar.didFail:
            cmd.warn('text="ff lamp off command failed"')
        cmd.warn('text="times=%0.2f %0.2f %0.2f"' % (t1-t0,t2-t0,t3-t0)) 

        self.q.put(self.msg)
        
def main(actor, queues):
    """Main loop for APOGEE ICC thread"""

    threadName = "apogee"
    actorState = myGlobals.actorState
    timeout = actorState.timeout
    
    # Set up readout callback object:
    
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

                try:
                    expType = msg.expType
                except AttributeError.e:
                    expType = "dark"
                    
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
                                                        (msg.expTime, expType, qstr(msg.comment)),
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

def script_main(actor, queues):
    """Main loop for APOGEE ICC thread"""

    threadName = "apogeeScript"
    actorState = myGlobals.actorState
    timeout = actorState.timeout
    apogeeFlatCB = ApogeeCB()
    
    # Set up readout callback object:
    
    while True:
        try:
            msg = queues[sopActor.APOGEE_SCRIPT].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                apogeeFlatCB.shutdown()
                return

            elif msg.type == Msg.POST_FLAT:
                cmd = msg.cmd
                n = 2
                replyQueue = Queue.Queue()
                replyMsg = True

                if False:
                    cmd.warn('text="SKIPPING flat exposure"')
                else:
                    queues[sopActor.APOGEE].put(Msg.EXPOSE, cmd, replyQueue=queues[sopActor.APOGEE_SCRIPT],
                                                expTime=50, expType='dark')

                apogeeFlatCB.waitForNthRead(cmd, n, replyQueue, replyMsg, cb=None)
                ret = replyQueue.get(True, 100)
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=ret)

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
                msg.replyQueue.put(Msg.EXIT, cmd=msg.cmd, success=False)
            except Exception, e:
                pass

