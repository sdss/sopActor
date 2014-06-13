"""Threads to control all of the mcp-related lamps."""
import Queue, threading, time
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

# don't bother doing anything with these lamps, as they aren't used for anything.
ignore_lamps = ['uv', 'wht']

class LampHandler(object):
    def __init__(self, actorState, queue, lampName):
        self.actorState = actorState
        self.queue = queue
        self.lampName = lampName
        self.name = self.lampName.lower()

    def do_lamp(self, cmd, action, replyQueue, noWait=False, delay=None):
        """
        Perform action on this lamp (on or off).

        * noWait: don't wait for a response from the mcp and just assume it
          succeeded. Useful if you want to quickly change the lamp state, without
          worrying about if something timed-out inbetween.
        * delay: wait that long before claiming success. Use this if the lamp
          takes a while to be fully "on".
        """

        if self.lampName in ignore_lamps:
            cmd.diag('text="ignoring %s.%s"' % (action, self.lampName))
            replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
            return

        # seconds
        timeLim = 0.1 if noWait else 30.0
        cmdVar = self.actorState.actor.cmdr.call(actor="mcp", forUserCmd=cmd,
                                                 cmdStr=("%s.%s" % (self.name, action)),
                                                 timeLim=timeLim)
        if noWait:
            cmd.warn('text="Not waiting for response from: %s %s"' % (self.lampName, action))
            replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=True)
        elif cmdVar.didFail:
            bypassName = "%s_lamp" % (self.name)
            bypassed = Bypass.get(name=bypassName)
            cmd.error('text="Failed to turn %s lamps %s (bypass(%s) = %s)"' % (self.lampName, action, bypassName, bypassed))
            if bypassed:
                cmd.warn('text="Ignoring failure on %s lamps"' % (self.lampName))
                replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=True)
            else:
                replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=False)
        elif delay is not None:
            if delay > 0:
                cmd.inform('text="Waiting %gs for %s lamps to warm up"' % (delay, self.lampName))

            endTime=time.time() + delay
            self.queue.put(Msg.WAIT_UNTIL, cmd=cmd, replyQueue=replyQueue, endTime=endTime)
        else:
            replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=True)

    def wait_until(self, cmd, endTime, replyQueue):
        """Wait until we reach endTime, to allow the lamp to warm up."""
        timeToGo = endTime - time.time()
        
        if timeToGo <= 0:
            replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=True)
        elif self.actorState.aborting:
            cmd.warn('text="Aborting warmup for %s lamps"' % (self.lampName))
            replyQueue.put(Msg.LAMP_COMPLETE, cmd=cmd, success=False)
        else:
            # output status every 5 seconds, unless we're almost done.
            if timeToGo > 1 and int(timeToGo)%5 == 0:
                cmd.inform('text="Warming up %s lamps; %ds left"' % (self.lampName, timeToGo))
            
            time.sleep(1)
            self.queue.put(Msg.WAIT_UNTIL, cmd=cmd, replyQueue=replyQueue, endTime=endTime)
#...


def lamp_main(actor, queue, lampName):
    """Main loop for lamps thread"""

    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout
    threadName = lampName
    lampHandler = LampHandler(actorState, queue, lampName)

    while True:
        try:
            msg = queue.get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform('text="Exiting thread %s"' % (threading.current_thread().name))

                return
            elif msg.type == Msg.LAMP_ON:
                action = "on" if msg.on else "off"
                noWait = hasattr(msg, 'noWait')
                delay = getattr(msg, "delay", None)
                lampHandler.do_lamp(msg.cmd, action, msg.replyQueue, delay=delay, noWait=noWait)

            elif msg.type == Msg.WAIT_UNTIL:
                # used to delay while the lamps warm up
                lampHandler.wait_until(msg.cmd, msg.endTime, msg.replyQueue)


            elif msg.type == Msg.STATUS:
                if lampName not in ignore_lamps:
                    msg.cmd.inform('text="%s thread"' % threadName)
                    msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)

            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor, e, threadName, msg)

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
