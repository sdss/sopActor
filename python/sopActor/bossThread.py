import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def main(actor, queues):
    """Main loop for boss ICC thread"""

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
                msg.cmd.respond("text=\"starting exposure\"")

                timeLim = msg.expTime + 180.0  # seconds
                if True:
                    cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=msg.cmd,
                                                        cmdStr=("exposure %s itime=%g" % (msg.expType, msg.expTime)),
                                                        keyVars=[], timeLim=timeLim)
                else:
                    msg.cmd.inform('text="Not taking a %gs exposure"' % msg.expTime)

                    class Foo(object):
                        @property
                        def didFail(self): return False
                    cmdVar = Foo()
                    
                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="BOSS thread"')
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
        except Queue.Empty:
            actor.bcast.diag("text=\"boss alive\"")
