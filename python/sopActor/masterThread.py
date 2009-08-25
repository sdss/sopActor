import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

class SopState(object):
    """Save the state of the sop"""

    def __init__(self):
        pass
try:
    gState
except:
    gState = SopState()

def main(actor, queues):
    """Main loop for master thread"""

    timeout = sopActor.myGlobals.actorState.timeout

    guideCmd = None                     # the Cmd that started the guide loop
    
    while True:
        try:
            msg = queues[MASTER].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return
            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="Master thread"')
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ("Unknown message type %s" % msg.type)
        except Queue.Empty:
            actor.bcast.diag("text=\"master alive\"")
