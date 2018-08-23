import math
import Queue
import threading

import numpy

import sopActor
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback
from sopActor import *


def main(actor, queues):
    """Main loop for flat field screen thread"""

    threadName = 'ffs'
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.FFS].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform(
                        "text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return
            elif msg.type == Msg.FFS_MOVE:
                cmd = msg.cmd

                ffsStatus = actorState.models['mcp'].keyVarDict['ffsStatus']

                open, closed = 0, 0
                giveUp = False
                for s in ffsStatus:
                    if s == None:
                        cmd.warn('text="Failed to get state of flat field screen from MCP"')
                        giveUp = True
                        break

                    open += int(s[0])
                    closed += int(s[1])

                if giveUp:
                    msg.replyQueue.put(Msg.FFS_COMPLETE, cmd=cmd, success=False)
                    continue

                action = None  # what we need to do
                if closed == 8:  # flat field screens are all closed
                    if msg.open:
                        action = 'open'
                    else:
                        pass  # nothing to do
                elif open == 8:  # flat field screens are all open
                    if msg.open:
                        pass  # nothing to do
                    else:
                        action = 'close'
                else:
                    cmd.warn('text=%s' %
                             qstr('Flat field screens are neither open nor closed (%d v. %d)' %
                                  (open, closed)))
                    msg.replyQueue.put(Msg.FFS_COMPLETE, cmd=cmd, success=False)

                    continue

                if action:
                    ffsStatusKey = actorState.models['mcp'].keyVarDict['ffsStatus']

                    timeLim = 120.0  # seconds
                    cmdVar = actorState.actor.cmdr.call(
                        actor='mcp',
                        forUserCmd=cmd,
                        cmdStr=('ffs.%s' % action),
                        keyVars=[ffsStatusKey],
                        timeLim=timeLim)
                    if cmdVar.didFail:
                        cmd.warn("text=\"Failed to %s flat field screen\"" % action)

                        msg.replyQueue.put(Msg.FFS_COMPLETE, cmd=cmd, success=False)

                        continue

                msg.replyQueue.put(Msg.FFS_COMPLETE, cmd=cmd, success=True)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                msg.cmd.warn('Unknown message type %s' % msg.type)
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor, e, threadName, msg)
