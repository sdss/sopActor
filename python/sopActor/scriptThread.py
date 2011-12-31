import Queue, threading
import math, numpy, time

import sopActor
from sopActor import *
import sopActor.myGlobals as myGlobals

import script
reload(script)

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

def main(actor, queues):
    """Main loop for general scripting thread.

    Can only run one script at a time, but only because the issues have not
    been thought through."""

    threadName = "script"
    myQueueName = sopActor.SCRIPT
    runningScript = None

    while True:
        actorState = myGlobals.actorState
        timeout = actorState.timeout

        try:
            msg = actorState.queues[myQueueName].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))
                return

            elif msg.type == Msg.NEW_SCRIPT:
                if runningScript:
                    msg.cmd.fail('text="%s thread is already running a script: %s"' %
                                 (threadName, runningScript.name))
                    continue

                scriptName = msg.scriptName
                runningScript = script.Script(msg.cmd, scriptName)
                actorState.queues[myQueueName].put(Msg.SCRIPT_STEP, msg.cmd)

            elif msg.type == Msg.SCRIPT_STEP:
                if not runningScript:
                    msg.cmd.fail('text="%s thread is not running a script, so cannot step it."' %
                             (threadName))
                    continue

                scriptLine = runningScript.fetchNextStep()
                if not scriptLine:
                    msg.cmd.finish('text="script %s appears to be done"' % (runningScript.name))
                    runningScript = None
                    continue

                actorName, cmdStr, maxTime = scriptLine
                if maxTime == 0.0:
                    maxTime = 30.0

                msg.cmd.warn('text="firing off script line: %s %s (maxTime=%0.1f)"' % (actorName, cmdStr, maxTime))
                cmdVar = actorState.actor.cmdr.call(actor=actorName, forUserCmd=msg.cmd,
                                                    cmdStr=cmdStr,
                                                    timeLim=maxTime+15)
                if cmdVar.didFail:
                    msg.cmd.fail('text="Failed to run %s %s"' % (actorName, cmdStr))
                    runningScript = None
                else:
                    actorState.queues[myQueueName].put(Msg.SCRIPT_STEP, msg.cmd)

            elif msg.type == Msg.STOP_SCRIPT:
                if not runningScript:
                    msg.cmd.fail('text="%s thread is not running a script, so cannot stop it."' %
                                 (threadName))
                    continue

                # Just signal that we are done.
                runningScript.stop()

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
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=False)
            except Exception, e:
                pass
