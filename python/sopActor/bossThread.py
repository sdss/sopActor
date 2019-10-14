"""Thread to send commands to the BOSS camera."""
import Queue
import threading

import sopActor
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from sopActor import Msg


def getExpTimeCmd(expTime, expType, cmd, readout=True):
    """
    Return an exposure time command string and a readout command string,
    to append to a boss exposure cmdr call.
    """
    expTimeCmd = ''
    readoutCmd = ''
    if expTime >= 0:
        if expType != 'bias':
            expTimeCmd = ('itime=%g' % expTime)
            readoutCmd = '' if readout else 'noreadout'
    else:
        readoutCmd = 'readout'
        if not readout:
            cmd.warn('text="Saw readout == False but expTime == %g"' % expTime)
    return expTimeCmd, readoutCmd


def single_hartmann(cmd, actorState, replyQueue, expTime, mask):
    """Take a single hartmann frame, with one hartmann in position."""
    expType = 'arc'
    expTimeCmd, readoutCmd = getExpTimeCmd(expTime, expType, cmd)

    validMasks = ('left', 'right', 'out')
    if mask.lower() not in validMasks:
        err = qstr("Do not understand Hartmann mask '%s'." % mask)
        cmd.error('text=%s' % err)
        replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
        return

    cmd.inform('text=\"Taking a %gs %s Hartmann exposure.\"' % (expTime, mask))

    timeLim = expTime + 180.0  # seconds
    timeLim += 100
    cmdVar = actorState.actor.cmdr.call(
        actor='boss',
        forUserCmd=cmd,
        cmdStr=('exposure %s %s hartmann=%s' % (expType, expTimeCmd, mask)),
        keyVars=[],
        timeLim=timeLim)

    replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=not cmdVar.didFail)


#...


def hartmann(cmd, actorState, replyQueue, args):
    """Collimate the spectrographs via 'hartmann collimate (args)'"""
    cmd.respond("text=\"Starting BOSS Hartmann collimation sequence\"")
    timeLim = 260  # two readouts, plus flush and expose time, and moving pistons.
    cmdStr = 'collimate'
    if args is not None:
        cmdStr = ' '.join((cmdStr, args))
    cmdVar = actorState.actor.cmdr.call(
        actor='hartmann', forUserCmd=cmd, cmdStr=cmdStr, keyVars=[], timeLim=timeLim)

    if cmdVar.didFail:
        cmd.error("text='Failed to collimate BOSS spectrographs.")
    replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=not cmdVar.didFail)


def main(actor, queues):
    """Main loop for boss ICC thread"""

    threadName = 'boss'
    actorState = sopActor.myGlobals.actorState
    timeout = actorState.timeout

    while True:
        try:
            msg = queues[sopActor.BOSS_ACTOR].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform(
                        "text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.EXPOSE:
                expType = getattr(msg, 'expType', '')
                if msg.readout and msg.expTime <= 0:
                    cmdTxt = 'exposure readout'
                else:
                    cmdTxt = '%s%s exposure' % (((('%gs ' % msg.expTime)
                                                  if msg.expTime > 0 else ''), expType))
                msg.cmd.respond('text="starting %s"' % cmdTxt)
                expTimeCmd, readoutCmd = getExpTimeCmd(msg.expTime, expType, msg.cmd, msg.readout)

                timeLim = msg.expTime + 180.0  # seconds
                timeLim += 100
                cmdVar = actorState.actor.cmdr.call(
                    actor='boss',
                    forUserCmd=msg.cmd,
                    cmdStr=('exposure %s %s %s' % (expType, expTimeCmd, readoutCmd)),
                    keyVars=[],
                    timeLim=timeLim)
                if cmdVar.didFail:
                    msg.cmd.error('text="BOSS failed on %s"' % cmdTxt)

                if getattr(msg, 'finish_msg', False):
                    msg.cmd.inform(msg.finish_msg)

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=msg.cmd, success=not cmdVar.didFail)

            elif msg.type == Msg.SINGLE_HARTMANN:
                single_hartmann(msg.cmd, actorState, msg.replyQueue, msg.expTime, msg.mask)

            elif msg.type == Msg.HARTMANN:
                args = getattr(msg, 'args', None)
                hartmann(msg.cmd, actorState, msg.replyQueue, args)

            elif msg.type == Msg.STATUS:
                msg.cmd.inform('text="%s thread"' % threadName)
                msg.replyQueue.put(Msg.REPLY, cmd=msg.cmd, success=True)
            else:
                raise ValueError, ('Unknown message type %s' % msg.type)


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)
        except Exception, e:
            sopActor.handle_bad_exception(actor, e, threadName, msg)
