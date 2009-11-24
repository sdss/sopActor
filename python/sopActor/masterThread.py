import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback
from sopActor import MultiCommand

def doLamps(cmd, actorState, FF=False, Ne=False, HgCd=False, WHT=False, UV=False,
            openFFS=None, openHartmann=None):
    """Turn all the lamps on/off"""

    multiCmd = MultiCommand(cmd, actorState.timeout)

    multiCmd.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=FF)
    multiCmd.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=Ne)
    multiCmd.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=HgCd)
    multiCmd.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=WHT)
    multiCmd.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=UV)
    if openFFS is not None:
        multiCmd.append(actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=openFFS)
    #
    # There's no Hartmann thread, so just open them synchronously for now.  This should be rare.
    #
    if openHartmann is not None:
        cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd,
                                            cmdStr=("hartmann out"), keyVars=[], timeLim=actorState.timeout)

        if cmdVar.didFail:
            cmd.warn('text="Failed to take Hartmann mask out"')
            return False
    
    return multiCmd.run()

def main(actor, queues):
    """Main loop for master thread"""

    threadName = "master"
    timeout = sopActor.myGlobals.actorState.timeout
    overhead = 150                      # overhead per exposure, minimum; seconds

    while True:
        try:
            msg = queues[MASTER].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.DO_CALIB:
                #import pdb; pdb.set_trace()

                cmd = msg.cmd
                actorState = msg.actorState

                narc = msg.narc
                nbias = msg.nbias
                ndark = msg.ndark
                nflat = msg.nflat
                arcTime = msg.arcTime
                darkTime = msg.darkTime
                flatTime = msg.flatTime
                guiderFlatTime = msg.guiderFlatTime
                inEnclosure = msg.inEnclosure
                startGuider = msg.startGuider
                #
                # Close the petals
                #
                success = True

                if not inEnclosure and nflat + narc > 0:
                    if not MultiCommand(cmd, timeout,
                                        actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=False).run():
                        cmd.warn('text="Failed to close the flat field screen"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # Biases
                #
                if nbias + ndark > 0:
                    if not doLamps(cmd, actorState):
                        cmd.warn('text="Failed to prepare for bias/dark exposures"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                    for n in range(nbias):
                        cmd.inform('text="Taking a bias exposure"')

                        if not MultiCommand(cmd, overhead,
                                            actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                            expTime=0.0, expType="bias").run():
                            cmd.warn('text="Failed to take bias"')
                            success = False
                            break

                        cmd.inform('text="bias exposure finished"')

                        if not success:
                            msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                            continue

                    for n in range(ndark):
                        cmd.inform('text="Taking a %gs dark exposure"' % darkTime)

                        if not MultiCommand(cmd, darkTime + overhead,
                                            actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                            expTime=darkTime, expType="dark").run():
                            cmd.warn('text="Failed to take dark"') 
                            success = False
                            break

                        cmd.inform('text="bias exposure finished"')

                    if not success:
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # Flats
                #
                if nflat > 0:
                    if not doLamps(cmd, actorState, FF=True):
                        cmd.warn('text="Failed to prepare for flat exposure"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                    for n in range(nflat):
                        doFlat = MultiCommand(cmd, flatTime + guiderFlatTime + overhead)

                        if flatTime > 0:
                            cmd.inform('text="Taking a %gs flat exposure"' % (flatTime))

                            doFlat.append(actorState.queues[sopActor.BOSS], Msg.EXPOSE, expTime=flatTime, expType="flat",
                                          timeout=flatTime + 180)

                        if guiderFlatTime > 0 and msg.cartridge > 0:
                            cmd.inform('text="Taking a %gs guider flat exposure"' % (guiderFlatTime))
                            doFlat.append(actorState.queues[sopActor.GCAMERA], Msg.EXPOSE,
                                          expTime=guiderFlatTime, expType="flat", cartridge=msg.cartridge,
                                          timeout=guiderFlatTime + 15)
                                         

                        if not doFlat.run():
                            cmd.warn('text="Failed to take flat field"')
                            success = False
                            break

                        cmd.inform('text="flat exposure finished"')

                    if not success:
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # Arcs
                #
                if narc > 0:
                    if not doLamps(cmd, actorState, Ne=True, HgCd=True):
                        cmd.warn('text="Failed to prepare for arc exposure"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                    for n in range(narc):
                        cmd.inform('text="Taking a %gs arc exposure"' % (arcTime))
                        if not MultiCommand(cmd, arcTime + overhead,
                                            actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                            expTime=arcTime, expType="arc").run():
                            cmd.warn("text=\"Failed to take arc\"")
                            failed = True
                            break
                        
                    if not success:
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # We're done.  Return telescope to desired state
                #
                if not doLamps(cmd, actorState, openFFS=None if inEnclosure else True):
                    cmd.warn('text="Failed to turn lamps off"')
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    continue

                if startGuider:
                    # Try to start the guider; ignore any responses on the queue
                    
                    msg.cmd.warn('text="Starting guider"')
                    actorState.actor.cmdr.cmdq(actor="guider", forUserCmd=cmd, cmdStr=("on"))

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=True)

            elif msg.type == Msg.DO_SCIENCE:
                #import pdb; pdb.set_trace()

                cmd = msg.cmd
                actorState = msg.actorState

                expTime = msg.expTime
                #
                # Open the petals and Hartmann doors
                #
                if not doLamps(cmd, actorState, openFFS=True, openHartmann=True):
                    cmd.warn('text="Failed to open the flat field screen"')
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    continue

                cmd.inform('text="Taking a science exposure"')

                if not MultiCommand(cmd, overhead + expTime,
                                    actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                    expTime=expTime, expType="science").run():
                    cmd.warn('text="Failed to take science exposure"')
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    continue

                cmd.inform('text="science exposure finished"')

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=True)

            elif msg.type == Msg.HARTMANN:
                """Take two arc exposures with the left then the right Hartmann screens in"""
                #import pdb; pdb.set_trace()

                cmd = msg.cmd
                actorState = msg.actorState

                expTime = msg.expTime
                sp1 = msg.sp1
                sp2 = msg.sp2
                if sp1:
                    if not sp2:
                        specArg = "spec=sp1"
                elif sp2:
                    specArg = "spec=sp2"
                else:
                    specArg = ""

                ffsState0 = actorState.models["mcp"].keyVarDict["ffsCommandedOpen"][0] # initial state
                openFFS = False if ffsState0 else None                                 # False => close FFS

                if not doLamps(cmd, actorState, Ne=True, HgCd=True, openFFS=openFFS):
                    cmd.warn('text="Some lamps failed to turn on"')
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    continue

                success = True
                for state, expose in [("left", True), ("right", True)]:
                    if expose:
                        if False:
                            print "XXXXXXXXXXXX Faking exposure"
                            cmd.warn('text="XXXXXXXXXXXX Faking exposure"')
                            continue

                        cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd,
                                                            cmdStr=("exposure %s itime=%g hartmann=%s" % \
                                                                        ("arc", expTime, state)),
                                                            keyVars=[], timeLim=expTime + overhead)

                        if cmdVar.didFail:
                            cmd.warn('text="Failed to take %gs exposure"' % expTime)
                            cmd.warn('text="Moving Hartmann masks out"')
                            success = False
                            break

                #
                # We're done.  Return telescope to desired state
                #
                openFFS = True if ffsState0 else None # True => reopen FFS

                if not doLamps(cmd, actorState, openFFS=openFFS):
                    cmd.warn('text="Failed to turn lamps off"')
                    success = False

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=success)

            elif msg.type == Msg.DITHERED_FLAT:
                """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

                cmd = msg.cmd
                actorState = msg.actorState

                expTime = msg.expTime
                spN = msg.spN
                nStep = msg.nStep
                nTick = msg.nTick

                if not doLamps(cmd, actorState, FF=True):
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    cmd.warn('text="Some lamps failed to turn on"')
                    continue

                success = True          # let's be optimistic
                moved = 0
                for i in range(nStep + 1):  # +1: final large move to get back to where we started
                    expose = True
                    if i == 0:
                        move = nTick*(nStep//2)
                    elif i == nStep:
                        move = -moved
                        expose = False
                    else:
                        move = -nTick

                    dA = dB = move
                    dC = -dA

                    for sp in spN:
                        cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd,
                                                            cmdStr=("moveColl spec=%s a=%d b=%d c=%d" % (sp, dA, dB, dC)),
                                                            keyVars=[], timeLim=timeout)

                        if cmdVar.didFail:
                            cmd.warn('text="Failed to move collimator for %s"' % sp)
                            success = False
                            break

                    if not success:
                        break

                    moved += move
                    cmd.inform('text="After %dth collimator move: at %d"' % (i, moved))

                    if expose:
                        if False:
                            cmd.inform('text="XXXXX Not taking a %gs exposure"' % expTime)
                        else:
                            cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd,
                                                                cmdStr=("exposure %s itime=%g" % ("flat", expTime)),
                                                                keyVars=[], timeLim=expTime + overhead)

                            if cmdVar.didFail:
                                cmd.warn('text="Failed to take %gs exposure"' % expTime)
                                cmd.warn('text="Moving collimators back to initial positions"')

                                dA = dB = -moved
                                dC = -dA

                                for sp in spN:
                                    cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd,
                                                                        cmdStr=("moveColl spec=%s a=%d b=%d c=%d" % (sp, dA, dB, dC)),
                                                                        keyVars=[], timeLim=timeout)

                                    if cmdVar.didFail:
                                        cmd.warn('text="Failed to move collimator for %s back to initial position"' % sp)
                                        break

                                success = False
                                break

                doLamps(cmd, actorState)

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=success)

            elif msg.type == Msg.EXPOSURE_FINISHED:
                if msg.success:
                    cmd.finish()
                else:
                    msg.cmd.fail("")

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
                msg.replyQueue.put(Msg.EXIT, cmd=msg.cmd, success=False)
            except Exception, e:
                pass
