import Queue, threading
import math, numpy

from sopActor import *
import sopActor.myGlobals
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback
from sopActor import MultiCommand

def main(actor, queues):
    """Main loop for master thread"""

    threadName = "master"
    timeout = sopActor.myGlobals.actorState.timeout

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
                leaveScreenClosed = msg.leaveScreenClosed

                overhead = 150          # overhead per exposure, minimum; seconds
                #
                # Close the petals
                #
                success = True

                if nflat + narc > 0:
                    if not MultiCommand(cmd, actorState.timeout,
                                        actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=False).run():
                        cmd.warn('text="Failed to close the flat field screen"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # Biases
                #
                if nbias + ndark > 0:
                    turnLampsOff = MultiCommand(cmd, actorState.timeout)

                    turnLampsOff.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
                    turnLampsOff.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
                    turnLampsOff.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
                    turnLampsOff.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
                    turnLampsOff.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

                    if not turnLampsOff.run():
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
                    turnFFOn = MultiCommand(cmd, actorState.timeout)

                    turnFFOn.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=True)
                    turnFFOn.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
                    turnFFOn.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
                    turnFFOn.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
                    turnFFOn.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

                    if not turnFFOn.run():
                        cmd.warn('text="Failed to prepare for flat exposure"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                    for n in range(nflat):
                        cmd.inform('text="Taking a %gs flat exposure"' % (flatTime))

                        doFlat = MultiCommand(cmd, flatTime + guiderFlatTime + overhead)

                        if flatTime > 0:
                            doFlat.append(actorState.queues[sopActor.BOSS], Msg.EXPOSE, expTime=flatTime, expType="flat",
                                          timeout=flatTime + 180)

                        if guiderFlatTime > 0 and msg.cartridge > 0:
                            doFlat.append(actorState.queues[sopActor.GCAMERA], Msg.EXPOSE,
                                          expTime=guiderFlatTime, expType="flat", cartridge=msg.cartridge,
                                          timeout=guideFlatTime + 15)
                                         

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
                    turnArcsOn = MultiCommand(cmd, arcTime + overhead)

                    turnArcsOn.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
                    turnArcsOn.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=True)
                    turnArcsOn.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=True)
                    turnArcsOn.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
                    turnArcsOn.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

                    if not turnArcsOn.run():
                        cmd.warn('text="Failed to prepare for arc exposure"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                    for n in range(narc):
                        cmd.inform('text="Take a %gs arc exposure here"' % (arcTime))
                        if not MultiCommand(cmd, actorState.timeout,
                                            actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                            expTime=arcTime, expType="arc").run():
                            cmd.warn("text=\"Failed to take arc\"")
                            success = False
                            return

                    if not success:
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue
                #
                # We're done.  Return telescope to desired state
                #
                turnLampsOff = MultiCommand(cmd, actorState.timeout)

                turnLampsOff.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
                turnLampsOff.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
                turnLampsOff.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
                turnLampsOff.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
                turnLampsOff.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)
                
                if not turnLampsOff.run():
                    cmd.warn('text="Failed to turn lamps off"')
                    msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                    continue

                if not leaveScreenClosed:
                    if not MultiCommand(cmd, actorState.timeout,
                                        actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=True).run():
                        cmd.warn('text="Failed to open the flat field screen"')
                        msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=False)
                        continue

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=True)

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
