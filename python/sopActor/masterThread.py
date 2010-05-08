import Queue, threading
import math, time

from sopActor import *
import sopActor
import sopActor.myGlobals as myGlobals
#from SopCmd import status
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback
from sopActor import MultiCommand

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SopPrecondition(Precondition):
    """This class is used to pass preconditions for a command to MultiCmd.  Only if required() returns True
is the command actually scheduled and then run"""
    
    def __init__(self, queueName, msgId=None, timeout=None, **kwargs):
        Precondition.__init__(self, queueName, msgId, timeout, **kwargs)
        self.queueName = queueName

    def required(self):
        """Here is the real logic.  We're thinking of running a command to get the system into a
desired state, but if it's already in that state no command is required; so return False"""

        if self.queueName in myGlobals.warmupTime.keys():
            assert self.msgId == Msg.LAMP_ON

            isOn, timeSinceTransition = self.lampIsOn(self.queueName)
            if self.kwargs.get('on'):                    # we want to turn them on
                if not isOn:
                    timeSinceTransition = 0                              # we want the time since turn on
                warmupTime = myGlobals.warmupTime[self.queueName]
                delay = warmupTime - timeSinceTransition # how long until they're ready
                if delay > 0:
                    isOn = False
                    self.kwargs["delay"] = self.kwargs["duration"] = int(delay)

                if not isOn:    # op is required if they are not already on (or not warmed up)
                    return True
            else:
                return isOn
        elif self.queueName in (sopActor.FFS,):
            assert self.msgId == Msg.FFS_MOVE

            if self.kwargs.get('open'): # we want to open them
                return not self.ffsAreOpen() # op is required if they are not already open
            else:
                return self.ffsAreOpen()

        return True
    #
    # Commands to get state from e.g. the MCP
    #
    def ffsAreOpen(self):
        """Return True if flat field petals are open; False if they are close, and None if indeterminate"""

        ffsStatus = myGlobals.actorState.models["mcp"].keyVarDict["ffsStatus"]

        open, closed = 0, 0
        for s in ffsStatus:
            if s == None:
                raise RuntimeError, "Unable to read FFS status"

            open += int(s[0])
            closed += int(s[1])

        if open == 8:
            return True
        elif closed == 8:
            return False
        else:
            return None

    def lampIsOn(self, queueName):
        """Return (True iff some lamps are on, timeSinceTransition)"""

        if queueName == sopActor.FF_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["ffLamp"]
        elif queueName == sopActor.HGCD_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["hgCdLamp"]
        elif queueName == sopActor.NE_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["neLamp"]
        elif queueName == sopActor.UV_LAMP:
            return myGlobals.actorState.models["mcp"].keyVarDict["uvLampCommandedOn"], 0
        elif queueName == sopActor.WHT_LAMP:
            return myGlobals.actorState.models["mcp"].keyVarDict["whtLampCommandedOn"], 0
        else:
            print "Unknown lamp queue %s" % queueName
            return False, 0

        if status == None:
            raise RuntimeError, ("Unable to read %s lamp status" % queueName)

        on = 0
        for i in status:
            on += i

        return (True if on == 4 else False), (time.time() - status.timestamp)

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

# how long it takes to do various things
ffsDuration = 10                        # move the FFS
flushDuration = 15                      # flush the chips prior to an exposure
guiderReadoutDuration = 1               # readout the guider
hartmannDuration = 90                   # take a Hartmann sequence and move the collimators
readoutDuration = 90                    # read the chips

class SopMultiCommand(MultiCommand):
    """A MultiCommand for sop that knows about how long sop commands take to execute"""
    
    def __init__(self, cmd, timeout, label, *args, **kwargs):
        MultiCommand.__init__(self, cmd, timeout, label, *args, **kwargs)
        pass

    def setMsgDuration(self, queueName, msg):
        """Set msg's expected duration in seconds"""

        if msg.type == Msg.FFS_MOVE:
            msg.duration = ffsDuration
        elif msg.type == Msg.EXPOSE:
            msg.duration = 0

            if queueName == sopActor.GCAMERA:
                msg.duration += msg.expTime
                msg.duration += guiderReadoutDuration
            elif queueName == sopActor.BOSS:
                if msg.expTime >= 0:
                    msg.duration += flushDuration
                    msg.duration += msg.expTime
                if msg.readout:
                    msg.duration += readoutDuration
        elif msg.type == Msg.HARTMANN:
            msg.duration = hartmannDuration

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def doLamps(cmd, actorState, FF=False, Ne=False, HgCd=False, WHT=False, UV=False,
            openFFS=None, openHartmann=None):
    """Turn all the lamps on/off"""

    multiCmd = SopMultiCommand(cmd, actorState.timeout, ".doLamps")

    multiCmd.append(sopActor.FF_LAMP  , Msg.LAMP_ON, on=FF)
    multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=Ne)
    multiCmd.append(sopActor.NE_LAMP  , Msg.LAMP_ON, on=HgCd)
    multiCmd.append(sopActor.WHT_LAMP , Msg.LAMP_ON, on=WHT)
    multiCmd.append(sopActor.UV_LAMP  , Msg.LAMP_ON, on=UV)
    if openFFS is not None:
        multiCmd.append(sopActor.FFS, Msg.FFS_MOVE, open=openFFS)
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
    timeout = myGlobals.actorState.timeout
    overhead = 150                      # overhead per exposure, minimum; seconds

    while True:
        try:
            msg = queues[MASTER].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform("text=\"Exiting thread %s\"" % (threading.current_thread().name))

                return

            elif msg.type == Msg.DO_CALIBS:
                cmd = msg.cmd
                actorState = msg.actorState
                cmdState = msg.cmdState
                cartridge = msg.cartridge
                survey = msg.survey
                #
                # Tell sop that we've accepted the command
                #
                msg.replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
                #
                # Define the command that we use to communicate our state to e.g. STUI
                #
                def status(cmd):
                    if cmd:
                        actorState.actor.commandSets["SopCmd"].status(cmd, threads=False, finish=False)
                #
                # Take the data
                #
                ffsInitiallyOpen = SopPrecondition(None).ffsAreOpen()

                pendingReadout = False

                failMsg = ""            # message to use if we've failed
                while cmdState.nBiasLeft > 0 or cmdState.nDarkLeft > 0 or \
                          cmdState.nFlatLeft > 0 or cmdState.nArcLeft > 0:
                    
                    status(cmdState.cmd)

                    if cmdState.nBiasLeft > 0:
                        expTime, expType = 0.0, "bias"
                    elif cmdState.nDarkLeft > 0:
                        expTime, expType = cmdState.darkTime, "dark"
                    elif cmdState.nFlatLeft > 0:
                        expTime, expType = cmdState.flatTime, "flat"
                    elif cmdState.nArcLeft > 0:
                        expTime, expType = cmdState.arcTime, "arc"
                    else:
                        failMsg = "Impossible condition; complain to RHL"
                        break

                    if pendingReadout:
                        multiCmd = SopMultiCommand(cmd, actorState.timeout + readoutDuration, 
                                                   "doCalibs.pendingReadout")

                        multiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
                        pendingReadout = False

                        multiCmd.append(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False)
                        multiCmd.append(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False)

                        if expType in ("arc"):
                            multiCmd.append(sopActor.FFS      , Msg.FFS_MOVE, open=False)
                            multiCmd.append(sopActor.FF_LAMP  , Msg.LAMP_ON,  on=False)
                            multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON,  on=True)
                            multiCmd.append(sopActor.NE_LAMP  , Msg.LAMP_ON,  on=True)
                        else:
                            failMsg = "Impossible condition; complain to RHL"
                            break

                        if not multiCmd.run():
                            failMsg = "Failed to prepare for %s" % expType
                            break
                    #
                    # Now take the exposure
                    #
                    timeout = flushDuration + expTime + actorState.timeout
                    if expType in ("bias", "dark"):
                        timeout += readoutDuration

                    # We need to let MultiCommand entries adjust the command timeout, or have the preconditions
                    # take a separate timeout. In the meanwhile fudge the longest case.
                    if expType == "arc":
                        timeout += myGlobals.warmupTime[sopActor.HGCD_LAMP]

                    multiCmd = SopMultiCommand(cmd, timeout, "doCalibs.expose")

                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))

                    if expType == "arc":
                        pendingReadout = True
                        multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                        expTime=expTime, expType=expType, readout=False)

                        multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))
                        multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON,  on=False))
                        multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON,  on=True))
                        multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON,  on=True))
                    elif expType in ("bias", "dark"):
                        pendingReadout = False
                        multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                        expTime=expTime, expType=expType, readout=True)

                        multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
                        multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
                        multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
                    elif expType == "flat":
                        if cmdState.flatTime > 0:
                            pendingReadout = True
                            multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                            expTime=expTime, expType=expType, readout=False)

                        if cmdState.guiderFlatTime > 0 and cmdState.nArcDone == 0:
                            cmd.inform('text="Taking a %gs guider flat exposure"' % (cmdState.guiderFlatTime))
                            multiCmd.append(sopActor.GCAMERA, Msg.EXPOSE,
                                            expTime=cmdState.guiderFlatTime, expType="flat",
                                            cartridge=cartridge)

                        multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))
                        multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON,  on=True))
                        multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON,  on=False))
                        multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON,  on=False))
                    else:
                        failMsg = "Impossible condition; complain to RHL"
                        break

                    cmd.inform('text="Taking %s %s exposure"' %
                               (("an" if expType[0] in ("a", "e", "i", "o", "u") else "a"), expType))
                    if not multiCmd.run():
                        if pendingReadout:
                            SopMultiCommand(cmd, actorState.timeout + readoutDuration, "doCalibs.readout",
                                            sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True).run()

                        failMsg = "Failed to take %s exposure" % expType
                        break

                    if expType == "bias":
                        cmdState.nBiasDone += 1
                        cmdState.nBiasLeft -= 1
                    elif expType == "dark":
                        cmdState.nDarkDone += 1
                        cmdState.nDarkLeft -= 1
                    elif expType == "flat":
                        cmdState.nFlatDone += 1
                        cmdState.nFlatLeft -= 1
                    elif expType == "arc":
                        cmdState.nArcDone += 1
                        cmdState.nArcLeft -= 1
                    else:
                        failMsg = "Impossible condition; complain to RHL"
                        break
                #
                # Did we break out of that loop?
                #
                if failMsg:
                    if pendingReadout:
                        if not SopMultiCommand(cmd, actorState.timeout + readoutDuration, 
                                               "doCalibs.readoutCleanup",
                                               sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True).run():
                            cmd.warn('text="%s"' % failMsg)
                        cmd.fail('text="Failed to readout last exposure"')
                    else:
                        cmd.fail('text="%s"' % failMsg)

                        continue
                #
                # Readout any pending data and return telescope to initial state
                #
                multiCmd = SopMultiCommand(cmd,
                                           actorState.timeout + (readoutDuration if pendingReadout else 0),
                                           "doCalibs.readoutFinish")

                if pendingReadout:
                    multiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
                    pendingReadout = False

                multiCmd.append(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False)
                multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False)
                multiCmd.append(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False)
                multiCmd.append(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False)
                multiCmd.append(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False)

                multiCmd.append(sopActor.FFS, Msg.FFS_MOVE, open=ffsInitiallyOpen)

                if not multiCmd.run():
                    cmd.fail('text="Failed to restore telescope to pristine state"')
                    continue

                msg.replyQueue.put(Msg.EXPOSURE_FINISHED, cmd=cmd, success=True)
                #
                # We're done
                #
                if actorState.aborting:
                    cmd.fail('text="doCalibs was aborted')
                else:
                    cmd.finish('text="Your calibration data are ready, sir')

            elif msg.type == Msg.DO_SCIENCE:
                cmd = msg.cmd
                actorState = msg.actorState
                cmdState = msg.cmdState
                cartridge = msg.cartridge
                survey = msg.survey
                #
                # Tell sop that we've accepted the command
                #
                msg.replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
                #
                # Define the command that we use to communicate our state to e.g. STUI
                #
                def status(cmd):
                    if cmd:
                        actorState.actor.commandSets["SopCmd"].status(cmd, threads=False, finish=False)

                failMsg = ""            # message to use if we've failed
                while cmdState.nExpLeft > 0:
                    status(cmdState.cmd)

                    expTime = cmdState.expTime

                    multiCmd = SopMultiCommand(cmd,
                                               flushDuration + expTime + readoutDuration + actorState.timeout,
                                               "doScience")

                    multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                    expTime=expTime, expType="science", readout=True)
                    
                    multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=True))
                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON,  on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON,  on=False))
                    multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON,  on=False))
                    multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON,  on=False))
                    multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON,  on=False))

                    cmd.inform('text="Taking a science exposure"')

                    if not multiCmd.run():
                        failMsg = "Failed to take science exposure"
                        break

                    cmdState.nExpDone += 1
                    cmdState.nExpLeft -= 1
                #
                # Did we break out of that loop?
                #
                if failMsg:
                    cmd.fail('text="%s"' % failMsg)
                    continue
                #
                # We're done
                #
                if actorState.aborting:
                    cmd.fail('text="doScience was aborted')
                else:
                    cmd.finish('text="Your Nobel Prize is a little closer, sir')

            elif msg.type == Msg.GOTO_FIELD:
                cmd = msg.cmd
                actorState = msg.actorState
                cmdState = msg.cmdState
                cartridge = msg.cartridge
                survey = msg.survey
                #
                # Tell sop that we've accepted the command
                #
                cmdState.setCommandState('active')
                msg.replyQueue.put(Msg.REPLY, cmd=cmd, success=True)
                #
                # Define the command that we use to communicate our state to e.g. STUI
                #
                def status(cmd, newState=None):
                    if newState:
                        cmdState.setCommandState(newState)
                    if cmd:
                        actorState.actor.commandSets["SopCmd"].status(cmd, threads=False, finish=False,
                                                                      oneCommand="gotoField")
                #
                # Slew to field
                #
                slewTimeout = 180

                status(cmdState.cmd)

                if cmdState.doSlew:
                    multiCmd = SopMultiCommand(cmd, slewTimeout + actorState.timeout, "gotoField.slew")
                    cmdState.setStageState("slew", "active")

                    if True:
                        multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState,
                                        ra=cmdState.ra, dec=cmdState.dec, rot=cmdState.rotang)
                    else:
                        cmd.warn('text="RHL is skipping the slew"')

                    # eval doGuiderFlat here in case cmdState changed
                    doGuiderFlat = True if (cmdState.doGuider and cmdState.guiderFlatTime > 0) else False
                    if doGuiderFlat and (cmdState.nFlatLeft == 0 and survey == sopActor.MARVELS):
                        cmd.inform('text="commanding guider flat for Marvels"')
                        multiCmd.append(SopPrecondition(sopActor.FFS,     Msg.FFS_MOVE, open=False))
                        multiCmd.append(SopPrecondition(sopActor.FF_LAMP, Msg.LAMP_ON, on=True))
                        multiCmd.append(sopActor.GCAMERA, Msg.EXPOSE,
                                        expTime=cmdState.guiderFlatTime, expType="flat",
                                        cartridge=cartridge)
                        doGuiderFlat = False

                    if doGuiderFlat or cmdState.nFlatLeft > 0:
                        multiCmd.append(sopActor.FF_LAMP  , Msg.LAMP_ON, on=True)

                    if cmdState.nArcLeft > 0 or cmdState.doHartmann:
                        multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True)
                        multiCmd.append(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True)
                        multiCmd.append(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False)
                        multiCmd.append(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False)

                    if (cmdState.nArcLeft > 0 or cmdState.nFlatLeft > 0 or
                        cmdState.doHartmann or cmdState.doGuider):
                        multiCmd.append(sopActor.FFS, Msg.FFS_MOVE, open=False)

                    if not multiCmd.run():
                        cmdState.setStageState("slew", "failed")
                        cmd.fail('text="Failed to close screens, warm up lamps, and slew to field"')
                        continue

                    # Umm is cmd == cmdState.cmd? If so, the .fail above will cause trouble.
                    cmdState.setStageState("slew", "done")
                    status(cmdState.cmd)
                #
                # OK, we're there. 
                #
                if cmdState.doHartmann:
                    hartmannDelay = 180
                    cmdState.setStageState("hartmann", "active")
                    multiCmd = SopMultiCommand(cmd, actorState.timeout + hartmannDelay, "gotoField.hartmann")

                    multiCmd.append(sopActor.BOSS, Msg.HARTMANN)

                    multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
                    # N.b. not a precondition on HgCd as we don't want to wait for warmup (we'll wait for Ne)
                    # Yeah, but we would like to turn the HgCd on ASAP...
                    multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True)
                    multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True))
                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

                    if not multiCmd.run():
                        cmdState.setStageState("hartmann", "failed")
                        cmd.fail('text="Failed to do Hartmann sequence"')
                        continue

                    cmdState.setStageState("hartmann", "done")
                    status(cmdState.cmd)
                #
                # Calibs.  Arcs first
                #
                pendingReadout = False
                doingCalibs = False
                if (cmdState.nArcLeft > 0 or cmdState.nFlatLeft > 0) and \
                       cmdState.nArcDone == 0 and cmdState.nFlatDone == 0:
                    cmdState.setStageState("calibs", "active")
                    doingCalibs = True
                    
                if cmdState.nArcLeft > 0:
                    timeout = actorState.timeout + myGlobals.warmupTime[sopActor.HGCD_LAMP]

                    multiCmd = SopMultiCommand(cmd, timeout, "gotoField.calibs.arcs")

                    multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True))
                    multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True))
                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

                    if not multiCmd.run():
                        cmd.fail('text="Failed to prepare for arcs"')
                        continue
                    #
                    # Now take the exposure
                    #
                    if cmdState.nArcLeft > 0:  # not aborted since we last checked
                        if SopMultiCommand(cmd, cmdState.arcTime + actorState.timeout,  
                                           "gotoField.calibs.arcExposure",
                                           sopActor.BOSS, Msg.EXPOSE,
                                           expTime=cmdState.arcTime, expType="arc", readout=False,).run():
                            pendingReadout = True
                        else:
                            cmdState.setStageState("calibs", "failed")
                            cmd.fail('text="Failed to take arcs"')
                            continue

                    cmdState.nArcLeft -= 1
                    cmdState.nArcDone += 1

                    status(cmdState.cmd)

                #
                # Now the flats
                #
                multiCmd = SopMultiCommand(cmd,
                                           actorState.timeout + (readoutDuration if pendingReadout else 0),
                                           "gotoField.calibs.flats")
                if pendingReadout:
                    multiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
                    pendingReadout = False

                doGuiderFlat = True if (cmdState.doGuider and cmdState.guiderFlatTime > 0) else False
                if cmdState.nFlatLeft > 0 or doGuiderFlat:
                    multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=True))
                    multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

                if not multiCmd.run():
                    cmdState.setStageState("calibs", "failed")
                    cmd.fail('text="Failed to prepare for flats"')
                    continue
                #
                # Now take the exposure
                #
                if cmdState.nFlatLeft > 0 or doGuiderFlat:
                    multiCmd = SopMultiCommand(cmd, cmdState.flatTime + actorState.timeout,
                                               "gotoField.calibs.flatExposure")

                    if cmdState.nFlatLeft > 0:
                        pendingReadout = True
                        multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                        expTime=cmdState.flatTime, expType="flat", readout=False)
                    if cmdState.doGuider and cmdState.guiderFlatTime > 0:
                        multiCmd.append(sopActor.GCAMERA, Msg.EXPOSE,
                                        expTime=cmdState.guiderFlatTime, expType="flat", cartridge=cartridge)

                    if not multiCmd.run():
                        if pendingReadout:
                            SopMultiCommand(cmd, actorState.timeout + readoutDuration,
                                            "gotoField.calibs.flatReadout",
                                            sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True).run()

                        cmdState.setStageState("calibs", "failed")
                        cmd.fail('text="Failed to take flats"')
                        continue

                    cmdState.nFlatLeft -= 1
                    cmdState.nFlatDone += 1

                    status(cmdState.cmd)
                #
                # Readout any pending data and prepare to guide
                #
                if pendingReadout:
                    readoutMultiCmd = SopMultiCommand(cmd, readoutDuration + actorState.timeout,
                                                      "gotoField.calibs.lastFlatReadout")

                    readoutMultiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
                    pendingReadout = False

                    readoutMultiCmd.start()
                else:
                    if doingCalibs:
                        cmdState.setStageState("calibs", "done")

                    readoutMultiCmd = None

                multiCmd = SopMultiCommand(cmd,
                                           actorState.timeout + (readoutDuration if pendingReadout else 0),
                                           "gotoField.guide.prep")

                multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
                multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
                multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
                multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))

                if cmdState.doGuider:
                    multiCmd.append(sopActor.FFS, Msg.FFS_MOVE, open=True)

                if not multiCmd.run():
                    if readoutMultiCmd:
                        readoutMultiCmd.finish()

                    cmd.fail('text="Failed to prepare to guide"')
                    continue
                #
                # Start the guider
                #
                if cmdState.doGuider:
                    cmdState.setStageState("guider", "active")
                    multiCmd = SopMultiCommand(cmd, actorState.timeout + cmdState.guiderTime,
                                               "gotoField.guide.start")

                    for w in ("axes", "focus", "scale"):
                        multiCmd.append(sopActor.GUIDER, Msg.ENABLE, what=w, on=False)

                    multiCmd.append(sopActor.GUIDER, Msg.START, on=True,
                                    expTime=cmdState.guiderTime, oneExposure=True)

                    multiCmd.append(SopPrecondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
                    multiCmd.append(SopPrecondition(sopActor.FFS      , Msg.FFS_MOVE, open=True))

                    if not multiCmd.run():
                        cmdState.setStageState("guider", "failed")
                        cmd.fail('text="Failed to start guiding"')
                        continue

                    startedGuider = True
                    cmdState.setStageState("guider", "done")
                    status(cmdState.cmd)
                #
                # Catch the last readout's completion
                #
                if readoutMultiCmd:
                    if not readoutMultiCmd.finish():
                        cmdState.setStageState("calibs", "failed")
                        cmd.fail('text="Failed to readout last exposure"')
                        continue
                    else:
                        cmdState.setStageState("calibs", "done")
                #
                # We're done
                #
                if actorState.aborting:
                    cmdState.setCommandState('failed')
                    cmd.fail('text="gotoField was aborted')
                else:
                    cmdState.setCommandState('done')
                    cmd.finish('text="on field')
                
            elif msg.type == Msg.HARTMANN:
                """Take two arc exposures with the left then the right Hartmann screens in"""
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
